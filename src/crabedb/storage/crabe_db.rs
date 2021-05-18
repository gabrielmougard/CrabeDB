use std::collections::{BTreeSet, HashMap};
use std::collections::hash_map::{Entry as HashMapEntry, Keys};
use std::path::PathBuf;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::vec::Vec;

use time;
use log::{info, warn, debug};

use super::options::{StorageOptions, SyncOptions};
use super::slot::{MemIdx, MemIdxEntry, Log, CompactionHint};
use super::error::Result;
use super::lsm::{Lsm, LsmWrite};
use super::util::human_readable_byte_count;

pub struct CrabeDBinternal {
    current_seq: u64,
    idx: MemIdx,
    lsm: Lsm,
}

impl CrabeDBinternal {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let val = match self.idx.get(key) {
            Some(idx_log) => {
                let log = self.lsm.read_log(
                    idx_log.file_id,
                    idx_log.pos,
                )?;
                if log.deleted {
                    warn!(
                        "Index pointed to dead log: Log {{ key: {:?}, sequence: {} }} at \
                        file: {}",
                        log.key,
                        log.seq,
                        idx_log.file_id
                    );
                    None
                } else {
                    Some(log.value.into_owned())
                }
            }
            _ => None,
        };

        Ok(val)
    }

    fn put(&mut self, key: Vec<u8>, value: &[u8]) -> Result<()> {
        let idx_log = {
            let log = Log::new(self.current_seq, &*key, value)?;
            let (file_id, file_pos) = self.lsm.append_log(&log)?;
            self.current_seq += 1;

            MemIdxEntry {
                pos: file_pos,
                seq: log.seq,
                size: log.size(),
                file_id: file_id,
            }
        };

        self.idx.set(key, idx_log);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.idx.remove(key).is_some() {
            let log = Log::deleted(self.current_seq, key);
            self.lsm.append_log(&log)?;
            self.current_seq += 1;
        }
        Ok(())
    }

    pub fn keys(&self) -> Keys<Vec<u8>, MemIdxEntry> {
        self.idx.keys()
    }
}

#[derive(Clone)]
pub struct CrabeDB {
    path: PathBuf,
    options: StorageOptions,
    dropped: Arc<AtomicBool>,
    internal: Arc<RwLock<CrabeDBinternal>>,
    compaction: Arc<Mutex<()>>,
}

impl CrabeDB {
    pub fn load(path: &str, options: StorageOptions) -> Result<CrabeDB> {
        info!("loading key/value store: {:?}", &path);
        let mut lsm = Lsm::load(
            path,
            options.create,
            options.sync == SyncOptions::Always,
            options.max_file_size,
            options.file_chunk_queue_size,
        )?;

        let mut idx = MemIdx::new();
        let mut seq = 0;

        for file_id in lsm.files() {
            let mut update_idx_func = |ch: CompactionHint| {
                if ch.seq > seq {
                    seq = ch.seq;
                }
                idx.update(ch, file_id);
            };

            match lsm.compaction_hints(file_id)? {
                Some(chs) => {
                    for ch in chs {
                        update_idx_func(ch?);
                    }
                }
                None => {
                    for ch in lsm.update_compaction_hints(file_id)? {
                        update_idx_func(ch?);
                    }
                }
            };
        }

        info!("loaded key/value store: {:?}", &path);
        info!("Current sequence number: {:?}", seq);

        let crabe_db = CrabeDB {
            path: lsm.path.clone(),
            options: options,
            dropped: Arc::new(AtomicBool::new(false)),
            internal: Arc::new(RwLock::new(CrabeDBinternal {
                current_seq: seq + 1,
                lsm: lsm,
                idx: idx,
            })),
            compaction: Arc::new(Mutex::new(())),
        };

        if let SyncOptions::Frequency(millis) = crabe_db.options.sync {
            let crabe_db = crabe_db.clone();

            thread::spawn(move || {
                let duration = Duration::from_millis(millis as u64);
                loop {
                    if crabe_db.dropped.load(Ordering::SeqCst) {
                        info!(
                            "CrabeDB has been dropped, background file sync thread is exiting"
                        );
                        break;
                    }

                    debug!("Background file sync");
                    crabe_db.internal.read().unwrap().lsm.sync().unwrap();
                    thread::sleep(duration);
                }
            });
        };

        if crabe_db.options.compaction {
            let crabe_db = crabe_db.clone();

            thread::spawn(move || {
                let duration = Duration::from_secs(crabe_db.options.compaction_check_frequency);
                loop {
                    if crabe_db.dropped.load(Ordering::SeqCst) {
                        info!(
                            "CrabeDB has been dropped, background compaction thread is exiting"
                        );
                        break;
                    }

                    info!("Compaction thread wake up");

                    let current_hour = time::now().tm_hour as usize;
                    let (window_start, window_end) = crabe_db.options.compaction_window;
                    let in_window = if window_start <= window_end {
                        current_hour >= window_start && current_hour <= window_end
                    } else {
                        current_hour >= window_end || current_hour <= window_end
                    };

                    if !in_window {
                        info!(
                            "Compaction outside defined window {:?}",
                            crabe_db.options.compaction_window
                        );
                    } else if let Err(err) = crabe_db.compact() {
                        warn!("Error during compaction: {}", err);
                    }

                    thread::sleep(duration);
                }
            });
        }

        Ok(crabe_db)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.internal.read().unwrap().get(key.as_ref())
    }

    pub fn set<K: Into<Vec<u8>>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.internal.write().unwrap().put(key.into(), value.as_ref())
    }

    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.internal.write().unwrap().delete(key.as_ref())
    }

    fn compact_files_util(&self, files: &[u32]) -> Result<(Vec<u32>, Vec<u32>)> {
        let active_file_id = {
            self.internal.read().unwrap().lsm.active_file_id
        };

        let compacted_files_hints = files.iter().flat_map(|&file_id| {
            if active_file_id.is_some() && active_file_id.unwrap() == file_id {
                None
            } else {
                self.internal
                    .read()
                    .unwrap()
                    .lsm
                    .compaction_hints(file_id)
                    .ok()
                    .and_then(|compaction_hints| compaction_hints.map(|h| (file_id, h)))
            }
        });

        let mut compacted_files = Vec::new();
        let mut new_files = Vec::new();
        let mut deletes = HashMap::new();

        let mut lsm_writer = {
            self.internal.read().unwrap().lsm.writer()
        };

        for (file_id, compaction_hints) in compacted_files_hints {
            let mut inserts = Vec::new();

            for ch in compaction_hints {
                let ch = ch?;
                let internal = self.internal.read().unwrap();
                let idx_log = internal.idx.get(&*ch.key);
                if ch.deleted {
                    if idx_log.is_none() {
                        match deletes.entry(ch.key.to_vec()) {
                            HashMapEntry::Occupied(mut occupied) => {
                                if *occupied.get() < ch.seq {
                                    occupied.insert(ch.seq);
                                }
                            }
                            HashMapEntry::Vacant(entry) => {
                                entry.insert(ch.seq);
                            }
                        }
                    }
                } else if idx_log.is_some() && idx_log.unwrap().seq == ch.seq {
                    inserts.push(ch)
                }
            }

            for ch in inserts {
                let lsm = &self.internal.read().unwrap().lsm;
                let lsm_write = lsm_writer.write(&lsm.read_log(file_id, ch.log_pos)?)?;

                if let LsmWrite::NewFile(file_id) = lsm_write {
                    new_files.push(file_id);
                }
            }

            compacted_files.push(file_id);
        }

        for (key, seq) in deletes {
            lsm_writer.write(&Log::deleted(seq, key))?;
        }

        Ok((compacted_files, new_files))
    }

    fn compact_files(&self, files: &[u32]) -> Result<()> {
        info!("Compacting data files: {:?}", files);
        let (ref compacted_files, ref new_files) = self.compact_files_util(files)?;
        for &file_id in new_files {
            let compaction_hints = {
                self.internal.read().unwrap().lsm.compaction_hints(file_id)?
            };

            if let Some(chs) = compaction_hints {
                for ch in chs {
                    let ch = ch?;
                    self.internal.write().unwrap().idx.update(ch, file_id);
                }
            };
        }
        self.internal.write().unwrap().idx.compaction_analysis.remove_files(
            compacted_files,
        );
        self.internal.write().unwrap().lsm.swap_files(
            compacted_files,
            new_files,
        )?;
        info!(
            "Finished compacting data files: {:?} into: {:?}",
            compacted_files,
            new_files
        );
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        let _lock = self.compaction.lock().unwrap();
        let active_file_id = {
            self.internal.read().unwrap().lsm.active_file_id
        };
        let compaction_analysis = {
            self.internal.read().unwrap().idx.compaction_analysis.file_analysis()
        };

        let mut files = BTreeSet::new();
        let mut triggered = false;

        for (file_id, fragmentation, dead_bytes) in compaction_analysis {
            if active_file_id.is_some() && file_id == active_file_id.unwrap() {
                continue;
            }

            if !triggered {
                if fragmentation >= self.options.fragmentation_trigger {
                    info!(
                        "File {} has fragmentation factor of {:.1}%, compaction will start",
                        file_id,
                        fragmentation * 100.0
                    );
                    triggered = true;
                    files.insert(file_id);
                } else if dead_bytes >= self.options.dead_bytes_trigger && !files.contains(&file_id) {
                    info!(
                        "File {} has {} of dead data, triggered compaction",
                        file_id,
                        human_readable_byte_count(dead_bytes as usize, true)
                    );
                    triggered = true;
                    files.insert(file_id);
                }
            }

            if fragmentation >= self.options.fragmentation_threshold && !files.contains(&file_id) {
                info!(
                    "File {} has fragmentation factor of {:.1}%, adding for compaction",
                    file_id,
                    fragmentation * 100.0
                );
                files.insert(file_id);
            } else if dead_bytes >= self.options.dead_bytes_threshold && !files.contains(&file_id) {
                info!(
                    "File {} has {} of dead data, adding for compaction",
                    file_id,
                    human_readable_byte_count(dead_bytes as usize, true)
                );
                files.insert(file_id);
            }

            if !files.contains(&file_id) {
                let file_size = {
                    self.internal.read().unwrap().lsm.file_size(file_id).ok()
                };

                if let Some(file_size) = file_size {
                    if file_size <= self.options.small_file_threshold {
                        info!(
                            "File {} has total size of {}, adding for compaction",
                            file_id,
                            human_readable_byte_count(file_size as usize, true)
                        );
                        files.insert(file_id);
                    }
                };
            }
        }

        if triggered {
            let files: Vec<_> = files.into_iter().collect();
            self.compact_files(&files)?;
        } else if !files.is_empty() {
            info!(
                "Compaction of files {:?} aborted due to missing trigger",
                &files
            );
        } else {
            info!("No files eligible for compaction")
        }

        Ok(())
    }
}

impl Drop for CrabeDB {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        let _lock = self.compaction.lock().unwrap();
    }
}