use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Take};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::result::Result::Ok;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use lazy_static::lazy_static;
use log::{info, warn};
use regex::Regex;

use super::slot::{CowEntry, CowHint};
use super::error::{Error, Result};
use super::chunk_queue::{ChunkQueue};
use super::util::{human_readable_byte_count, get_file_handle};
use super::xxhash::{XxHash32, xxhash32};

const DATA_FILE_EXTENSION: &'static str = "crabe.data";
const HINT_FILE_EXTENSION: &'static str = "crabe.hint";
const LOCK_FILE_NAME: &'static str = "crabe.lock";

pub struct Sequence(AtomicUsize);

impl Sequence {
    pub fn new(id: u32) -> Sequence {
        Sequence(AtomicUsize::new(id as usize))
    }

    pub fn increment(&self) -> u32 {
        self.0.fetch_add(1, Ordering::SeqCst) as u32 + 1
    }
}

pub struct Lsm {
    pub path: PathBuf,
    max_file_size: usize,
    lock_file: File,
    files: Vec<u32>,
    file_id_seq: Arc<Sequence>,
    file_chunk_queue: Mutex<ChunkQueue>,
    lsm_writer: LsmWriter,
    pub active_file_id: Option<u32>,
}

impl Lsm {
    pub fn open(
        path: &str,
        create: bool,
        sync: bool,
        max_file_size: usize,
        file_chunk_queue_size: usize,
    ) -> Result<Lsm> {
        let path_str = path;
        let path = PathBuf::from(path);

        if create {
            if path.exists() && !path.is_dir() {
                return Err(Error::InvalidPath(path_str.to_string()));
            } else if !path.exists() {
                fs::create_dir(&path)?;
            }
        } else {
            if !path.exists() || !path.is_dir() {
                return Err(Error::InvalidPath(path_str.to_string()));
            }
        }

        let lock_file = File::create(path.join(LOCK_FILE_NAME))?;
        lock_file.try_lock_exclusive()?;

        let files = find_data_files(&path)?;
        let current_file_id = if files.is_empty() {
            0
        } else {
            files[files.len() - 1]
        };

        let file_id_seq = Arc::new(Sequence::new(current_file_id));
        info!("Current file id : {}", current_file_id);
        let lsm_writer = LsmWriter::new(&path, sync, max_file_size, file_id_seq.clone());

        Ok(Lsm {
            path: path,
            max_file_size: max_file_size,
            lock_file: lock_file,
            files: files,
            file_id_seq: file_id_seq,
            file_chunk_queue: Mutex::new(ChunkQueue::new(file_chunk_queue_size)),
            lsm_writer: lsm_writer,
            active_file_id: None,
        })
    }

    pub fn file_size(&self, file_id: u32) -> Result<u64> {
        let data_file = self.file_chunk_queue
            .lock()
            .unwrap()
            .get(file_id)
            .map(Ok)
            .unwrap_or_else(|| {
                get_file_handle(&get_data_file_path(&self.path, file_id), false)
            })?;
        let res = Ok(data_file.metadata()?.len());
        self.file_chunk_queue.lock().unwrap().put(file_id, data_file);
        res
    }

    pub fn files(&self) -> Vec<u32> {
        self.files.clone()
    }

    pub fn entries<'a>(&self, file_id: u32) -> Result<Entries<'a>> {
        let data_file_path = get_data_file_path(&self.path, file_id);
        info!("Loading data file: {:?}", data_file_path);
        let data_file = get_file_handle(&data_file_path, false)?;
        let data_file_size = data_file.metadata()?.len();

        Ok(Entries {
            data_file: data_file.take(data_file_size),
            data_file_pos: 0,
            phantom: PhantomData,
        })
    }

    pub fn hints<'a>(&self, file_id: u32) -> Result<Option<Hints<'a>>> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        Ok(if is_valid_hint_file(&hint_file_path)? {
            info!("Loading hint file: {:?}", hint_file_path);
            let hint_file = get_file_handle(&hint_file_path, false)?;
            let hint_file_size = hint_file.metadata()?.len();

            Some(Hints {
                hint_file: hint_file.take(hint_file_size - 4),
                phantom: PhantomData,
            })
        } else {
            None
        })
    }

    pub fn recreate_hints<'a>(&mut self, file_id: u32) -> Result<RecreateHints<'a>> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        warn!("Re-creating hint file: {:?}", hint_file_path);

        let hint_writer = HintWriter::new(&self.path, file_id)?;
        let entries = self.entries(file_id)?;

        Ok(RecreateHints {
            hint_writer: hint_writer,
            entries: entries,
        })
    }

    pub fn read_entry<'a>(&self, file_id: u32, entry_pos: u64) -> Result<CowEntry<'a>> {
        let mut data_file = self.file_chunk_queue
            .lock()
            .unwrap()
            .get(file_id)
            .map(Ok)
            .unwrap_or_else(|| {
                get_file_handle(&get_data_file_path(&self.path, file_id), false)
            })?;

        data_file.seek(SeekFrom::Start(entry_pos))?;
        let res = CowEntry::from_read(&mut data_file);

        self.file_chunk_queue.lock().unwrap().put(file_id, data_file);

        res
    }

    pub fn append_entry<'a>(&mut self, entry: &CowEntry<'a>) -> Result<(u32, u64)> {
        Ok(match self.lsm_writer.write(entry)? {
            LsmWrite::NewFile(file_id) => {
                if let Some(active_file_id) = self.active_file_id {
                    self.add_file(active_file_id);
                }
                self.active_file_id = Some(file_id);
                info!(
                    "New active data file {:?}",
                    self.lsm_writer.entry_writer()?.data_file_path
                );
                (file_id, 0)
            }
            LsmWrite::Ok(entry_pos) => (self.active_file_id.unwrap(), entry_pos),
        })
    }

    pub fn writer(&self) -> LsmWriter {
        LsmWriter::new(
            &self.path,
            false,
            self.max_file_size,
            self.file_id_seq.clone(),
        )
    }

    pub fn sync(&self) -> Result<()> {
        self.lsm_writer.sync()
    }

    pub fn swap_files(&mut self, old_files: &[u32], new_files: &[u32]) -> Result<()> {
        for &file_id in old_files {
            let idx = self.files.binary_search(&file_id).map_err(|_| {
                Error::InvalidFileId(file_id)
            })?;

            self.files.remove(idx);

            let data_file_path = get_data_file_path(&self.path, file_id);
            let hint_file_path = get_hint_file_path(&self.path, file_id);

            fs::remove_file(data_file_path)?;
            let _ = fs::remove_file(hint_file_path);
        }

        self.files.extend(new_files);
        self.files.sort();

        Ok(())
    }

    fn add_file(&mut self, file_id: u32) {
        self.files.push(file_id);
        self.files.sort();
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

pub struct LsmWriter {
    path: PathBuf,
    sync: bool,
    max_file_size: usize,
    file_id_seq: Arc<Sequence>,
    entry_writer: Option<EntryWriter>,
}

pub enum LsmWrite {
    Ok(u64),
    NewFile(u32),
}

impl LsmWriter {
    pub fn new(
        path: &Path,
        sync: bool,
        max_file_size: usize,
        file_id_seq: Arc<Sequence>,
    ) -> LsmWriter {

        LsmWriter {
            path: path.to_path_buf(),
            sync: sync,
            max_file_size: max_file_size,
            file_id_seq: file_id_seq,
            entry_writer: None,
        }
    }

    fn entry_writer(&mut self) -> Result<&EntryWriter> {
        if self.entry_writer.is_none() {
            self.new_entry_writer()?;
        }
        Ok(self.entry_writer.as_ref().unwrap())
    }

    fn new_entry_writer(&mut self) -> Result<u32> {
        let file_id = self.file_id_seq.increment();

        if self.entry_writer.is_some() {
            info!(
                "Closed data file {:?}",
                self.entry_writer.as_ref().unwrap().data_file_path
            );
        }

        self.entry_writer = Some(EntryWriter::new(&self.path, self.sync, file_id)?);
        Ok(file_id)
    }

    pub fn write(&mut self, entry: &CowEntry) -> Result<LsmWrite> {
        Ok(if self.entry_writer.is_none() ||
            self.entry_writer.as_ref().unwrap().data_file_pos + entry.size() >
            self.max_file_size as u64
        {
            if self.entry_writer.is_some() {
                info!(
                    "Data file {:?} reached file limit of {}",
                    self.entry_writer.as_ref().unwrap().data_file_path,
                    human_readable_byte_count(self.max_file_size, true)
                );
            }

            let file_id = self.new_entry_writer()?;
            let entry_pos = self.entry_writer.as_mut().unwrap().write(entry)?;

            assert_eq!(entry_pos, 0);

            LsmWrite::NewFile(file_id)
        } else {
            let entry_pos = self.entry_writer.as_mut().unwrap().write(entry)?;
            LsmWrite::Ok(entry_pos)
        })
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(ref writer) = self.entry_writer {
            writer.data_file.sync_data()?
        }
        Ok(())
    }
}

pub struct EntryWriter {
    sync: bool,
    data_file_path: PathBuf,
    data_file: File,
    data_file_pos: u64,
    hint_writer: HintWriter,
}

impl EntryWriter {
    pub fn new(path: &Path, sync: bool, file_id: u32) -> Result<EntryWriter> {
        let data_file_path = get_data_file_path(path, file_id);
        let data_file = get_file_handle(&data_file_path, true)?;

        info!("Created new data file {:?}", data_file_path);

        let hint_writer = HintWriter::new(path, file_id)?;

        Ok(EntryWriter {
            sync: sync,
            data_file_path: data_file_path,
            data_file: data_file,
            data_file_pos: 0,
            hint_writer: hint_writer,
        })
    }

    pub fn write<'a>(&mut self, entry: &CowEntry<'a>) -> Result<u64> {
        let entry_pos = self.data_file_pos;

        let hint = CowHint::new(entry, entry_pos);
        entry.write_bytes(&mut self.data_file)?;

        self.hint_writer.write(&hint)?;

        if self.sync {
            self.data_file.sync_data()?;
        }

        self.data_file_pos += entry.size();

        Ok(entry_pos)
    }
}

impl Drop for EntryWriter {
    fn drop(&mut self) {
        let _ = self.data_file.sync_data();
    }
}

struct HintWriter {
    hint_file: File,
    hint_file_hasher: XxHash32,
}

impl HintWriter {
    pub fn new(path: &Path, file_id: u32) -> Result<HintWriter> {
        let hint_file = get_file_handle(&get_hint_file_path(path, file_id), true)?;

        Ok(HintWriter {
            hint_file: hint_file,
            hint_file_hasher: XxHash32::new(),
        })
    }

    pub fn write<'a>(&mut self, hint: &CowHint<'a>) -> Result<()> {
        hint.write_bytes(&mut self.hint_file)?;
        hint.write_bytes(&mut self.hint_file_hasher)?;
        Ok(())
    }
}

impl Drop for HintWriter {
    fn drop(&mut self) {
        let _ = self.hint_file.write_u32::<LittleEndian>(
            self.hint_file_hasher.get(),
        );
    }
}

pub struct Entries<'a> {
    data_file: Take<File>,
    data_file_pos: u64,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (u64, Result<CowEntry<'a>>);

    fn next(&mut self) -> Option<(u64, Result<CowEntry<'a>>)> {
        let limit = self.data_file.limit();
        if limit == 0 {
            None
        } else {
            let entry = CowEntry::from_read(&mut self.data_file);
            let entry_pos = self.data_file_pos;

            let read = limit - self.data_file.limit();

            self.data_file_pos += read;

            let entry = match entry {
                Ok(entry) => {
                    assert_eq!(entry.size(), read);
                    Ok(entry)
                }
                e => e,
            };

            Some((entry_pos, entry))
        }
    }
}

pub struct Hints<'a> {
    hint_file: Take<File>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Hints<'a> {
    type Item = Result<CowHint<'a>>;

    fn next(&mut self) -> Option<Result<CowHint<'a>>> {
        if self.hint_file.limit() == 0 {
            None
        } else {
            Some(CowHint::from_read(&mut self.hint_file))
        }
    }
}

pub struct RecreateHints<'a> {
    hint_writer: HintWriter,
    entries: Entries<'a>,
}

impl<'a> Iterator for RecreateHints<'a> {
    type Item = Result<CowHint<'a>>;

    fn next(&mut self) -> Option<Result<CowHint<'a>>> {
        self.entries.next().map(|e| {
            let (entry_pos, entry) = e;
            let hint = CowHint::from(entry?, entry_pos);
            self.hint_writer.write(&hint)?;
            Ok(hint)
        })
    }
}

impl<'a> Drop for RecreateHints<'a> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

fn get_data_file_path(path: &Path, file_id: u32) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id).with_extension(DATA_FILE_EXTENSION)
}

fn get_hint_file_path(path: &Path, file_id: u32) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id).with_extension(HINT_FILE_EXTENSION)
}

fn find_data_files(path: &Path) -> Result<Vec<u32>> {
    let files = fs::read_dir(path)?;

    lazy_static! {
        static ref RE: Regex =
            Regex::new(&format!("(\\d+).{}$", DATA_FILE_EXTENSION)).unwrap();
    }

    let mut data_files = Vec::new();

    for file in files {
        let file = file?;
        if file.metadata()?.is_file() {
            let file_name = file.file_name();
            let captures = RE.captures(file_name.to_str().unwrap());
            if let Some(n) = captures.and_then(|c| {
                c.get(1).and_then(|n| n.as_str().parse::<u32>().ok())
            })
            {
                data_files.push(n)
            }
        }
    }

    data_files.sort();

    Ok(data_files)
}

fn is_valid_hint_file(path: &Path) -> Result<bool> {
    Ok(
        path.is_file() &&
            {
                let mut hint_file = get_file_handle(path, false)?;
                let mut buf = Vec::new();
                hint_file.read_to_end(&mut buf)?;

                buf.len() >= 4 &&
                    {
                        let hash = xxhash32(&buf[..buf.len() - 4]);

                        let mut cursor = Cursor::new(&buf[buf.len() - 4..]);
                        let checksum = cursor.read_u32::<LittleEndian>()?;

                        let valid = hash == checksum;

                        if !valid {
                            warn!("Found corrupt hint file: {:?}", &path);
                        }
                        valid
                    }
            },
    )
}