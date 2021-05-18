use std::borrow::Cow;
use std::io::prelude::*;
use std::io::Cursor;
use std::result::Result::{Err, Ok};
use std::collections::HashMap;
use std::collections::hash_map::{Entry as HashMapEntry, Keys};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::warn;
use twox_hash::RandomXxHashBuilder32;

use super::error::{Error, Result};
use super::xxhash::XxHash32;

const LOG_STATIC_SIZE: usize = 18; // checksum(4) + seq(8) + key_size(2) + value_size(4)
const LOG_TOMBSTONE: u32 = !0;
pub const MAX_VALUE_SIZE: u32 = !0 - 1;
pub const MAX_KEY_SIZE: u16 = !0;

#[derive(Debug)]
pub struct MemIdxEntry {
    pub pos: u64,
    pub seq: u64,
    pub size: u64,
    pub file_id: u32,
}

struct CompactionAnalysisEntry {
    entries: u64,
    dead_entries: u64,
    dead_bytes: u64,
}

pub struct CompactionAnalysis {
    map: HashMap<u32, CompactionAnalysisEntry>,
}

impl CompactionAnalysis {
    pub fn new() -> CompactionAnalysis {
        CompactionAnalysis {
            map: HashMap::new()
        }
    }

    pub fn add(&mut self, entry: &MemIdxEntry) {
        match self.map.entry(entry.file_id) {
            HashMapEntry::Occupied(mut occupied) => {
                occupied.get_mut().entries += 1;
            }
            HashMapEntry::Vacant(e) => {
                e.insert(CompactionAnalysisEntry {
                    entries: 1,
                    dead_entries: 0,
                    dead_bytes: 0,
                });
            }
        }
    }

    pub fn remove(&mut self, entry: &MemIdxEntry) {
        match self.map.entry(entry.file_id) {
            HashMapEntry::Occupied(mut occupied) => {
                occupied.get_mut().dead_entries += 1;
                occupied.get_mut().dead_bytes += entry.size;
            }
            HashMapEntry::Vacant(_) => {
                warn!("Tried to reclaim non-existant entry {:?}", entry);
            }
        }
    }

    pub fn remove_files(&mut self, files: &[u32]) {
        for file_id in files {
            self.map.remove(file_id);
        }
    }

    pub fn file_analysis(&self) -> Vec<(u32, f64, u64)> {
        self.map
            .iter()
            .map(|e| {
                (
                    *e.0,
                    e.1.dead_entries as f64 / e.1.entries as f64,
                    e.1.dead_bytes,
                )
            })
            .collect()
    }
}

pub struct MemIdx {
    mem: HashMap<Vec<u8>, MemIdxEntry, RandomXxHashBuilder32>,
    pub compaction_analysis: CompactionAnalysis,
}

impl MemIdx {
    pub fn new() -> MemIdx {
        // Use xxHash for lookup and insertion speed at RAM's limits
        let hash : HashMap<Vec<u8>, MemIdxEntry, RandomXxHashBuilder32> = Default::default();
        MemIdx {
            mem: hash,
            compaction_analysis: CompactionAnalysis::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, entry: MemIdxEntry) -> Option<MemIdxEntry> {
        self.compaction_analysis.add(&entry);
        self.mem.insert(key, entry).map(|entry| {
            self.compaction_analysis.remove(&entry);
            entry
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemIdxEntry> {
        self.mem.get(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<MemIdxEntry> {
        self.mem.remove(key).map(|entry| {
            self.compaction_analysis.remove(&entry);
            entry
        })
    }

    pub fn update(&mut self, ch: CompactionHint, file_id: u32) {
        let mem_idx_entry = MemIdxEntry {
            pos: ch.log_pos,
            seq: ch.seq,
            size: ch.log_size(),
            file_id: file_id,
        };

        match self.mem.entry(ch.key.to_vec()) {
            HashMapEntry::Occupied(mut occupied) => {
                if occupied.get().seq <= ch.seq {
                    self.compaction_analysis.remove(occupied.get());
                    if ch.deleted {
                        occupied.remove();
                    } else {
                        self.compaction_analysis.add(&mem_idx_entry);
                        occupied.insert(mem_idx_entry);
                    }
                } else {
                    self.compaction_analysis.add(&mem_idx_entry);
                    self.compaction_analysis.remove(&mem_idx_entry);
                }
            }
            HashMapEntry::Vacant(e) => {
                if !ch.deleted {
                    self.compaction_analysis.add(&mem_idx_entry);
                    e.insert(mem_idx_entry);
                }
            }
        }
    }

    pub fn keys(&self) -> Keys<Vec<u8>, MemIdxEntry> {
        self.mem.keys()
    }
}

#[derive(Eq, PartialEq)]
pub struct Log<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, [u8]>,
    pub seq: u64,
    pub deleted: bool,
}

impl<'a> Log<'a> {
    pub fn new<K, V>(seq: u64, key: K, value: V) -> Result<Log<'a>>
    where
        Cow<'a, [u8]>: From<K>,
        Cow<'a, [u8]>: From<V>,
    {
        let k = Cow::from(key);
        let v = Cow::from(value);

        if k.len() > MAX_KEY_SIZE as usize {
            return Err(Error::InvalidKeySize(k.len()));
        }

        if v.len() > MAX_VALUE_SIZE as usize {
            return Err(Error::InvalidValueSize(v.len()));
        }

        Ok(Log {
            key: k,
            value: v,
            seq: seq,
            deleted: false,
        })
    }

    pub fn deleted<K>(seq: u64, key: K) -> Log<'a>
    where
        Cow<'a, [u8]>: From<K>,
    {
        Log {
            key: Cow::from(key),
            value: Cow::Borrowed(&[]),
            seq: seq,
            deleted: true,
        }
    }

    pub fn size(&self) -> u64 {
        LOG_STATIC_SIZE as u64 + self.key.len() as u64 + self.value.len() as u64
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut cursor = Cursor::new(Vec::with_capacity(LOG_STATIC_SIZE));
        cursor.set_position(4);
        cursor.write_u64::<LittleEndian>(self.seq)?;
        cursor.write_u16::<LittleEndian>(self.key.len() as u16)?;

        if self.deleted {
            cursor.write_u32::<LittleEndian>(LOG_TOMBSTONE)?;
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32)?;
        }

        let checksum = {
            let mut hasher = XxHash32::new();
            hasher.update(&cursor.get_ref()[4..]);
            hasher.update(&self.key);
            hasher.update(&self.value);
            hasher.get()
        };

        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum)?;

        writer.write_all(&cursor.into_inner())?;
        writer.write_all(&self.key)?;

        if !self.deleted {
            writer.write_all(&self.value)?;
        }

        Ok(())
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Result<Log<'a>> {
        let mut header = vec![0u8; LOG_STATIC_SIZE as usize];
        reader.read_exact(&mut header)?;

        let mut cursor = Cursor::new(header);
        let checksum = cursor.read_u32::<LittleEndian>()?;
        let seq = cursor.read_u64::<LittleEndian>()?;
        let key_size = cursor.read_u16::<LittleEndian>()?;
        let value_size = cursor.read_u32::<LittleEndian>()?;

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key)?;

        let deleted = value_size == LOG_TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            let mut value = vec![0u8; value_size as usize];
            reader.read_exact(&mut value)?;
            Cow::from(value)
        };

        let hash = {
            let mut hasher = XxHash32::new();
            hasher.update(&cursor.get_ref()[4..]);
            hasher.update(&key);
            hasher.update(&value);
            hasher.get()
        };

        if hash != checksum {
            return Err(Error::InvalidChecksum {
                expected: checksum,
                found: hash,
            });
        }

        Ok(Log {
            key: Cow::from(key),
            value: value,
            seq: seq,
            deleted: deleted,
        })
    }
}

pub struct CompactionHint<'a> {
    pub key: Cow<'a, [u8]>,
    pub log_pos: u64,
    pub value_size: u32,
    pub seq: u64,
    pub deleted: bool,
}

impl<'a> CompactionHint<'a> {
    pub fn new(e: &'a Log, log_pos: u64) -> CompactionHint<'a> {
        CompactionHint {
            key: Cow::from(&*e.key),
            log_pos: log_pos,
            value_size: e.value.len() as u32,
            seq: e.seq,
            deleted: e.deleted,
        }
    }

    pub fn from(e: Log<'a>, log_pos: u64) -> CompactionHint<'a> {
        CompactionHint {
            key: e.key,
            log_pos: log_pos,
            value_size: e.value.len() as u32,
            seq: e.seq,
            deleted: e.deleted,
        }
    }

    pub fn log_size(&self) -> u64 {
        LOG_STATIC_SIZE as u64 + self.key.len() as u64 + self.value_size as u64
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u64::<LittleEndian>(self.seq)?;
        writer.write_u16::<LittleEndian>(self.key.len() as u16)?;

        if self.deleted {
            writer.write_u32::<LittleEndian>(LOG_TOMBSTONE)?;
        } else {
            writer.write_u32::<LittleEndian>(self.value_size)?;
        }

        writer.write_u64::<LittleEndian>(self.log_pos)?;
        writer.write_all(&self.key)?;

        Ok(())
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Result<CompactionHint<'a>> {
        let seq = reader.read_u64::<LittleEndian>()?;
        let key_size = reader.read_u16::<LittleEndian>()?;
        let value_size = reader.read_u32::<LittleEndian>()?;
        let log_pos = reader.read_u64::<LittleEndian>()?;

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key)?;

        let deleted = value_size == LOG_TOMBSTONE;

        Ok(CompactionHint {
            key: Cow::from(key),
            log_pos: log_pos,
            value_size: if deleted { 0 } else { value_size },
            seq: seq,
            deleted: value_size == LOG_TOMBSTONE,
        })
    }
}
