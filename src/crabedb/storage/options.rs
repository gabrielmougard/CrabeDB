use super::crabe_db::CrabeDB;
use super::error::Result;

#[derive(Clone, PartialEq)]
pub enum SyncOptions {
    // Synchronize writes in the background every `n` milliseconds.
    Frequency(usize),
    // Never synchronize writes.
    Never,
    // Always synchronize writes.
    Always,
}

#[derive(Clone)]
pub struct StorageOptions {
    pub create: bool,
    pub sync: SyncOptions,
    pub max_file_size: usize,
    pub file_chunk_queue_size: usize,
    pub compaction: bool,
    pub compaction_check_frequency: u64,
    pub compaction_window: (usize, usize),
    pub fragmentation_trigger: f64,
    pub dead_bytes_trigger: u64,
    pub fragmentation_threshold: f64,
    pub dead_bytes_threshold: u64,
    pub small_file_threshold: u64,
}

impl Default for StorageOptions {
    fn default() -> StorageOptions {
        StorageOptions {
            create: true,
            sync: SyncOptions::Frequency(2000),
            max_file_size: 2 * 1024 * 1024 * 1024, // 2MBytes
            file_chunk_queue_size: 2048,
            compaction: true,
            compaction_check_frequency: 3600,
            compaction_window: (0, 23),
            fragmentation_trigger: 0.6,
            dead_bytes_trigger: 512 * 1024 * 1024,
            fragmentation_threshold: 0.4,
            dead_bytes_threshold: 128 * 1024 * 1024,
            small_file_threshold: 10 * 1024 * 1024,
        }
    }
}

impl StorageOptions {
    pub fn new() -> StorageOptions {
        StorageOptions::default()
    }

    pub fn sync(&mut self, sync: SyncOptions) -> &mut StorageOptions {
        self.sync = sync;
        self
    }

    pub fn max_file_size(&mut self, max_file_size: usize) -> &mut StorageOptions {
        self.max_file_size = max_file_size;
        self
    }

    pub fn file_chunk_queue_size(&mut self, file_chunk_queue_size: usize) -> &mut StorageOptions {
        self.file_chunk_queue_size = file_chunk_queue_size;
        self
    }

    pub fn compaction(&mut self, compaction: bool) -> &mut StorageOptions {
        self.compaction = compaction;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut StorageOptions {
        self.create = create;
        self
    }

    pub fn compaction_check_frequency(&mut self, compaction_check_frequency: u64) -> &mut StorageOptions {
        self.compaction_check_frequency = compaction_check_frequency;
        self
    }

    pub fn compaction_window(&mut self, start: usize, end: usize) -> &mut StorageOptions {
        self.compaction_window = (start, end);
        self
    }

    pub fn fragmentation_trigger(&mut self, fragmentation_trigger: f64) -> &mut StorageOptions {
        self.fragmentation_trigger = fragmentation_trigger;
        self
    }

    pub fn dead_bytes_trigger(&mut self, dead_bytes_trigger: u64) -> &mut StorageOptions {
        self.dead_bytes_trigger = dead_bytes_trigger;
        self
    }

    pub fn fragmentation_threshold(&mut self, fragmentation_threshold: f64) -> &mut StorageOptions {
        self.fragmentation_threshold = fragmentation_threshold;
        self
    }

    pub fn dead_bytes_threshold(&mut self, dead_bytes_threshold: u64) -> &mut StorageOptions {
        self.dead_bytes_threshold = dead_bytes_threshold;
        self
    }

    pub fn small_file_threshold(&mut self, small_file_threshold: u64) -> &mut StorageOptions {
        self.small_file_threshold = small_file_threshold;
        self
    }

    pub fn open(&self, path: &str) -> Result<CrabeDB> {
        CrabeDB::open(path, self.clone())
    }
}
