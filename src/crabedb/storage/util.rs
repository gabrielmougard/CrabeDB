use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io::Result;

pub fn human_readable_byte_count(bytes: usize, si: bool) -> String {
    let unit = if si { 1000 } else { 1024 };
    if bytes < unit {
        return format!("{} B", bytes);
    }
    let exp = ((bytes as f64).ln() / (unit as f64).ln()) as usize;

    let units = if si { "kMGTPE" } else { "KMGTPE" };
    let pre = format!(
        "{}{}",
        units.chars().nth(exp - 1).unwrap(),
        if si { "" } else { "i" }
    );

    format!("{:.1} {}B", bytes / unit.pow(exp as u32), pre)
}

pub fn get_file_handle(path: &Path, write: bool) -> Result<File> {
    if write {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    } else {
        OpenOptions::new().read(true).open(path)
    }
}