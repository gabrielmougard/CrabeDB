use std::io::{Result, Write};
use std::result::Result::Ok;
use std::hash::Hasher;

use twox_hash::XxHash32 as TwoXhash32;

pub struct XxHash32(TwoXhash32);

impl XxHash32 {
    pub fn new() -> XxHash32 {
        XxHash32(TwoXhash32::with_seed(0))
    }

    pub fn update(&mut self, buf: &[u8]) {
        self.0.write(buf);
    }

    pub fn get(&self) -> u32 {
        self.0.finish() as u32
    }
}

impl Write for XxHash32 {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn xxhash32(buf: &[u8]) -> u32 {
    let mut hasher = TwoXhash32::with_seed(0);
    hasher.write(buf);
    hasher.finish() as u32
}
