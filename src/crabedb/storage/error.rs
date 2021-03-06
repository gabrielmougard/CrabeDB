use std::convert::From;
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use tonic::{Status, Code};

use super::slot::{MAX_KEY_SIZE, MAX_VALUE_SIZE};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidFileId(u32),
    InvalidKeySize(usize),
    InvalidValueSize(usize),
    InvalidChecksum { expected: u32, found: u32 },
    InvalidPath(String),
}

pub type Result<T> = result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::InvalidFileId(file_id) => write!(f, "Invalid file id: {}", file_id),
            Error::InvalidKeySize(size) => {
                write!(
                    f,
                    "Invalid key size, max: {}, found: {}",
                    MAX_KEY_SIZE,
                    size
                )
            }
            Error::InvalidValueSize(size) => {
                write!(
                    f,
                    "Invalid value size, max: {}, found: {}",
                    MAX_VALUE_SIZE,
                    size
                )
            }
            Error::InvalidChecksum { expected, found } => {
                write!(
                    f,
                    "Invalid checksum, expected: {}, found: {}",
                    expected,
                    found
                )
            }
            Error::InvalidPath(ref path) => write!(f, "Invalid path provided: {}", path),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<Error> for Status {
    fn from(_: Error) -> Self {
        Status::new(Code::Internal, "CrabeDB internal error.")
    }
}

impl error::Error for Error {
    #![allow(deprecated)]
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::InvalidFileId(..) => "Invalid file id",
            Error::InvalidChecksum { .. } => "Invalid checksum",
            Error::InvalidKeySize(..) => "Invalid key size",
            Error::InvalidValueSize(..) => "Invalid value size",
            Error::InvalidPath(..) => "Invalid path",
        }
    }
}