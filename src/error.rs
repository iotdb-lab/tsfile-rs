use std::io;
use std::result;

pub type Result<T> = result::Result<T, TsFileError>;


#[derive(Debug, PartialEq)]
pub enum TsFileError {
    General(String)
}

#[macro_export]
macro_rules! general_err {
    ($fmt:expr) => (TsFileError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (TsFileError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (TsFileError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        TsFileError::General(&format!($fmt, $($args),*), $e));
}


impl std::fmt::Display for TsFileError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            TsFileError::General(ref message) => {
                write!(fmt, "TsFileError error: {}", message)
            }
        }
    }
}

impl From<std::str::Utf8Error> for TsFileError {
    fn from(e: std::str::Utf8Error) -> TsFileError {
        TsFileError::General(format!("underlying utf8 error: {}", e))
    }
}

impl std::error::Error for TsFileError {
    fn cause(&self) -> Option<&dyn ::std::error::Error> {
        None
    }
}

impl From<io::Error> for TsFileError {
    fn from(e: io::Error) -> TsFileError {
        TsFileError::General(format!("underlying IO error: {}", e))
    }
}