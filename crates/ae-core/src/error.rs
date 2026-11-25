use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{key}={value:?} is not a valid {expected}")]
    Invalid {
        key: &'static str,
        value: String,
        expected: &'static str,
    },
}
