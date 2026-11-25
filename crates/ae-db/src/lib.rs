//! `PostgreSQL` access for the engine: pools, the Go-compatible migration runner and queries.

mod migrate;
mod pool;
mod queries;

pub use pool::{Db, connect};
pub use queries::truncate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("db: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("db: invalid id {0:?}")]
    InvalidId(String),
    #[error("db: migration {name}: {source}")]
    Migration { name: String, source: sqlx::Error },
    #[error("db: unexpected value {0:?}")]
    Value(String),
}

pub type Result<T> = std::result::Result<T, Error>;
