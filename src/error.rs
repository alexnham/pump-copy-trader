use thiserror::Error;

#[derive(Debug, Error)]
pub enum CopyTraderError {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("signal source error: {0}")]
    Signal(String),
    #[error("transaction decode error: {0}")]
    Decode(String),
    #[error("unsupported transaction: {0}")]
    Unsupported(String),
    #[error("unsupported transaction: {1}")]
    OutOfScope(crate::domain::UnsupportedReason, String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
}

pub type Result<T> = std::result::Result<T, CopyTraderError>;
