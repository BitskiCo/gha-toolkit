pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Invalid chunk checksum")]
    CacheChunkChecksum,

    #[error("While {message}: expected chunk size {expected_size} got {actual_size}")]
    CacheChunkSize {
        expected_size: usize,
        actual_size: usize,
        message: &'static str,
    },

    #[error("Cache not found.")]
    CacheNotFound,

    #[error("Cache service responded with {status}: {message}")]
    CacheServiceStatus {
        status: http::StatusCode,
        message: String,
    },

    #[error("Cache size of {0} bytes is too large")]
    CacheSizeTooLarge(usize),

    #[error(transparent)]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    #[error("Key Validation Error: {0} cannot contain commas")]
    InvalidKeyComma(String),

    #[error("Key Validation Error: {0} cannot be larger than 512 characters")]
    InvalidKeyLength(String),

    #[error("Missing one of key or restore keys")]
    MissingKey,

    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    SerdeUrlencodedSerialize(#[from] serde_urlencoded::ser::Error),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    #[error("Error reading env var \"{name}\": {source} ")]
    VarError {
        #[source]
        source: std::env::VarError,
        name: &'static str,
    },
}
