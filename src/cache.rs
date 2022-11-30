//! # GitHub Actions cache client
//!
//! The [`CacheClient`] is an idiomatic Rust port of
//! [@actions/cache](https://github.com/actions/cache).
//!
//! See [Caching dependencies to speed up
//! workflows](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows)
//! for the official GitHub documentation.
//!
//! ```rust
//! use std::io::Cursor;
//! use std::time::SystemTime;
//!
//! # use gha_toolkit::cache::*;
//! #
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> anyhow::Result<()> {
//! let version = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
//! let cache_key = format!("gha-toolkit-{version:#x}");
//!
//! let client = CacheClient::from_env()?
//!     .cache_from([&cache_key].into_iter())
//!     .cache_to(&cache_key)
//!     .build()?;
//!
//! let scope = "gha_toolkit::cache";
//! let data = "Hello World!";
//!
//! // Create a new cache entry.
//! client.put(scope, Cursor::new(data)).await?;
//!
//! // Read from the cache entry.
//! let cache_entry = client.entry(scope).await?.expect("cache entry");
//!
//! // Fetch the cache data.
//! let archive_location = cache_entry.archive_location.expect("archive location");
//! let cached_bytes = client.get(&archive_location).await?;
//!
//! // Decode the cache data to a UTF-8 string.
//! let cached_data = String::from_utf8(cached_bytes)?;
//!
//! assert_eq!(cached_data, data);
//! # Ok(())
//! # }
//! ```

use std::env;
use std::io::{prelude::*, SeekFrom};
use std::ops::DerefMut as _;
use std::sync::Arc;
use std::time::Duration;

use async_lock::{Mutex, Semaphore};
use bytes::Bytes;
use futures::prelude::*;
use http::{header, header::HeaderName, HeaderMap, HeaderValue, StatusCode};
use hyperx::header::{ContentRange, ContentRangeSpec, Header as _};
use reqwest::{Body, Url};
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
#[cfg(doc)]
use reqwest_retry::policies::ExponentialBackoffBuilder;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry_after::RetryAfterMiddleware;
use reqwest_tracing::TracingMiddleware;
use sha2::{Digest, Sha256};
use tracing::{debug, instrument, warn};

use crate::{Error, Result};

use serde::{Deserialize, Serialize};

const BASE_URL_PATH: &str = "/_apis/artifactcache/";
const DEFAULT_USER_AGENT: &str = concat!(env!("CARGO_CRATE_NAME"), "/", env!("CARGO_PKG_VERSION"));
const DEFAULT_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_UPLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// GitHub Actions cache entry.
///
/// See [module][self] documentation.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactCacheEntry {
    /// Cache key for looking up cache entries by the key prefix.
    pub cache_key: Option<String>,

    /// Scope for the cache entry, e.g. the source filename or a hash of the
    /// source file(s).
    pub scope: Option<String>,

    /// Creation time for the cache entry.
    pub creation_time: Option<String>,

    /// URL for downloading the cache archive.
    pub archive_location: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommitCacheRequest {
    pub size: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReserveCacheRequest<'a> {
    pub key: &'a str,
    pub version: &'a str,
    pub cache_size: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReserveCacheResponse {
    pub cache_id: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheQuery<'a> {
    pub keys: &'a str,
    pub version: &'a str,
}

/// GitHub Actions cache client builder.
///
/// See [module][self] documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheClientBuilder {
    /// GitHub Actions cache API base URL.
    pub base_url: String,

    /// GitHub Actions access token.
    pub token: String,

    /// User agent for HTTP requests.
    pub user_agent: String,

    /// Cache key to write.
    pub cache_to: Option<String>,

    /// Cache key prefixes to read.
    pub cache_from: Vec<String>,

    /// Maximum number of retries.
    pub max_retries: u32,

    /// Minimum retry interval. See [`ExponentialBackoff::min_retry_interval`].
    pub min_retry_interval: Duration,

    /// Maximum retry interval. See [`ExponentialBackoff::max_retry_interval`].
    pub max_retry_interval: Duration,

    /// Retry backoff factor base. See [`ExponentialBackoff::backoff_exponent`].
    pub backoff_factor_base: u32,

    /// Maximum chunk size in bytes for downloads.
    pub download_chunk_size: u64,

    /// Maximum time for each chunk download request.
    pub download_chunk_timeout: Duration,

    /// Number of parallel downloads.
    pub download_concurrency: u32,

    /// Maximum chunk size in bytes for uploads.
    pub upload_chunk_size: u64,

    /// Maximum time for each chunk upload request.
    pub upload_chunk_timeout: Duration,

    /// Number of parallel uploads.
    pub upload_concurrency: u32,
}

impl Default for CacheClientBuilder {
    fn default() -> Self {
        Self {
            base_url: Default::default(),
            token: Default::default(),
            user_agent: DEFAULT_USER_AGENT.into(),
            cache_to: None,
            cache_from: vec![],
            max_retries: 2,
            min_retry_interval: Duration::from_millis(50),
            max_retry_interval: Duration::from_secs(10),
            backoff_factor_base: 3,
            download_chunk_size: 4 << 20, // 4 MiB
            download_chunk_timeout: DEFAULT_DOWNLOAD_TIMEOUT,
            download_concurrency: 8,
            upload_concurrency: 4,
            upload_chunk_size: 1 << 20, // 1 MiB
            upload_chunk_timeout: DEFAULT_UPLOAD_TIMEOUT,
        }
    }
}

impl CacheClientBuilder {
    /// Creates a new [`CacheClientBuilder`] for the given GitHub Actions cache
    /// API base URL and access token.
    pub fn new<B: Into<String>, T: Into<String>>(base_url: B, token: T) -> Self {
        Self {
            base_url: base_url.into(),
            token: token.into(),
            ..Default::default()
        }
    }

    /// Creates a new [`CacheClientBuilder`] from GitHub Actions cache
    /// environmental variables.
    ///
    /// The following environmental variables are read:
    ///
    /// - `ACTIONS_CACHE_URL` - GitHub Actions cache API base URL
    /// - `ACTIONS_RUNTIME_TOKEN` - GitHub Actions access token
    /// - `SEGMENT_DOWNLOAD_TIMEOUT_MINS` - download chunk timeout
    ///
    pub fn from_env() -> Result<Self> {
        let url = env::var("ACTIONS_CACHE_URL").map_err(|source| Error::VarError {
            source,
            name: "ACTIONS_CACHE_URL",
        })?;
        let token = env::var("ACTIONS_RUNTIME_TOKEN").map_err(|source| Error::VarError {
            source,
            name: "ACTIONS_RUNTIME_TOKEN",
        })?;

        let mut builder = CacheClientBuilder::new(&url, &token);

        if let Some(timeout) = std::env::var("SEGMENT_DOWNLOAD_TIMEOUT_MINS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(|v: u64| Duration::from_secs(v * 60))
        {
            builder.download_chunk_timeout = timeout;
        }

        Ok(builder)
    }

    /// Sets the GitHub Actions cache API base URL.
    pub fn base_url<T: Into<String>>(mut self, base_url: T) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Sets the cache key prefixes to read.
    pub fn token<T: Into<String>>(mut self, token: T) -> Self {
        self.token = token.into();
        self
    }

    /// Sets the user agent for HTTP requests.
    pub fn user_agent<T: Into<String>>(mut self, user_agent: T) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// Sets the cache key to write.
    pub fn cache_to<T: Into<String>>(mut self, cache_to: T) -> Self {
        self.cache_to = Some(cache_to.into());
        self
    }
    /// Sets the cache key prefixes to read.
    pub fn cache_from<T>(mut self, cache_from: T) -> Self
    where
        T: Iterator,
        T::Item: Into<String>,
    {
        self.cache_from = cache_from.map(Into::into).collect();
        self
    }

    /// Sets the maximum number of retries.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the minimum retry interval.
    pub fn min_retry_interval(mut self, min_retry_interval: Duration) -> Self {
        self.min_retry_interval = min_retry_interval;
        self
    }

    /// Sets the maximum retry interval.
    pub fn max_retry_interval(mut self, max_retry_interval: Duration) -> Self {
        self.max_retry_interval = max_retry_interval;
        self
    }

    /// Sets the retry backoff factor base.
    pub fn backoff_factor_base(mut self, backoff_factor_base: u32) -> Self {
        self.backoff_factor_base = backoff_factor_base;
        self
    }

    /// Maximum chunk size in bytes for downloads.
    pub fn download_chunk_size(mut self, download_chunk_size: u64) -> Self {
        self.download_chunk_size = download_chunk_size;
        self
    }

    /// Sets the maximum time for each chunk download request.
    pub fn download_chunk_timeout(mut self, download_chunk_timeout: Duration) -> Self {
        self.download_chunk_timeout = download_chunk_timeout;
        self
    }

    /// Sets the number of parallel downloads.
    pub fn download_concurrency(mut self, download_concurrency: u32) -> Self {
        self.download_concurrency = download_concurrency;
        self
    }

    /// Sets the maximum chunk size in bytes for uploads.
    pub fn upload_chunk_size(mut self, upload_chunk_size: u64) -> Self {
        self.upload_chunk_size = upload_chunk_size;
        self
    }

    /// Sets the maximum time for each chunk upload request.
    pub fn upload_chunk_timeout(mut self, upload_chunk_timeout: Duration) -> Self {
        self.upload_chunk_timeout = upload_chunk_timeout;
        self
    }

    /// Sets the number of parallel downloads.
    pub fn upload_concurrency(mut self, upload_concurrency: u32) -> Self {
        self.upload_concurrency = upload_concurrency;
        self
    }

    /// Consumes this [`CacheClientBuilder`] and build a [`CacheClient`].
    pub fn build(self) -> Result<CacheClient> {
        self.try_into()
    }
}

/// GitHub Actions cache client.
///
/// See [module][self] documentation.
pub struct CacheClient {
    client: ClientWithMiddleware,
    base_url: Url,
    api_headers: HeaderMap,

    cache_to: Option<String>,
    cache_from: Option<String>,

    download_chunk_size: u64,
    download_chunk_timeout: Duration,
    download_concurrency: u32,

    upload_chunk_size: u64,
    upload_chunk_timeout: Duration,
    upload_concurrency: u32,
}

impl TryInto<CacheClient> for CacheClientBuilder {
    type Error = Error;

    fn try_into(self) -> Result<CacheClient, Self::Error> {
        if self.cache_to.is_none() && self.cache_from.is_empty() {
            return Err(Error::MissingKey);
        }

        let cache_to = if let Some(cache_to) = self.cache_to {
            check_key(&cache_to)?;
            Some(cache_to)
        } else {
            None
        };

        let cache_from = if !self.cache_from.is_empty() {
            for key in &self.cache_from {
                check_key(key)?;
            }
            Some(self.cache_from.join(","))
        } else {
            None
        };

        let mut api_headers = HeaderMap::new();
        api_headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json;api-version=6.0-preview.1"),
        );

        let auth_value = Bytes::from(format!("Bearer {}", self.token));
        let mut auth_value = header::HeaderValue::from_maybe_shared(auth_value)?;
        auth_value.set_sensitive(true);
        api_headers.insert(http::header::AUTHORIZATION, auth_value);

        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(self.min_retry_interval, self.max_retry_interval)
            .backoff_exponent(self.backoff_factor_base)
            .build_with_max_retries(self.max_retries);

        let client = reqwest::ClientBuilder::new()
            .user_agent(self.user_agent)
            .build()?;
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with(TracingMiddleware::default())
            .with(RetryAfterMiddleware::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let base_url = Url::parse(&format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            BASE_URL_PATH
        ))?;

        Ok(CacheClient {
            client,
            base_url,
            api_headers,
            cache_to,
            cache_from,
            download_chunk_size: self.download_chunk_size,
            download_chunk_timeout: self.download_chunk_timeout,
            download_concurrency: self.download_concurrency,
            upload_concurrency: self.upload_concurrency,
            upload_chunk_timeout: self.upload_chunk_timeout,
            upload_chunk_size: self.upload_chunk_size,
        })
    }
}

impl CacheClient {
    /// Creates a new [`CacheClientBuilder`].
    ///
    /// See [`CacheClientBuilder::new`].
    pub fn builder<B: Into<String>, T: Into<String>>(base_url: B, token: T) -> CacheClientBuilder {
        CacheClientBuilder::new(base_url, token)
    }

    /// Creates a new [`CacheClientBuilder`] from environmental variables.
    ///
    /// See [`CacheClientBuilder::from_env`].
    pub fn from_env() -> Result<CacheClientBuilder> {
        CacheClientBuilder::from_env()
    }

    /// Gets the GitHub Actions cache API base URL.
    ///
    /// See [`CacheClientBuilder::base_url`].
    pub fn base_url(&self) -> &str {
        let base_url = self.base_url.as_str();
        &base_url[..base_url.len() - BASE_URL_PATH.len()]
    }

    /// Gets the cache key to write.
    ///
    /// See [`CacheClientBuilder::cache_to`].
    pub fn cache_to(&self) -> Option<&str> {
        self.cache_to.as_deref()
    }

    /// Gets the cache key prefixes to read.
    ///
    /// See [`CacheClientBuilder::cache_from`].
    pub fn cache_from(&self) -> Option<&str> {
        self.cache_from.as_deref()
    }

    /// Gets the cache entry identified by the given `version`.
    #[instrument(skip(self))]
    pub async fn entry(&self, version: &str) -> Result<Option<ArtifactCacheEntry>> {
        let cache_from = if let Some(cache_from) = self.cache_from.as_ref() {
            cache_from
        } else {
            return Ok(None);
        };

        let query = serde_urlencoded::to_string(&CacheQuery {
            keys: cache_from,
            version: &get_cache_version(version),
        })?;

        let mut url = self.base_url.join("cache")?;
        url.set_query(Some(&query));

        let response = self
            .client
            .get(url)
            .headers(self.api_headers.clone())
            .send()
            .await?;
        let status = response.status();
        if status == http::StatusCode::NO_CONTENT {
            return Ok(None);
        };
        if !status.is_success() {
            let message = response.text().await.unwrap_or_else(|err| err.to_string());
            return Err(Error::CacheServiceStatus { status, message });
        }

        let cache_result: ArtifactCacheEntry = response.json().await?;
        debug!("Cache Result: {}", serde_json::to_string(&cache_result)?);

        if let Some(cache_download_url) = cache_result.archive_location.as_ref() {
            println!(
                "::add-mask::{}",
                shell_escape::escape(cache_download_url.into())
            );
        } else {
            return Err(Error::CacheNotFound);
        }

        Ok(Some(cache_result))
    }

    /// Gets the cache archive as a byte array.
    #[instrument(skip(self))]
    pub async fn get(&self, url: &str) -> Result<Vec<u8>> {
        let uri = Url::parse(url)?;

        let (data, cache_size) = self.download_first_chunk(uri.clone()).await?;

        if cache_size.is_none() {
            return Ok(data.to_vec());
        }

        if let Some(ContentRange(ContentRangeSpec::Bytes {
            instance_length: Some(cache_size),
            ..
        })) = cache_size
        {
            let actual_size = data.len() as u64;
            if actual_size == cache_size {
                return Ok(data.to_vec());
            }
            if actual_size != self.download_chunk_size {
                return Err(Error::CacheChunkSize {
                    expected_size: self.download_chunk_size as usize,
                    actual_size: actual_size as usize,
                    message: "verifying the first chunk size using the content-range header",
                });
            }

            // Download chunks in parallel
            if cache_size as usize
                <= self.download_chunk_size as usize * self.download_concurrency as usize
            {
                let mut chunks = Vec::new();
                let mut start = self.download_chunk_size;
                while start < cache_size {
                    let chunk_size = u64::min(cache_size, self.download_chunk_size);
                    let uri = uri.clone();
                    chunks.push(self.download_chunk(uri, start, chunk_size));
                    start += self.download_chunk_size;
                }

                let mut chunks = future::try_join_all(chunks.into_iter()).await?;
                chunks.insert(0, data);

                return Ok(chunks.concat());
            }

            // Download chunks with max concurrency
            let permit = Arc::new(Semaphore::new(self.download_concurrency as usize));

            let mut chunks = Vec::new();
            let mut start = self.download_chunk_size;
            while start < cache_size {
                let chunk_size = u64::min(cache_size, self.download_chunk_size);
                let uri = uri.clone();
                let permit = permit.clone();

                chunks.push(async move {
                    let _guard = permit.acquire().await;
                    self.download_chunk(uri, start, chunk_size).await
                });

                start += self.upload_chunk_size;
            }

            let mut chunks = future::try_join_all(chunks).await?;
            chunks.insert(0, data);

            return Ok(chunks.concat());
        }

        debug!("Unable to validate download, no Content-Range header or unknown size");

        let actual_size = data.len() as u64;
        if actual_size < self.download_chunk_size {
            return Ok(data.to_vec());
        }
        if actual_size != self.download_chunk_size {
            return Err(Error::CacheChunkSize {
                expected_size: self.download_chunk_size as usize,
                actual_size: actual_size as usize,
                message: "verifying the first chunk size without the content-range header",
            });
        }

        let mut start = self.download_chunk_size;
        let mut chunks = vec![data];
        loop {
            let chunk = self
                .download_chunk(uri.clone(), start, self.download_chunk_size)
                .await?;
            if chunk.is_empty() {
                break;
            }

            let chunk_size = chunk.len() as u64;
            chunks.push(chunk);

            if chunk_size < self.download_chunk_size {
                break;
            }
            if chunk_size != self.download_chunk_size {
                return Err(Error::CacheChunkSize {
                    expected_size: self.download_chunk_size as usize,
                    actual_size: chunk_size as usize,
                    message: "verifying a chunk size without the content-range header",
                });
            }

            start += self.download_chunk_size;
        }

        Ok(chunks.concat())
    }

    #[instrument(skip(self, uri))]
    async fn download_first_chunk(&self, uri: Url) -> Result<(Bytes, Option<ContentRange>)> {
        self.do_download_chunk(uri, 0, self.download_chunk_size, true)
            .await
    }

    #[instrument(skip_all, fields(uri, start, size))]
    async fn download_chunk(&self, uri: Url, start: u64, size: u64) -> Result<Bytes> {
        let (bytes, _) = self.do_download_chunk(uri, start, size, false).await?;
        Ok(bytes)
    }

    #[instrument(skip(self, uri))]
    async fn do_download_chunk(
        &self,
        uri: Url,
        start: u64,
        size: u64,
        expect_partial: bool,
    ) -> Result<(Bytes, Option<ContentRange>)> {
        let range = format!("bytes={start}-{}", start + size - 1);

        let response = self
            .client
            .get(uri)
            .header(header::RANGE, HeaderValue::from_str(&range)?)
            .header(
                HeaderName::from_static("x-ms-range-get-content-md5"),
                HeaderValue::from_static("true"),
            )
            .timeout(self.download_chunk_timeout)
            .send()
            .await?;

        let status = response.status();
        let partial_content = expect_partial && status == StatusCode::PARTIAL_CONTENT;
        if !status.is_success() {
            let message = response.text().await.unwrap_or_else(|err| err.to_string());
            return Err(Error::CacheServiceStatus { status, message });
        }

        let content_length = response.content_length();
        let headers = response.headers();

        let content_range = if partial_content {
            headers
                .get(header::CONTENT_RANGE)
                .and_then(|v| ContentRange::parse_header(&v).ok())
        } else {
            Some(ContentRange(ContentRangeSpec::Bytes {
                range: None,
                instance_length: content_length,
            }))
        };

        let md5sum = response
            .headers()
            .get(HeaderName::from_static("content-md5"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| hex::decode(s).ok());

        let bytes = response.bytes().await?;
        let actual_size = bytes.len() as u64;
        if actual_size != content_length.unwrap_or(actual_size) || actual_size > size {
            return Err(Error::CacheChunkSize {
                expected_size: size as usize,
                actual_size: bytes.len(),
                message: if expect_partial {
                    "downloading a chunk"
                } else {
                    "downloading the first chunk"
                },
            });
        }

        if let Some(md5sum) = md5sum {
            use md5::Digest as _;
            let checksum = md5::Md5::digest(&bytes);
            if md5sum[..] != checksum[..] {
                return Err(Error::CacheChunkChecksum);
            }
        }

        Ok((bytes, content_range))
    }

    /// Puts the cache archive as the given `version`.
    #[instrument(skip(self, data))]
    pub async fn put<T: Read + Seek>(&self, version: &str, mut data: T) -> Result<()> {
        let cache_to = if let Some(cache_to) = self.cache_to.as_ref() {
            cache_to
        } else {
            return Ok(());
        };

        let cache_size = data.seek(SeekFrom::End(0))?;
        if cache_size > i64::MAX as u64 {
            return Err(Error::CacheSizeTooLarge(cache_size as usize));
        }

        let version = &get_cache_version(version);
        let cache_id = self.reserve(cache_to, version, cache_size).await?;

        if let Some(cache_id) = cache_id {
            data.rewind()?;
            self.upload(cache_id, cache_size, data).await?;
            self.commit(cache_id, cache_size).await?;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn reserve(&self, key: &str, version: &str, cache_size: u64) -> Result<Option<i64>> {
        let url = self.base_url.join("caches")?;

        let reserve_cache_request = ReserveCacheRequest {
            key,
            version,
            cache_size: cache_size as i64,
        };

        let response = self
            .client
            .post(url)
            .headers(self.api_headers.clone())
            .json(&reserve_cache_request)
            .send()
            .await?;

        let status = response.status();
        match status {
            http::StatusCode::NO_CONTENT | http::StatusCode::CONFLICT => {
                warn!("No cache ID for key {} version {version}: {status:?}", key);
                return Ok(None);
            }
            _ if !status.is_success() => {
                let message = response.text().await.unwrap_or_else(|err| err.to_string());
                return Err(Error::CacheServiceStatus { status, message });
            }
            _ => {}
        }

        let ReserveCacheResponse { cache_id } = response.json().await?;
        Ok(Some(cache_id))
    }

    #[instrument(skip(self, data))]
    async fn upload<T: Read + Seek>(
        &self,
        cache_id: i64,
        cache_size: u64,
        mut data: T,
    ) -> Result<()> {
        let uri = self.base_url.join(&format!("caches/{cache_id}"))?;

        // Upload all data
        if cache_size <= self.upload_chunk_size {
            let mut buf = Vec::new();
            let _ = data.read_to_end(&mut buf)?;
            return self.upload_chunk(uri, buf, 0, cache_size).await;
        }

        // Upload chunks in parallel
        if cache_size as usize <= self.upload_chunk_size as usize * self.upload_concurrency as usize
        {
            let mut chunks = Vec::new();
            let mut start = 0;
            while start < cache_size {
                let mut chunk = Vec::new();
                let chunk_size = u64::min(cache_size, self.upload_chunk_size);
                let _ = (&mut data).take(chunk_size).read_to_end(&mut chunk)?;
                chunks.push(self.upload_chunk(uri.clone(), chunk, start, chunk_size));
                start += self.upload_chunk_size;
            }

            let _ = future::try_join_all(chunks).await?;

            return Ok(());
        }

        // Upload chunks with max concurrency
        let data = Arc::new(Mutex::new(data));
        let permit = Arc::new(Semaphore::new(self.upload_concurrency as usize));

        let mut chunks = Vec::new();
        let mut start = 0;
        while start < cache_size {
            let chunk_size = u64::min(cache_size, self.upload_chunk_size);
            let uri = uri.clone();
            let data = data.clone();
            let permit = permit.clone();

            chunks.push(async move {
                let _guard = permit.acquire().await;

                let mut data = data.lock().await;
                let data = data.deref_mut();

                let mut chunk = Vec::new();
                let _ = data.seek(SeekFrom::Start(start))?;
                let _ = data.take(chunk_size).read_to_end(&mut chunk)?;

                self.upload_chunk(uri, chunk, start, chunk_size).await
            });

            start += self.upload_chunk_size;
        }

        let _ = future::try_join_all(chunks).await?;

        Ok(())
    }

    #[instrument(skip(self, uri, body))]
    async fn upload_chunk<T: Into<Body>>(
        &self,
        uri: Url,
        body: T,
        start: u64,
        size: u64,
    ) -> Result<()> {
        let content_range = format!("bytes {start}-{}/*", start + size - 1);

        let response = self
            .client
            .patch(uri)
            .headers(self.api_headers.clone())
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            )
            .header(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&content_range)?,
            )
            .body(body)
            .timeout(self.upload_chunk_timeout)
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let message = response.text().await.unwrap_or_else(|err| err.to_string());
            Err(Error::CacheServiceStatus { status, message })
        }
    }

    #[instrument(skip(self))]
    async fn commit(&self, cache_id: i64, cache_size: u64) -> Result<()> {
        let url = self.base_url.join(&format!("caches/{cache_id}"))?;
        let commit_cache_request = CommitCacheRequest {
            size: cache_size as i64,
        };

        let response = self
            .client
            .post(url)
            .headers(self.api_headers.clone())
            .json(&commit_cache_request)
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let message = response.text().await.unwrap_or_else(|err| err.to_string());
            Err(Error::CacheServiceStatus { status, message })
        }
    }
}

fn get_cache_version(version: &str) -> String {
    let mut hasher = Sha256::new();

    hasher.update(version);
    hasher.update("|");

    // Add salt to cache version to support breaking changes in cache entry
    hasher.update(env!("CARGO_PKG_VERSION_MAJOR"));
    hasher.update(".");
    hasher.update(env!("CARGO_PKG_VERSION_MINOR"));

    let result = hasher.finalize();
    hex::encode(&result[..])
}

pub fn check_key(key: &str) -> Result<()> {
    if key.len() > 512 {
        return Err(Error::InvalidKeyLength(key.to_string()));
    }
    if key.chars().any(|c| c == ',') {
        return Err(Error::InvalidKeyComma(key.to_string()));
    }
    Ok(())
}
