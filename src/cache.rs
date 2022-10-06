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
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use reqwest_retry_after::RetryAfterMiddleware;
use reqwest_tracing::TracingMiddleware;
use sha2::{Digest, Sha256};
use tracing::{debug, instrument, warn};

use crate::{Error, Result};

use serde::{Deserialize, Serialize};

const BASE_URL_PATH: &str = "/_apis/artifactcache/";

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactCacheEntry {
    pub cache_key: Option<String>,
    pub scope: Option<String>,
    pub creation_time: Option<String>,
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

pub struct CacheClientBuilder {
    user_agent: String,
    base_url: String,
    token: String,

    key: String,
    restore_keys: String,

    max_retries: u32,
    min_retry_interval: Duration,
    max_retry_interval: Duration,
    backoff_factor_base: u32,

    /// Maximum chunk size in bytes for downloads.
    download_chunk_size: u64,

    /// Maximum time for each chunk download request.
    download_chunk_timeout: Duration,

    /// Number of parallel downloads.
    download_concurrency: u32,

    /// Maximum chunk size in bytes for uploads.
    upload_chunk_size: u64,

    /// Maximum time for each chunk upload request.
    upload_chunk_timeout: Duration,

    /// Number of parallel uploads.
    upload_concurrency: u32,
}

impl CacheClientBuilder {
    pub fn new<B: Into<String>, T: Into<String>>(
        base_url: B,
        token: T,
        key: &str,
        restore_keys: &[&str],
    ) -> Result<Self> {
        for key in restore_keys {
            check_key(key)?;
        }

        let download_chunk_timeout = std::env::var("SEGMENT_DOWNLOAD_TIMEOUT_MINS")
            .ok()
            .and_then(|s| u64::from_str_radix(&s, 10).ok())
            .map(|v| Duration::from_secs(v * 60))
            .unwrap_or(Duration::from_secs(60));

        let restore_keys: Vec<String> = restore_keys.into_iter().map(|s| s.to_string()).collect();
        let restore_keys = restore_keys.join(",");

        Ok(Self {
            user_agent: format!("{}/{}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION")),
            base_url: base_url.into(),
            token: token.into(),
            key: key.to_string(),
            restore_keys,
            max_retries: 2,
            min_retry_interval: Duration::from_millis(50),
            max_retry_interval: Duration::from_secs(10),
            backoff_factor_base: 3,
            download_chunk_size: 4 << 20, // 4 MiB
            download_chunk_timeout,
            download_concurrency: 8,
            upload_concurrency: 4,
            upload_chunk_size: 1 << 20, // 1 MiB
            upload_chunk_timeout: download_chunk_timeout,
        })
    }

    pub fn user_agent<T: Into<String>>(mut self, user_agent: T) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    pub fn base_url<T: Into<String>>(mut self, base_url: T) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn token<T: Into<String>>(mut self, token: T) -> Self {
        self.token = token.into();
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn min_retry_interval(mut self, min_retry_interval: Duration) -> Self {
        self.min_retry_interval = min_retry_interval;
        self
    }

    pub fn max_retry_interval(mut self, max_retry_interval: Duration) -> Self {
        self.max_retry_interval = max_retry_interval;
        self
    }

    pub fn backoff_factor_base(mut self, backoff_factor_base: u32) -> Self {
        self.backoff_factor_base = backoff_factor_base;
        self
    }

    pub fn download_chunk_timeout(mut self, download_chunk_timeout: Duration) -> Self {
        self.download_chunk_timeout = download_chunk_timeout;
        self
    }

    pub fn download_chunk_size(mut self, download_chunk_size: u64) -> Self {
        self.download_chunk_size = download_chunk_size;
        self
    }

    pub fn download_concurrency(mut self, download_concurrency: u32) -> Self {
        self.download_concurrency = download_concurrency;
        self
    }

    pub fn upload_concurrency(mut self, upload_concurrency: u32) -> Self {
        self.upload_concurrency = upload_concurrency;
        self
    }

    pub fn upload_chunk_size(mut self, upload_chunk_size: u64) -> Self {
        self.upload_chunk_size = upload_chunk_size;
        self
    }

    pub fn upload_chunk_timeout(mut self, upload_chunk_timeout: Duration) -> Self {
        self.upload_chunk_timeout = upload_chunk_timeout;
        self
    }

    pub fn build(self) -> Result<CacheClient> {
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
            self.base_url.trim_end_matches("/"),
            BASE_URL_PATH
        ))?;

        Ok(CacheClient {
            client,
            base_url,
            api_headers,
            key: self.key,
            restore_keys: self.restore_keys,
            download_chunk_size: self.download_chunk_size,
            download_chunk_timeout: self.download_chunk_timeout,
            download_concurrency: self.download_concurrency,
            upload_concurrency: self.upload_concurrency,
            upload_chunk_timeout: self.upload_chunk_timeout,
            upload_chunk_size: self.upload_chunk_size,
        })
    }
}

pub struct CacheClient {
    client: ClientWithMiddleware,
    base_url: Url,
    api_headers: HeaderMap,

    key: String,
    restore_keys: String,

    download_chunk_size: u64,
    download_chunk_timeout: Duration,
    download_concurrency: u32,

    upload_chunk_size: u64,
    upload_chunk_timeout: Duration,
    upload_concurrency: u32,
}

impl CacheClient {
    pub fn builder<B: Into<String>, T: Into<String>>(
        base_url: B,
        token: T,
        key: &str,
        restore_keys: &[&str],
    ) -> Result<CacheClientBuilder> {
        CacheClientBuilder::new(base_url, token, key, restore_keys)
    }

    pub fn base_url(&self) -> &str {
        let base_url = self.base_url.as_str();
        &base_url[..base_url.len() - BASE_URL_PATH.len()]
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn restore_keys(&self) -> &str {
        &self.restore_keys
    }

    #[instrument(skip(self))]
    pub async fn entry(&self, version: &str) -> Result<Option<ArtifactCacheEntry>> {
        let query = serde_urlencoded::to_string(&CacheQuery {
            keys: &self.restore_keys,
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

    #[instrument(skip(self))]
    pub async fn get(&self, url: &str) -> Result<Vec<u8>> {
        let uri = Url::parse(url)?;

        let (data, cache_size) = self
            .download_first_chunk(uri.clone(), 0, self.download_chunk_size)
            .await?;

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
            if actual_size > cache_size {
                return Err(Error::CacheSize {
                    expected_size: cache_size as usize,
                    actual_size: actual_size as usize,
                });
            }
            if actual_size != self.download_chunk_size {
                return Err(Error::CacheChunkSize {
                    expected_size: self.download_chunk_size as usize,
                    actual_size: actual_size as usize,
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

                return Ok(chunks.concat().into());
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

            return Ok(chunks.concat().into());
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
                });
            }

            start += self.download_chunk_size;
        }

        Ok(chunks.concat().into())
    }

    #[instrument(skip(self, uri))]
    async fn download_first_chunk(
        &self,
        uri: Url,
        start: u64,
        size: u64,
    ) -> Result<(Bytes, Option<ContentRange>)> {
        self.do_download_chunk(uri, start, size, true).await
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
            .headers(self.api_headers.clone())
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

        let headers = response.headers();

        let content_range = if partial_content {
            headers
                .get(header::CONTENT_RANGE)
                .and_then(|v| ContentRange::parse_header(&v).ok())
        } else {
            None
        };

        let md5sum = response
            .headers()
            .get(HeaderName::from_static("content-md5"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| hex::decode(s).ok());

        let bytes = response.bytes().await?;
        if bytes.len() != size as usize {
            return Err(Error::CacheChunkSize {
                expected_size: size as usize,
                actual_size: bytes.len(),
            });
        }

        if let Some(md5sum) = md5sum {
            use md5::Digest as _;
            let checksum = md5::Md5::digest(&bytes);
            if &md5sum[..] != &checksum[..] {
                return Err(Error::CacheChunkChecksum);
            }
        }

        Ok((bytes, content_range))
    }

    #[instrument(skip(self, data))]
    pub async fn put<T: Read + Seek>(&self, version: &str, mut data: T) -> Result<()> {
        let cache_size = data.seek(SeekFrom::End(0))?;
        if cache_size > i64::MAX as u64 {
            return Err(Error::CacheSizeTooLarge(cache_size as usize));
        }

        let version = &get_cache_version(version);
        let cache_id = self.reserve(version, cache_size).await?;

        if let Some(cache_id) = cache_id {
            data.rewind()?;
            self.upload(cache_id, cache_size, data).await?;
            self.commit(cache_id, cache_size).await?;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn reserve(&self, version: &str, cache_size: u64) -> Result<Option<i64>> {
        let url = self.base_url.join("caches")?;

        let reserve_cache_request = ReserveCacheRequest {
            key: &self.key,
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
                warn!(
                    "No cache ID for key {} version {version}: {status:?}",
                    self.key
                );
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
            return Err(Error::CacheServiceStatus { status, message });
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
