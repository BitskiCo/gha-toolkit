use gha_toolkit::cache::{CacheClient, CacheClientBuilder};

use std::io;
use std::time::SystemTime;

use tokio::test;

#[test]
async fn builder() {
    assert!(CacheClient::builder("http://localhost", "token")
        .build()
        .is_err());

    assert!(CacheClient::builder("http://localhost", "token")
        .cache_to("key")
        .build()
        .is_ok());

    assert!(CacheClient::builder("http://localhost", "token")
        .cache_from(["key"].into_iter())
        .build()
        .is_ok());

    assert!(CacheClient::builder("http://localhost", "token")
        .cache_to("key")
        .cache_from(["key"].into_iter())
        .build()
        .is_ok());
}

#[test]
async fn from_env() {
    let version = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis();
    let cache_key = format!("gha-toolkit-{version:#x}");

    let client = CacheClientBuilder::from_env()
        .unwrap()
        .cache_to(&cache_key)
        .cache_from([&cache_key].into_iter())
        .build()
        .unwrap();

    const CACHE_ENTRY: &str = "from_env";
    const CACHE_DATA: &str = "Hello World!";

    client
        .put(CACHE_ENTRY, io::Cursor::new(CACHE_DATA))
        .await
        .unwrap();

    let entry = client.entry(CACHE_ENTRY).await.unwrap().unwrap();

    let url = entry.archive_location.unwrap();
    let actual_cache_data = client.get(&url).await.unwrap();
    let actual_cache_data = String::from_utf8_lossy(&actual_cache_data);
    assert_eq!(&actual_cache_data, CACHE_DATA);
}

#[test]
async fn from_env_big_data() {
    let version = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis();
    let cache_key = format!("gha-toolkit-{version:#x}");

    let client = CacheClientBuilder::from_env()
        .unwrap()
        .cache_to(&cache_key)
        .cache_from([&cache_key].into_iter())
        .build()
        .unwrap();

    const CACHE_ENTRY: &str = "from_env_big_data";
    let cache_data = [42u8; 6144];

    client
        .put(CACHE_ENTRY, io::Cursor::new(&cache_data))
        .await
        .unwrap();

    let entry = client.entry(CACHE_ENTRY).await.unwrap().unwrap();

    let url = entry.archive_location.unwrap();
    let actual_cache_data = client.get(&url).await.unwrap();
    assert_eq!(&actual_cache_data, &cache_data);
}
