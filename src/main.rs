use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use futures::stream::{FuturesUnordered, StreamExt};
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use memchr::memmem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct Event {
    s3_bucket_name: String,
    folder: String,
    find: Option<String>,
}

#[derive(Serialize)]
pub struct Response {
    lang: String,
    detail: String,
    result: String,
    time: f64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(function_handler)).await
}

pub async fn function_handler(event: LambdaEvent<Event>) -> Result<Response, Error> {
    let start = std::time::Instant::now();
    let result = processor(event.payload).await?;
    let elapsed = start.elapsed().as_secs_f64();
    let elapsed = format!("{:.1}", elapsed).parse::<f64>().unwrap();

    let response = Response {
        lang: "rust".to_string(),
        detail: "aws-sdk".to_string(),
        result,
        time: elapsed,
    };

    Ok(response)
}

async fn processor(event: Event) -> Result<String, Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("eu-north-1");

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    let bucket: Arc<str> = Arc::from(event.s3_bucket_name);
    let folder = event.folder;
    let find_pat: Option<Arc<[u8]>> = event
        .find
        .as_deref()
        .map(|s| Arc::<[u8]>::from(s.as_bytes()));

    #[allow(unused_mut)]
    let mut get_object_futures = FuturesUnordered::new();

    // List up to 1000 objects in a single request (bucket has max 1000 objects by requirement)
    let resp = client
        .list_objects_v2()
        .bucket(&*bucket)
        .prefix(&folder)
        .max_keys(1000)
        .send()
        .await?;

    for obj in resp.contents() {
        if let Some(key) = obj.key() {
            let client = client.clone();
            let bucket = bucket.clone();
            let key_owned = key.to_string();
            let find_pat_cloned = find_pat.clone();

            let fut = async move {
                let resp = get(
                    &client,
                    bucket.as_ref(),
                    &key_owned,
                    find_pat_cloned.as_deref(),
                )
                .await?;
                Ok::<Option<String>, Error>(resp)
            };
            get_object_futures.push(fut);
        }
    }

    // If searching, we must still fully read all bodies; track the first match while draining all
    if find_pat.is_some() {
        let mut first_match: Option<String> = None;
        while let Some(res) = get_object_futures.next().await {
            if let Ok(Some(key)) = res && first_match.is_none() {
                first_match = Some(key);
            }
        }
        return Ok(first_match.unwrap_or_else(|| "None".to_string()));
    }

    // Otherwise, ensure we fully read all bodies and count processed objects
    let mut count = 0usize;
    while let Some(_res) = get_object_futures.next().await {
        // We don't need the key; just ensure the future completed (body fully read)
        count += 1;
    }
    Ok(count.to_string())
}

async fn get(
    client: &Client,
    bucket_name: &str,
    key: &str,
    find: Option<&[u8]>,
) -> Result<Option<String>, Error> {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?;

    // Fully read the body into memory as required
    let aggregated_bytes = resp.body.collect().await?;
    let data_bytes = aggregated_bytes.into_bytes();

    if let Some(pattern) = find {
        if memmem::find(data_bytes.as_ref(), pattern).is_some() {
            Ok(Some(key.to_string()))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
