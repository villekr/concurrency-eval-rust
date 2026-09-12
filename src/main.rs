use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt};
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

    // List up to 1000 objects in a single request (bucket has max 1000 objects by requirement)
    let resp = client
        .list_objects_v2()
        .bucket(&*bucket)
        .prefix(&folder)
        .max_keys(1000)
        .send()
        .await?;

    // Preserve list order via index so find-mode returns the first matching key by list order.
    let keys: Vec<(usize, String)> = resp
        .contents()
        .iter()
        .filter_map(|obj| obj.key().map(|k| k.to_string()))
        .enumerate()
        .collect();

    // Bound in-flight reads to `cap`, while STILL fully reading every object body.
    let mut stream = stream::iter(keys.into_iter().map(|(idx, key_owned)| {
        let client = client.clone();
        let bucket = bucket.clone();
        let find_pat_cloned = find_pat.clone();
        async move {
            let matched = get(
                &client,
                bucket.as_ref(),
                &key_owned,
                find_pat_cloned.as_deref(),
            )
            .await?;
            Ok::<(usize, Option<String>), Error>((idx, matched))
        }
    }))
    .buffer_unordered(concurrency_cap());

    if find_pat.is_some() {
        return drain_find(&mut stream).await;
    }

    // No-find: fully read all bodies and count processed objects.
    let mut count = 0usize;
    while let Some(res) = stream.next().await {
        res?;
        count += 1;
    }
    Ok(count.to_string())
}

/// Bounded concurrency cap, identical formula across all language implementations:
/// cap = max(8, min(64, memory_MB / 32)); memory from Lambda env, default 1024.
fn concurrency_cap() -> usize {
    let memory_mb: usize = std::env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);
    (memory_mb / 32).clamp(8, 64)
}

/// Drain all read futures (each body is fully read) and return the first matching
/// key by list order (smallest index), or "None" if nothing matched.
async fn drain_find<S>(stream: &mut S) -> Result<String, Error>
where
    S: futures::Stream<Item = Result<(usize, Option<String>), Error>> + Unpin,
{
    let mut first: Option<(usize, String)> = None;
    while let Some(res) = stream.next().await {
        let (idx, matched) = res?;
        if let Some(key) = matched
            && first.as_ref().is_none_or(|(best, _)| idx < *best)
        {
            first = Some((idx, key));
        }
    }
    Ok(first
        .map(|(_, key)| key)
        .unwrap_or_else(|| "None".to_string()))
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
