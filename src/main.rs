use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use futures::stream::{FuturesUnordered, StreamExt};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};

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

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let bucket_name = event.s3_bucket_name;
    let folder = event.folder;
    let find = event.find.as_deref();

    let response = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(&folder)
        .send()
        .await?;

    let keys = response
        .contents.unwrap_or_default()
        .into_iter()
        .filter_map(|obj| obj.key)
        .collect::<Vec<String>>();

    let get_object_futures = FuturesUnordered::new();

    for key in keys {
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        let key = key.clone();

        let get_object_future = async move {
            let resp = get(&client, &bucket_name, &key, find).await?;
            Ok::<Option<String>, Error>(resp)
        };

        get_object_futures.push(get_object_future);
    }

    let responses: Vec<Result<Option<String>, Error>> = get_object_futures
        .map(|result| result)
        .collect()
        .await;
    if find.is_some() {
        return if let Some(result) = responses
            .into_iter()
            .find_map(Result::transpose)
            .and_then(Result::ok)
        {
            Ok(result)
        } else {
            Ok("Magic string not found.".parse().unwrap())
        };
    }
    Ok(responses.len().to_string())
}

async fn get(
    client: &Client,
    bucket_name: &str,
    key: &str,
    find: Option<&str>,
) -> Result<Option<String>, Error> {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?;

    let aggregated_bytes = resp.body.collect().await?;
    let data_bytes = aggregated_bytes.into_bytes();
    let data_vec = data_bytes.to_vec();
    let response_body = String::from_utf8(data_vec)?;

    if let Some(find_str) = find {
        match response_body.find(find_str) {
            Some(_) => Ok(Some(key.to_string())),
            None => Ok(None),
        }
    } else {
        Ok(None)
    }
}
