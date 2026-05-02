use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::sleep;

const DEFAULT_API_BASE_URL: &str = "https://sever-for-ai-ladsharing-1.onrender.com";

#[derive(Debug, Deserialize)]
struct FetchTaskResponse {
    task: Option<Task>,
}

#[derive(Debug, Deserialize)]
struct Task {
    id: u64,
    file_name: String,
    quality: String,
    payload: Value,
}

#[derive(Debug, Serialize)]
struct ResultRequest {
    task_id: u64,
    result: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_base_url = std::env::var("ZEPHOST_API_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_API_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string();
    let client = Client::new();

    loop {
        let response = client
            .get(format!("{api_base_url}/task"))
            .send()
            .await?
            .error_for_status()?
            .json::<FetchTaskResponse>()
            .await?;

        if let Some(task) = response.task {
            let sleep_secs = 2 + (task.id % 4);
            println!(
                "worker picked task #{} ({}, {})",
                task.id, task.file_name, task.quality
            );
            sleep(Duration::from_secs(sleep_secs)).await;

            let result = ResultRequest {
                task_id: task.id,
                result: json!({
                    "message": format!("processed {} at {} quality", task.file_name, task.quality),
                    "simulated_seconds": sleep_secs,
                    "input": task.payload
                }),
            };

            client
                .post(format!("{api_base_url}/result"))
                .json(&result)
                .send()
                .await?
                .error_for_status()?;

            println!("worker completed task #{}", task.id);
        } else {
            sleep(Duration::from_secs(2)).await;
        }
    }
}
