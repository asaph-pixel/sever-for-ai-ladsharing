use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};

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
    success: bool,
    reason: Option<String>,
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
            println!(
                "worker picked task #{} ({}, {})",
                task.id, task.file_name, task.quality
            );

            let result = run_task(&client, &task).await;
            let request = ResultRequest {
                task_id: task.id,
                success: result.is_ok(),
                reason: result.as_ref().err().cloned(),
                result: result.unwrap_or_else(|reason| json!({ "error": reason })),
            };

            client
                .post(format!("{api_base_url}/result"))
                .json(&request)
                .send()
                .await?
                .error_for_status()?;

            println!("worker completed task #{}", task.id);
        } else {
            sleep(Duration::from_secs(2)).await;
        }
    }
}

async fn run_task(client: &Client, task: &Task) -> Result<Value, String> {
    let job_type = task
        .payload
        .get("job_type")
        .or_else(|| task.payload.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("ai_inference");

    match job_type {
        "ai_inference" | "inference" => run_ai_inference(client, task).await,
        other => Err(format!("unsupported job type: {other}")),
    }
}

async fn run_ai_inference(client: &Client, task: &Task) -> Result<Value, String> {
    let ollama_url = std::env::var("OLLAMA_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:11434".to_string())
        .trim_end_matches('/')
        .to_string();
    let model = task
        .payload
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("llama3.2");
    let prompt = task
        .payload
        .get("prompt")
        .and_then(Value::as_str)
        .or_else(|| task.payload.get("input").and_then(Value::as_str))
        .unwrap_or("Summarize what Zephost should do next in one sentence.");

    let response = timeout(
        Duration::from_secs(120),
        client
            .post(format!("{ollama_url}/api/generate"))
            .json(&json!({
                "model": model,
                "prompt": prompt,
                "stream": false
            }))
            .send(),
    )
    .await
    .map_err(|_| "local inference timed out after 120 seconds".to_string())?
    .map_err(|error| format!("local inference request failed: {error}"))?;

    let status = response.status();
    let body = response
        .json::<Value>()
        .await
        .map_err(|error| format!("could not parse local inference response: {error}"))?;
    if !status.is_success() {
        return Err(format!("local inference failed with {status}: {body}"));
    }

    Ok(json!({
        "job_type": "ai_inference",
        "model": model,
        "prompt": prompt,
        "response": body.get("response").cloned().unwrap_or(body),
        "input": task.payload
    }))
}
