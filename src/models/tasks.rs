use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskPriority {
    High,
    Medium,
    Low,
}

#[derive(Clone, Deserialize)]
pub struct CreateTaskRequest {
    pub user_id: Option<String>,
    pub file_name: Option<String>,
    pub quality: Option<String>,
    pub priority: Option<TaskPriority>,
    pub payload: Option<Value>,
    pub webhook_url: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub api_keys: Vec<ApiKey>,
}

#[derive(Clone, Deserialize)]
pub struct CreateApiKeyRequest {
    pub label: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct SubmitResultRequest {
    pub task_id: u64,
    pub result: Value,
    pub success: Option<bool>,
    pub reason: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct HeartbeatRequest {
    pub session_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: u64,
    pub user_id: String,
    pub file_name: String,
    pub quality: String,
    pub priority: TaskPriority,
    pub payload: Value,
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub failure_reason: Option<String>,
    pub webhook_url: Option<String>,
    pub created_at_ms: u128,
    pub started_at_ms: Option<u128>,
    pub completed_at_ms: Option<u128>,
    pub estimated_completion_ms: Option<u128>,
}

#[derive(Serialize)]
pub struct FetchTaskResponse {
    pub task: Option<Task>,
}

#[derive(Clone, Serialize)]
pub struct ApiKey {
    pub key: String,
    pub label: String,
    pub created_at_ms: u128,
}

#[derive(Clone, Deserialize)]
pub struct WaitlistRequest {
    pub name: Option<String>,
    pub email: String,
    pub use_case: Option<String>,
}

#[derive(Serialize)]
pub struct WaitlistResponse {
    pub message: String,
}

#[derive(Serialize)]
pub struct TaskListResponse {
    pub items: Vec<Task>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

#[derive(Clone, Deserialize)]
pub struct TaskFilterQuery {
    pub format: Option<String>,
    pub status: Option<String>,
    pub user_id: Option<String>,
    pub from_ms: Option<u128>,
    pub to_ms: Option<u128>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub active_users: usize,
    pub beta_user_limit: usize,
    pub total_tasks: usize,
}

#[derive(Serialize)]
pub struct ApiError {
    pub error: String,
}
