use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
}

#[derive(Clone, Deserialize)]
pub struct CreateTaskRequest {
    pub user_id: Option<String>,
    pub task_type: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Clone, Deserialize)]
pub struct SubmitResultRequest {
    pub task_id: u64,
    pub result: Value,
}

#[derive(Clone, Deserialize)]
pub struct HeartbeatRequest {
    pub session_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: u64,
    pub user_id: String,
    pub task_type: String,
    pub payload: Value,
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub created_at_ms: u128,
    pub started_at_ms: Option<u128>,
    pub completed_at_ms: Option<u128>,
}

#[derive(Serialize)]
pub struct FetchTaskResponse {
    pub task: Option<Task>,
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
