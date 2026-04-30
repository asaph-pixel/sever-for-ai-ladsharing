use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskPriority {
    Low,
    High,
    Enterprise,
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::Low
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    Local,
    Cloud,
    Hybrid,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Cloud
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TaskRouteInfo {
    pub node_type: String,
    pub node_label: String,
    pub estimated_cost: f64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TaskRequest {
    pub task_type: String,
    pub input_files: Vec<String>,
    pub parameters: String,
    #[serde(default)]
    pub priority: TaskPriority,
    #[serde(default)]
    pub execution_mode: ExecutionMode,
}

#[derive(Clone, Deserialize)]
pub struct LocalTestRequest {
    pub dataset_size_mb: u32,
    pub iterations: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredTask {
    pub task_id: u64,
    pub task_type: String,
    pub input_files: Vec<String>,
    pub parameters: String,
    pub priority: TaskPriority,
    pub execution_mode: ExecutionMode,
    pub status: TaskStatus,
    pub progress_pct: u8,
    pub route: TaskRouteInfo,
    pub output_files: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct AdaptiveParameters {
    pub recommended_priority: String,
    pub recommended_parallel_jobs: u8,
    pub recommended_batch_size: u16,
    pub preferred_execution_mode: String,
}

#[derive(Serialize)]
pub struct TaskSummary {
    pub total_tasks: usize,
    pub queued_tasks: usize,
    pub running_tasks: usize,
    pub completed_tasks: usize,
    pub online_users: usize,
    pub online_nodes: usize,
    pub active_nodes: usize,
    pub total_nodes: usize,
    pub total_compute_units: usize,
    pub local_test_runs: usize,
    pub total_estimated_cost: f64,
    pub adaptive_parameters: AdaptiveParameters,
}
