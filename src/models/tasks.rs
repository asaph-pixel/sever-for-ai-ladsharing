use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct TaskRequest {
    pub task_type: String,
    pub input_files: Vec<String>,
    pub parameters: String,
}

#[derive(Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: u64,
    pub output_files: Vec<String>,
}