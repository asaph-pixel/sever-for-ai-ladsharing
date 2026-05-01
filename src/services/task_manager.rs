use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use tokio::sync::Mutex;

use crate::models::tasks::{
    CreateTaskRequest, StatusResponse, SubmitResultRequest, Task, TaskStatus,
};

const BETA_USER_LIMIT: usize = 4;

#[derive(Clone)]
pub struct TaskStore {
    next_id: Arc<AtomicU64>,
    state: Arc<Mutex<TaskState>>,
}

#[derive(Default)]
struct TaskState {
    queue: VecDeque<u64>,
    tasks: HashMap<u64, Task>,
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(Mutex::new(TaskState::default())),
        }
    }

    pub async fn create_task(&self, request: CreateTaskRequest) -> Result<Task, String> {
        let user_id = clean_or_default(request.user_id, "anonymous");
        let task_type = clean_or_default(request.task_type, "compute");
        let payload = request.payload.unwrap_or_else(|| json!({}));
        let mut state = self.state.lock().await;

        if !state.user_can_queue(&user_id) {
            return Err(format!(
                "beta limit reached: only {BETA_USER_LIMIT} active users can queue work"
            ));
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let task = Task {
            id,
            user_id,
            task_type,
            payload,
            status: TaskStatus::Queued,
            result: None,
            created_at_ms: now_ms(),
            started_at_ms: None,
            completed_at_ms: None,
        };

        state.queue.push_back(id);
        state.tasks.insert(id, task.clone());
        Ok(task)
    }

    pub async fn fetch_next_task(&self) -> Option<Task> {
        let mut state = self.state.lock().await;

        while let Some(id) = state.queue.pop_front() {
            if let Some(task) = state.tasks.get_mut(&id) {
                if task.status == TaskStatus::Queued {
                    task.status = TaskStatus::Running;
                    task.started_at_ms = Some(now_ms());
                    return Some(task.clone());
                }
            }
        }

        None
    }

    pub async fn submit_result(&self, request: SubmitResultRequest) -> Result<Task, String> {
        let mut state = self.state.lock().await;
        match state.tasks.get_mut(&request.task_id) {
            Some(task) => {
                task.status = TaskStatus::Completed;
                task.result = Some(request.result);
                task.completed_at_ms = Some(now_ms());
                Ok(task.clone())
            }
            None => Err("task not found".to_string()),
        }
    }

    pub async fn status(&self) -> StatusResponse {
        let state = self.state.lock().await;
        StatusResponse {
            queued: state.count_status(TaskStatus::Queued),
            running: state.count_status(TaskStatus::Running),
            completed: state.count_status(TaskStatus::Completed),
            active_users: state.active_users().len(),
            beta_user_limit: BETA_USER_LIMIT,
            total_tasks: state.tasks.len(),
        }
    }

    pub async fn list_tasks(&self) -> Vec<Task> {
        let state = self.state.lock().await;
        let mut tasks = state.tasks.values().cloned().collect::<Vec<_>>();
        tasks.sort_by(|left, right| right.id.cmp(&left.id));
        tasks
    }
}

impl TaskState {
    fn user_can_queue(&self, user_id: &str) -> bool {
        let users = self.active_users();
        users.contains(user_id) || users.len() < BETA_USER_LIMIT
    }

    fn active_users(&self) -> HashSet<String> {
        self.tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Queued | TaskStatus::Running))
            .map(|task| task.user_id.clone())
            .collect()
    }

    fn count_status(&self, status: TaskStatus) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status == status)
            .count()
    }
}

fn clean_or_default(value: Option<String>, default: &str) -> String {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
