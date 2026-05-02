use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::models::tasks::{
    ApiKey, CreateTaskRequest, HeartbeatRequest, LoginRequest, LoginResponse, StatusResponse,
    SubmitResultRequest, Task, TaskFilterQuery, TaskListResponse, TaskPriority, TaskStatus,
};

const BETA_USER_LIMIT: usize = 4;
const SESSION_TTL_MS: u128 = 30_000;
const TASK_RATE_LIMIT_PER_MINUTE: usize = 20;

#[derive(Clone)]
pub struct TaskStore {
    next_id: Arc<AtomicU64>,
    state: Arc<Mutex<TaskState>>,
    events: broadcast::Sender<String>,
    http_client: Client,
}

#[derive(Default)]
struct TaskState {
    queue: VecDeque<u64>,
    tasks: HashMap<u64, Task>,
    sessions: HashMap<String, u128>,
    users: HashMap<String, String>,
    auth_tokens: HashMap<String, String>,
    api_keys: HashMap<String, Vec<ApiKey>>,
    task_rate_window: HashMap<String, Vec<u128>>,
}

impl TaskStore {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(200);
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(Mutex::new(TaskState::default())),
            events,
            http_client: Client::new(),
        }
    }

    pub async fn create_task(&self, request: CreateTaskRequest) -> Result<Task, String> {
        let user_id = clean_or_default(request.user_id, "anonymous");
        let file_name = clean_or_default(request.file_name, "sample-file.mp4");
        let quality = clean_or_default(request.quality, "beta");
        let priority = request.priority.unwrap_or(TaskPriority::Medium);
        let payload = request.payload.unwrap_or_else(|| json!({}));
        let mut state = self.state.lock().await;

        if !state.user_can_queue(&user_id) {
            return Err(format!(
                "beta limit reached: only {BETA_USER_LIMIT} active users can queue work"
            ));
        }
        if !state.within_rate_limit(&user_id) {
            return Err("rate limit reached for this user (20 tasks / minute)".to_string());
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let estimated_completion_ms =
            state.estimate_completion(now_ms(), state.queue.len(), &priority, false);
        let task = Task {
            id,
            user_id,
            file_name,
            quality,
            priority,
            payload,
            status: TaskStatus::Queued,
            result: None,
            webhook_url: request.webhook_url,
            created_at_ms: now_ms(),
            started_at_ms: None,
            completed_at_ms: None,
            estimated_completion_ms: Some(estimated_completion_ms),
        };

        state.queue.push_back(id);
        state.tasks.insert(id, task.clone());
        let _ = self.events.send("task_created".to_string());
        Ok(task)
    }

    pub async fn cancel_task(&self, task_id: u64) -> Result<Task, String> {
        let mut state = self.state.lock().await;
        if let Some(task) = state.tasks.get_mut(&task_id) {
            if matches!(task.status, TaskStatus::Completed | TaskStatus::Cancelled) {
                return Err("task is already finalized".to_string());
            }
            task.status = TaskStatus::Cancelled;
            task.completed_at_ms = Some(now_ms());
            let cloned = task.clone();
            state.queue.retain(|queued_id| *queued_id != task_id);
            let _ = self.events.send("task_cancelled".to_string());
            Ok(cloned)
        } else {
            Err("task not found".to_string())
        }
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<StatusResponse, String> {
        let session_id = request.session_id.trim();
        if session_id.is_empty() {
            return Err("session_id is required".to_string());
        }

        let mut state = self.state.lock().await;
        state.prune_sessions();

        if !state.sessions.contains_key(session_id) && state.sessions.len() >= BETA_USER_LIMIT {
            return Err(format!(
                "beta limit reached: only {BETA_USER_LIMIT} active dashboard sessions are allowed"
            ));
        }

        state.sessions.insert(session_id.to_string(), now_ms());
        Ok(state.status_response())
    }

    pub async fn fetch_next_task(&self) -> Option<Task> {
        let mut state = self.state.lock().await;

        while let Some(id) = state.queue.pop_front() {
            if let Some(task) = state.tasks.get_mut(&id) {
                if task.status == TaskStatus::Queued {
                    task.status = TaskStatus::Running;
                    task.started_at_ms = Some(now_ms());
                    task.estimated_completion_ms =
                        Some(now_ms() + estimated_runtime_ms(&task.priority, true));
                    let _ = self.events.send("task_running".to_string());
                    return Some(task.clone());
                }
            }
        }

        None
    }

    pub async fn submit_result(&self, request: SubmitResultRequest) -> Result<Task, String> {
        let mut webhook: Option<String> = None;
        let mut state = self.state.lock().await;
        let outcome = match state.tasks.get_mut(&request.task_id) {
            Some(task) => {
                task.status = TaskStatus::Completed;
                task.result = Some(request.result);
                task.completed_at_ms = Some(now_ms());
                task.estimated_completion_ms = Some(now_ms());
                webhook = task.webhook_url.clone();
                Ok(task.clone())
            }
            None => Err("task not found".to_string()),
        };
        let _ = self.events.send("task_completed".to_string());
        drop(state);

        if let (Ok(task), Some(webhook_url)) = (&outcome, webhook) {
            let client = self.http_client.clone();
            let payload = json!({
                "task_id": task.id,
                "status": "completed",
                "result": task.result
            });
            tokio::spawn(async move {
                let _ = client.post(webhook_url).json(&payload).send().await;
            });
        }

        outcome
    }

    pub async fn status(&self) -> StatusResponse {
        let mut state = self.state.lock().await;
        state.prune_sessions();
        state.status_response()
    }

    pub async fn list_tasks(&self, query: TaskFilterQuery) -> TaskListResponse {
        let state = self.state.lock().await;
        let mut tasks = state.tasks.values().cloned().collect::<Vec<_>>();
        if let Some(status) = query.status.as_ref() {
            let status = status.to_lowercase();
            tasks.retain(|task| format!("{:?}", task.status).to_lowercase() == status);
        }
        if let Some(user_id) = query.user_id.as_ref() {
            tasks.retain(|task| task.user_id == *user_id);
        }
        if let Some(from_ms) = query.from_ms {
            tasks.retain(|task| task.created_at_ms >= from_ms);
        }
        if let Some(to_ms) = query.to_ms {
            tasks.retain(|task| task.created_at_ms <= to_ms);
        }
        tasks.sort_by(|left, right| right.id.cmp(&left.id));
        let total = tasks.len();
        let page = query.page.unwrap_or(1).max(1);
        let per_page = query.per_page.unwrap_or(20).max(1).min(100);
        let start = (page - 1) * per_page;
        let items = tasks.into_iter().skip(start).take(per_page).collect::<Vec<_>>();
        TaskListResponse {
            items,
            total,
            page,
            per_page,
        }
    }

    pub async fn export_tasks_json(&self, query: TaskFilterQuery) -> serde_json::Value {
        let list = self.list_tasks(query).await;
        json!(list.items)
    }

    pub async fn export_tasks_csv(&self, query: TaskFilterQuery) -> String {
        let list = self.list_tasks(query).await;
        let mut csv = String::from("id,user_id,file_name,quality,priority,status,created_at_ms,started_at_ms,completed_at_ms\n");
        for task in list.items {
            csv.push_str(&format!(
                "{},{},{},{},{:?},{:?},{},{},{}\n",
                task.id,
                escape_csv(&task.user_id),
                escape_csv(&task.file_name),
                escape_csv(&task.quality),
                task.priority,
                task.status,
                task.created_at_ms,
                task.started_at_ms.unwrap_or_default(),
                task.completed_at_ms.unwrap_or_default()
            ));
        }
        csv
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginResponse, String> {
        let mut state = self.state.lock().await;
        let username = request.username.trim().to_string();
        if username.is_empty() || request.password.trim().is_empty() {
            return Err("username and password are required".to_string());
        }
        let user_password = state
            .users
            .entry(username.clone())
            .or_insert_with(|| request.password.clone())
            .clone();
        if user_password != request.password {
            return Err("invalid credentials".to_string());
        }
        let token = format!("zephost-token-{}", Uuid::new_v4());
        state.auth_tokens.insert(token.clone(), username.clone());
        let api_keys = state.api_keys.get(&username).cloned().unwrap_or_default();
        Ok(LoginResponse { token, api_keys })
    }

    pub async fn create_api_key(&self, token: &str, label: Option<String>) -> Result<ApiKey, String> {
        let mut state = self.state.lock().await;
        let user_id = state
            .auth_tokens
            .get(token)
            .cloned()
            .ok_or_else(|| "unauthorized".to_string())?;
        let key = ApiKey {
            key: format!("zpk_{}", Uuid::new_v4().simple()),
            label: clean_or_default(label, "default"),
            created_at_ms: now_ms(),
        };
        state.api_keys.entry(user_id).or_default().push(key.clone());
        Ok(key)
    }

    pub async fn verify_api_access(&self, token: Option<&str>, api_key: Option<&str>) -> bool {
        let state = self.state.lock().await;
        if let Some(token) = token {
            if state.auth_tokens.contains_key(token) {
                return true;
            }
        }
        if let Some(api_key) = api_key {
            return state
                .api_keys
                .values()
                .flatten()
                .any(|candidate| candidate.key == api_key);
        }
        false
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
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

    fn prune_sessions(&mut self) {
        let cutoff = now_ms().saturating_sub(SESSION_TTL_MS);
        self.sessions
            .retain(|_, last_seen_ms| *last_seen_ms >= cutoff);
    }

    fn within_rate_limit(&mut self, user_id: &str) -> bool {
        let now = now_ms();
        let window = self.task_rate_window.entry(user_id.to_string()).or_default();
        let cutoff = now.saturating_sub(60_000);
        window.retain(|timestamp| *timestamp >= cutoff);
        if window.len() >= TASK_RATE_LIMIT_PER_MINUTE {
            return false;
        }
        window.push(now);
        true
    }

    fn estimate_completion(
        &self,
        now_ms: u128,
        queue_depth: usize,
        priority: &TaskPriority,
        running: bool,
    ) -> u128 {
        let runtime = estimated_runtime_ms(priority, running);
        now_ms + runtime * (queue_depth as u128 + 1)
    }

    fn status_response(&self) -> StatusResponse {
        StatusResponse {
            queued: self.count_status(TaskStatus::Queued),
            running: self.count_status(TaskStatus::Running),
            completed: self.count_status(TaskStatus::Completed),
            active_users: self.sessions.len(),
            beta_user_limit: BETA_USER_LIMIT,
            total_tasks: self.tasks.len(),
        }
    }
}

fn estimated_runtime_ms(priority: &TaskPriority, running: bool) -> u128 {
    let base = match priority {
        TaskPriority::High => 8_000,
        TaskPriority::Medium => 15_000,
        TaskPriority::Low => 22_000,
    };
    if running {
        base
    } else {
        base + 5_000
    }
}

fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
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
