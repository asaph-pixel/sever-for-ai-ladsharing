use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bcrypt::{hash, verify, DEFAULT_COST};
use reqwest::Client;
use serde_json::json;
use sqlx::{PgPool, Row};
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::models::tasks::{
    ApiKey, CreateTaskRequest, HeartbeatRequest, LoginRequest, LoginResponse, StatusResponse,
    SubmitResultRequest, Task, TaskFilterQuery, TaskListResponse, TaskPriority, TaskStatus,
    WaitlistRequest,
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
    db: Option<PgPool>,
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
    waitlist: Vec<WaitlistRequest>,
}

impl TaskStore {
    pub async fn new() -> Result<Self, String> {
        let (events, _) = broadcast::channel(200);
        let db = match std::env::var("DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => {
                let pool = PgPool::connect(&url)
                    .await
                    .map_err(|error| format!("could not connect to PostgreSQL: {error}"))?;
                migrate(&pool).await?;
                seed_admin_user(&pool).await?;
                Some(pool)
            }
            _ => None,
        };
        let mut initial_state = TaskState::default();
        if db.is_none() {
            seed_memory_admin_user(&mut initial_state)?;
        }

        Ok(Self {
            next_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(Mutex::new(initial_state)),
            events,
            http_client: Client::new(),
            db,
        })
    }

    pub async fn create_task(
        &self,
        authenticated_user_id: String,
        request: CreateTaskRequest,
    ) -> Result<Task, String> {
        let _legacy_user_id = request.user_id.as_deref();
        let user_id = clean_or_default(Some(authenticated_user_id), "anonymous");
        let file_name = clean_or_default(request.file_name, "sample-file.mp4");
        let quality = clean_or_default(request.quality, "beta");
        let priority = request.priority.unwrap_or(TaskPriority::Medium);
        let payload = request.payload.unwrap_or_else(|| json!({}));

        {
            let mut state = self.state.lock().await;
            if !state.within_rate_limit(&user_id) {
                return Err("rate limit reached for this user (20 tasks / minute)".to_string());
            }
        }

        if let Some(db) = &self.db {
            let active_users: i64 = sqlx::query_scalar(
                "select count(distinct user_id) from tasks where status in ('queued', 'running')",
            )
            .fetch_one(db)
            .await
            .map_err(db_error)?;
            let user_has_active: bool = sqlx::query_scalar(
                "select exists(select 1 from tasks where user_id = $1 and status in ('queued', 'running'))",
            )
            .bind(&user_id)
            .fetch_one(db)
            .await
            .map_err(db_error)?;
            if !user_has_active && active_users as usize >= BETA_USER_LIMIT {
                return Err(format!(
                    "beta limit reached: only {BETA_USER_LIMIT} active users can queue work"
                ));
            }

            let queue_depth: i64 =
                sqlx::query_scalar("select count(*) from tasks where status = 'queued'")
                    .fetch_one(db)
                    .await
                    .map_err(db_error)?;
            let estimated_completion_ms =
                now_ms() + estimated_runtime_ms(&priority, false) * (queue_depth as u128 + 1);
            let row = sqlx::query(
                "insert into tasks (user_id, file_name, quality, priority, payload, status, webhook_url, created_at_ms, estimated_completion_ms)
                 values ($1, $2, $3, $4, $5, 'queued', $6, $7, $8)
                 returning *",
            )
            .bind(&user_id)
            .bind(&file_name)
            .bind(&quality)
            .bind(priority_to_str(&priority))
            .bind(payload)
            .bind(&request.webhook_url)
            .bind(ms_i64(now_ms()))
            .bind(ms_i64(estimated_completion_ms))
            .fetch_one(db)
            .await
            .map_err(db_error)?;
            let task = task_from_row(&row)?;
            self.log_job(task.id, TaskStatus::Queued, Some("task queued".to_string()))
                .await;
            let _ = self.events.send("task_created".to_string());
            return Ok(task);
        }

        let mut state = self.state.lock().await;
        if !state.user_can_queue(&user_id) {
            return Err(format!(
                "beta limit reached: only {BETA_USER_LIMIT} active users can queue work"
            ));
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
            failure_reason: None,
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
        if let Some(db) = &self.db {
            let row = sqlx::query(
                "update tasks set status = 'cancelled', completed_at_ms = $1
                 where id = $2 and status not in ('completed', 'failed', 'cancelled')
                 returning *",
            )
            .bind(ms_i64(now_ms()))
            .bind(task_id as i64)
            .fetch_optional(db)
            .await
            .map_err(db_error)?;
            let row = row.ok_or_else(|| "task not found or already finalized".to_string())?;
            let task = task_from_row(&row)?;
            self.log_job(
                task.id,
                TaskStatus::Cancelled,
                Some("task cancelled".to_string()),
            )
            .await;
            let _ = self.events.send("task_cancelled".to_string());
            return Ok(task);
        }

        let mut state = self.state.lock().await;
        if let Some(task) = state.tasks.get_mut(&task_id) {
            if matches!(
                task.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
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
        drop(state);
        Ok(self.status().await)
    }

    pub async fn fetch_next_task(&self) -> Option<Task> {
        if let Some(db) = &self.db {
            let row = sqlx::query(
                "update tasks set status = 'running', started_at_ms = $1, estimated_completion_ms = $2
                 where id = (
                    select id from tasks where status = 'queued' order by id asc limit 1 for update skip locked
                 )
                 returning *",
            )
            .bind(ms_i64(now_ms()))
            .bind(ms_i64(now_ms() + estimated_runtime_ms(&TaskPriority::Medium, true)))
            .fetch_optional(db)
            .await
            .ok()
            .flatten()?;
            let task = task_from_row(&row).ok()?;
            self.log_job(
                task.id,
                TaskStatus::Running,
                Some("worker started".to_string()),
            )
            .await;
            let _ = self.events.send("task_running".to_string());
            return Some(task);
        }

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
        let success = request.success.unwrap_or(true);
        let status = if success {
            TaskStatus::Completed
        } else {
            TaskStatus::Failed
        };
        let reason = clean_optional(request.reason);

        if let Some(db) = &self.db {
            let row = sqlx::query(
                "update tasks set status = $1, result = $2, failure_reason = $3, completed_at_ms = $4, estimated_completion_ms = $4
                 where id = $5 returning *",
            )
            .bind(status_to_str(&status))
            .bind(request.result)
            .bind(&reason)
            .bind(ms_i64(now_ms()))
            .bind(request.task_id as i64)
            .fetch_optional(db)
            .await
            .map_err(db_error)?;
            let row = row.ok_or_else(|| "task not found".to_string())?;
            let task = task_from_row(&row)?;
            self.log_job(task.id, status.clone(), reason.clone()).await;
            self.dispatch_webhook(&task).await;
            let _ = self.events.send("task_completed".to_string());
            return Ok(task);
        }

        let mut state = self.state.lock().await;
        let outcome = match state.tasks.get_mut(&request.task_id) {
            Some(task) => {
                task.status = status.clone();
                task.result = Some(request.result);
                task.failure_reason = reason.clone();
                task.completed_at_ms = Some(now_ms());
                task.estimated_completion_ms = Some(now_ms());
                Ok(task.clone())
            }
            None => Err("task not found".to_string()),
        };
        drop(state);
        if let Ok(task) = &outcome {
            self.dispatch_webhook(task).await;
        }
        let _ = self.events.send("task_completed".to_string());
        outcome
    }

    pub async fn status(&self) -> StatusResponse {
        let active_users = {
            let mut state = self.state.lock().await;
            state.prune_sessions();
            state.sessions.len()
        };
        if let Some(db) = &self.db {
            let queued = count_db_status(db, "queued").await;
            let running = count_db_status(db, "running").await;
            let completed = count_db_status(db, "completed").await;
            let total_tasks = sqlx::query_scalar::<_, i64>("select count(*) from tasks")
                .fetch_one(db)
                .await
                .unwrap_or_default() as usize;
            return StatusResponse {
                queued,
                running,
                completed,
                active_users,
                beta_user_limit: BETA_USER_LIMIT,
                total_tasks,
            };
        }
        let state = self.state.lock().await;
        state.status_response(active_users)
    }

    pub async fn list_tasks(&self, query: TaskFilterQuery) -> TaskListResponse {
        if let Some(db) = &self.db {
            return self.list_tasks_db(db, query).await;
        }
        let state = self.state.lock().await;
        let mut tasks = state.tasks.values().cloned().collect::<Vec<_>>();
        apply_filters(&mut tasks, &query);
        tasks.sort_by(|left, right| right.id.cmp(&left.id));
        paginate(tasks, query)
    }

    pub async fn export_tasks_json(&self, query: TaskFilterQuery) -> serde_json::Value {
        let list = self.list_tasks(query).await;
        json!(list.items)
    }

    pub async fn export_tasks_csv(&self, query: TaskFilterQuery) -> String {
        let list = self.list_tasks(query).await;
        let mut csv = String::from("id,user_id,file_name,quality,priority,status,created_at_ms,started_at_ms,completed_at_ms,failure_reason\n");
        for task in list.items {
            csv.push_str(&format!(
                "{},{},{},{},{:?},{:?},{},{},{},{}\n",
                task.id,
                escape_csv(&task.user_id),
                escape_csv(&task.file_name),
                escape_csv(&task.quality),
                task.priority,
                task.status,
                task.created_at_ms,
                task.started_at_ms.unwrap_or_default(),
                task.completed_at_ms.unwrap_or_default(),
                escape_csv(task.failure_reason.as_deref().unwrap_or(""))
            ));
        }
        csv
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginResponse, String> {
        let username = request.username.trim().to_string();
        if username.is_empty() || request.password.trim().is_empty() {
            return Err("username and password are required".to_string());
        }

        if let Some(db) = &self.db {
            let row = sqlx::query("select password_hash from users where username = $1")
                .bind(&username)
                .fetch_optional(db)
                .await
                .map_err(db_error)?;
            if let Some(row) = row {
                let password_hash: String = row.get("password_hash");
                if !verify(&request.password, &password_hash)
                    .map_err(|_| "invalid credentials".to_string())?
                {
                    return Err("invalid credentials".to_string());
                }
            } else if public_signup_enabled() {
                let password_hash =
                    hash(&request.password, DEFAULT_COST).map_err(|_| "could not hash password")?;
                sqlx::query("insert into users (username, password_hash, created_at_ms) values ($1, $2, $3)")
                    .bind(&username)
                    .bind(password_hash)
                    .bind(ms_i64(now_ms()))
                    .execute(db)
                    .await
                    .map_err(db_error)?;
            } else {
                return Err("invite required".to_string());
            }
            let token = self.create_session_token(username.clone()).await;
            let api_keys = self.api_keys_for_user(&username).await;
            return Ok(LoginResponse { token, api_keys });
        }

        let mut state = self.state.lock().await;
        let stored_hash = match state.users.get(&username) {
            Some(stored_hash) => stored_hash.clone(),
            None => {
                if !public_signup_enabled() {
                    return Err("invite required".to_string());
                }
                let password_hash =
                    hash(&request.password, DEFAULT_COST).map_err(|_| "could not hash password")?;
                state.users.insert(username.clone(), password_hash.clone());
                password_hash
            }
        };
        if !verify(&request.password, &stored_hash)
            .map_err(|_| "invalid credentials".to_string())?
        {
            return Err("invalid credentials".to_string());
        }
        let token = format!("zephost-token-{}", Uuid::new_v4());
        state.auth_tokens.insert(token.clone(), username.clone());
        let api_keys = state.api_keys.get(&username).cloned().unwrap_or_default();
        Ok(LoginResponse { token, api_keys })
    }

    pub async fn create_api_key(
        &self,
        token: &str,
        label: Option<String>,
    ) -> Result<ApiKey, String> {
        let user_id = self
            .user_for_access(Some(token), None)
            .await
            .ok_or_else(|| "unauthorized".to_string())?;
        let key = ApiKey {
            key: format!("zpk_{}", Uuid::new_v4().simple()),
            label: clean_or_default(label, "default"),
            created_at_ms: now_ms(),
        };
        if let Some(db) = &self.db {
            sqlx::query(
                "insert into api_keys (key, user_id, label, created_at_ms) values ($1, $2, $3, $4)",
            )
            .bind(&key.key)
            .bind(&user_id)
            .bind(&key.label)
            .bind(ms_i64(key.created_at_ms))
            .execute(db)
            .await
            .map_err(db_error)?;
            return Ok(key);
        }

        let mut state = self.state.lock().await;
        state.api_keys.entry(user_id).or_default().push(key.clone());
        Ok(key)
    }

    pub async fn user_for_access(
        &self,
        token: Option<&str>,
        api_key: Option<&str>,
    ) -> Option<String> {
        let state = self.state.lock().await;
        if let Some(token) = token {
            if let Some(user_id) = state.auth_tokens.get(token) {
                return Some(user_id.clone());
            }
        }
        drop(state);

        if let (Some(db), Some(api_key)) = (&self.db, api_key) {
            return sqlx::query_scalar("select user_id from api_keys where key = $1")
                .bind(api_key)
                .fetch_optional(db)
                .await
                .ok()
                .flatten();
        }
        if let Some(api_key) = api_key {
            let state = self.state.lock().await;
            return state
                .api_keys
                .iter()
                .find(|(_, keys)| keys.iter().any(|candidate| candidate.key == api_key))
                .map(|(user_id, _)| user_id.clone());
        }
        None
    }

    pub async fn join_waitlist(&self, request: WaitlistRequest) -> Result<(), String> {
        if request.email.trim().is_empty() || !request.email.contains('@') {
            return Err("a valid email is required".to_string());
        }
        if let Some(db) = &self.db {
            sqlx::query(
                "insert into waitlist (email, name, use_case, created_at_ms)
                 values ($1, $2, $3, $4)
                 on conflict (email) do update set name = excluded.name, use_case = excluded.use_case",
            )
            .bind(request.email.trim())
            .bind(clean_optional(request.name))
            .bind(clean_optional(request.use_case))
            .bind(ms_i64(now_ms()))
            .execute(db)
            .await
            .map_err(db_error)?;
            return Ok(());
        }
        let mut state = self.state.lock().await;
        state.waitlist.push(request);
        Ok(())
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
    }

    async fn create_session_token(&self, username: String) -> String {
        let token = format!("zephost-token-{}", Uuid::new_v4());
        let mut state = self.state.lock().await;
        state.auth_tokens.insert(token.clone(), username);
        token
    }

    async fn api_keys_for_user(&self, username: &str) -> Vec<ApiKey> {
        if let Some(db) = &self.db {
            return sqlx::query(
                "select key, label, created_at_ms from api_keys where user_id = $1 order by created_at_ms desc",
            )
            .bind(username)
            .fetch_all(db)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|row| ApiKey {
                key: row.get("key"),
                label: row.get("label"),
                created_at_ms: row.get::<i64, _>("created_at_ms") as u128,
            })
            .collect();
        }
        Vec::new()
    }

    async fn list_tasks_db(&self, db: &PgPool, query: TaskFilterQuery) -> TaskListResponse {
        let rows = sqlx::query("select * from tasks order by id desc")
            .fetch_all(db)
            .await
            .unwrap_or_default();
        let mut tasks = rows
            .iter()
            .filter_map(|row| task_from_row(row).ok())
            .collect::<Vec<_>>();
        apply_filters(&mut tasks, &query);
        paginate(tasks, query)
    }

    async fn log_job(&self, task_id: u64, status: TaskStatus, reason: Option<String>) {
        if let Some(db) = &self.db {
            let _ = sqlx::query(
                "insert into job_logs (task_id, status, reason, created_at_ms) values ($1, $2, $3, $4)",
            )
            .bind(task_id as i64)
            .bind(status_to_str(&status))
            .bind(reason)
            .bind(ms_i64(now_ms()))
            .execute(db)
            .await;
        }
    }

    async fn dispatch_webhook(&self, task: &Task) {
        if let Some(webhook_url) = task.webhook_url.clone() {
            let client = self.http_client.clone();
            let payload = json!({
                "task_id": task.id,
                "status": status_to_str(&task.status),
                "result": task.result,
                "failure_reason": task.failure_reason
            });
            tokio::spawn(async move {
                let _ = client.post(webhook_url).json(&payload).send().await;
            });
        }
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
        let window = self
            .task_rate_window
            .entry(user_id.to_string())
            .or_default();
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
        now_ms + estimated_runtime_ms(priority, running) * (queue_depth as u128 + 1)
    }

    fn status_response(&self, active_users: usize) -> StatusResponse {
        StatusResponse {
            queued: self.count_status(TaskStatus::Queued),
            running: self.count_status(TaskStatus::Running),
            completed: self.count_status(TaskStatus::Completed),
            active_users,
            beta_user_limit: BETA_USER_LIMIT,
            total_tasks: self.tasks.len(),
        }
    }
}

async fn migrate(db: &PgPool) -> Result<(), String> {
    for statement in [
        "create table if not exists users (
            username text primary key,
            password_hash text not null,
            created_at_ms bigint not null
        )",
        "create table if not exists api_keys (
            key text primary key,
            user_id text not null references users(username) on delete cascade,
            label text not null,
            created_at_ms bigint not null
        )",
        "create table if not exists tasks (
            id bigserial primary key,
            user_id text not null,
            file_name text not null,
            quality text not null,
            priority text not null,
            payload jsonb not null default '{}'::jsonb,
            status text not null,
            result jsonb,
            failure_reason text,
            webhook_url text,
            created_at_ms bigint not null,
            started_at_ms bigint,
            completed_at_ms bigint,
            estimated_completion_ms bigint
        )",
        "create table if not exists job_logs (
            id bigserial primary key,
            task_id bigint not null references tasks(id) on delete cascade,
            status text not null,
            reason text,
            created_at_ms bigint not null
        )",
        "create table if not exists waitlist (
            email text primary key,
            name text,
            use_case text,
            created_at_ms bigint not null
        )",
    ] {
        sqlx::query(statement).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}

async fn seed_admin_user(db: &PgPool) -> Result<(), String> {
    let Some((username, password)) = admin_credentials() else {
        return Ok(());
    };
    let exists: bool = sqlx::query_scalar("select exists(select 1 from users where username = $1)")
        .bind(&username)
        .fetch_one(db)
        .await
        .map_err(db_error)?;
    if exists {
        return Ok(());
    }
    let password_hash = hash(password, DEFAULT_COST).map_err(|_| "could not hash password")?;
    sqlx::query("insert into users (username, password_hash, created_at_ms) values ($1, $2, $3)")
        .bind(username)
        .bind(password_hash)
        .bind(ms_i64(now_ms()))
        .execute(db)
        .await
        .map_err(db_error)?;
    Ok(())
}

fn seed_memory_admin_user(state: &mut TaskState) -> Result<(), String> {
    let Some((username, password)) = admin_credentials() else {
        return Ok(());
    };
    let password_hash = hash(password, DEFAULT_COST).map_err(|_| "could not hash password")?;
    state.users.insert(username, password_hash);
    Ok(())
}

async fn count_db_status(db: &PgPool, status: &str) -> usize {
    sqlx::query_scalar::<_, i64>("select count(*) from tasks where status = $1")
        .bind(status)
        .fetch_one(db)
        .await
        .unwrap_or_default() as usize
}

fn task_from_row(row: &sqlx::postgres::PgRow) -> Result<Task, String> {
    Ok(Task {
        id: row.get::<i64, _>("id") as u64,
        user_id: row.get("user_id"),
        file_name: row.get("file_name"),
        quality: row.get("quality"),
        priority: priority_from_str(row.get::<String, _>("priority").as_str()),
        payload: row.get("payload"),
        status: status_from_str(row.get::<String, _>("status").as_str()),
        result: row.get("result"),
        failure_reason: row.get("failure_reason"),
        webhook_url: row.get("webhook_url"),
        created_at_ms: row.get::<i64, _>("created_at_ms") as u128,
        started_at_ms: row
            .get::<Option<i64>, _>("started_at_ms")
            .map(|value| value as u128),
        completed_at_ms: row
            .get::<Option<i64>, _>("completed_at_ms")
            .map(|value| value as u128),
        estimated_completion_ms: row
            .get::<Option<i64>, _>("estimated_completion_ms")
            .map(|value| value as u128),
    })
}

fn apply_filters(tasks: &mut Vec<Task>, query: &TaskFilterQuery) {
    if let Some(status) = query.status.as_ref() {
        let status = status.to_lowercase();
        tasks.retain(|task| status_to_str(&task.status) == status);
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
}

fn paginate(tasks: Vec<Task>, query: TaskFilterQuery) -> TaskListResponse {
    let total = tasks.len();
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(20).max(1).min(100);
    let start = (page - 1) * per_page;
    let items = tasks
        .into_iter()
        .skip(start)
        .take(per_page)
        .collect::<Vec<_>>();
    TaskListResponse {
        items,
        total,
        page,
        per_page,
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

fn status_to_str(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

fn status_from_str(status: &str) -> TaskStatus {
    match status {
        "running" => TaskStatus::Running,
        "completed" => TaskStatus::Completed,
        "failed" => TaskStatus::Failed,
        "cancelled" => TaskStatus::Cancelled,
        _ => TaskStatus::Queued,
    }
}

fn priority_to_str(priority: &TaskPriority) -> &'static str {
    match priority {
        TaskPriority::High => "high",
        TaskPriority::Medium => "medium",
        TaskPriority::Low => "low",
    }
}

fn priority_from_str(priority: &str) -> TaskPriority {
    match priority {
        "high" => TaskPriority::High,
        "low" => TaskPriority::Low,
        _ => TaskPriority::Medium,
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

fn clean_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn ms_i64(value: u128) -> i64 {
    value.min(i64::MAX as u128) as i64
}

fn db_error(error: sqlx::Error) -> String {
    format!("database error: {error}")
}

fn public_signup_enabled() -> bool {
    std::env::var("ALLOW_PUBLIC_SIGNUP")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn admin_credentials() -> Option<(String, String)> {
    let username = std::env::var("ZEPHOST_ADMIN_USERNAME").ok()?;
    let password = std::env::var("ZEPHOST_ADMIN_PASSWORD").ok()?;
    let username = username.trim().to_string();
    if username.is_empty() || password.trim().is_empty() {
        return None;
    }
    Some((username, password))
}
