use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::models::tasks::{
    AdaptiveParameters, ExecutionMode, LocalTestRequest, StoredTask, TaskRequest, TaskStatus, TaskSummary,
};
use crate::services::tasks_router;

const TOTAL_NODES: usize = 24;
const ONLINE_NODES: usize = 20;
const COMPUTE_UNITS_PER_NODE: usize = 8;

#[derive(Clone)]
pub struct TaskStore {
    next_id: Arc<AtomicU64>,
    tasks: Arc<RwLock<HashMap<u64, StoredTask>>>,
}

impl TaskStore {
    pub fn new() -> Self {
        let store = Self {
            next_id: Arc::new(AtomicU64::new(4_820)),
            tasks: Arc::new(RwLock::new(HashMap::new())),
        };

        store.seed_demo_tasks();
        store
    }

    pub async fn assign_task(&self, task: TaskRequest) -> Result<StoredTask, String> {
        let task_type = task.task_type.trim().to_lowercase();
        if task_type.is_empty() {
            return Err("task_type is required".to_string());
        }

        let input_files = task
            .input_files
            .into_iter()
            .map(|file| file.trim().to_string())
            .filter(|file| !file.is_empty())
            .collect::<Vec<_>>();

        if input_files.is_empty() {
            return Err("at least one input file is required".to_string());
        }

        let parameters = task.parameters.trim().to_string();
        if !parameters.is_empty() {
            serde_json::from_str::<serde_json::Value>(&parameters)
                .map_err(|_| "parameters must be valid JSON when provided".to_string())?;
        }

        let task_id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let route = tasks_router::route_task(&task.priority);
        let output_files = input_files
            .iter()
            .enumerate()
            .map(|(index, _)| format!("zephost_{}_{}_{}", task_type, task_id, index + 1))
            .collect::<Vec<_>>();

        let stored_task = StoredTask {
            task_id,
            task_type,
            input_files,
            parameters,
            priority: task.priority,
            execution_mode: task.execution_mode,
            status: TaskStatus::Queued,
            progress_pct: 12,
            route,
            output_files,
        };

        self.tasks.write().await.insert(task_id, stored_task.clone());
        self.spawn_progress_simulation(task_id);

        Ok(stored_task)
    }

    pub async fn list_tasks(&self) -> Vec<StoredTask> {
        let mut tasks = self.tasks.read().await.values().cloned().collect::<Vec<_>>();
        tasks.sort_by(|left, right| right.task_id.cmp(&left.task_id));
        tasks
    }

    pub async fn get_task(&self, task_id: u64) -> Option<StoredTask> {
        self.tasks.read().await.get(&task_id).cloned()
    }

    pub async fn summary(&self) -> TaskSummary {
        let tasks = self.tasks.read().await;
        let total_tasks = tasks.len();
        let queued_tasks = tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Queued))
            .count();
        let running_tasks = tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Running))
            .count();
        let completed_tasks = tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Completed))
            .count();
        let local_test_runs = tasks
            .values()
            .filter(|task| matches!(task.execution_mode, ExecutionMode::Local))
            .count();
        let active_nodes = tasks
            .values()
            .filter(|task| matches!(task.status, TaskStatus::Queued | TaskStatus::Running))
            .map(|task| task.route.node_label.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();
        let online_users = 18 + (running_tasks * 6) + (queued_tasks * 3) + (completed_tasks.min(12));
        let total_estimated_cost = tasks.values().map(|task| task.route.estimated_cost).sum::<f64>();
        let adaptive_parameters = self.build_adaptive_parameters(running_tasks, queued_tasks, active_nodes);

        TaskSummary {
            total_tasks,
            queued_tasks,
            running_tasks,
            completed_tasks,
            online_users,
            online_nodes: ONLINE_NODES,
            active_nodes,
            total_nodes: TOTAL_NODES,
            total_compute_units: ONLINE_NODES * COMPUTE_UNITS_PER_NODE,
            local_test_runs,
            total_estimated_cost,
            adaptive_parameters,
        }
    }

    pub async fn run_local_test(&self, local_test: LocalTestRequest) -> Result<StoredTask, String> {
        if local_test.dataset_size_mb == 0 {
            return Err("dataset_size_mb must be greater than 0".to_string());
        }

        if local_test.iterations == 0 {
            return Err("iterations must be greater than 0".to_string());
        }

        let summary = self.summary().await;
        let parameters = serde_json::json!({
            "dataset_size_mb": local_test.dataset_size_mb,
            "iterations": local_test.iterations,
            "suggested_parallel_jobs": summary.adaptive_parameters.recommended_parallel_jobs,
            "suggested_batch_size": summary.adaptive_parameters.recommended_batch_size,
            "online_users": summary.online_users,
            "active_nodes": summary.active_nodes
        });

        let priority = if local_test.dataset_size_mb > 2048 {
            crate::models::tasks::TaskPriority::High
        } else {
            crate::models::tasks::TaskPriority::Low
        };

        self.assign_task(TaskRequest {
            task_type: "local_test".to_string(),
            input_files: vec![format!(
                "local://benchmark/{}mb-{}x.bin",
                local_test.dataset_size_mb, local_test.iterations
            )],
            parameters: parameters.to_string(),
            priority,
            execution_mode: ExecutionMode::Local,
        })
        .await
    }

    fn spawn_progress_simulation(&self, task_id: u64) {
        let tasks = Arc::clone(&self.tasks);

        tokio::spawn(async move {
            let checkpoints = [
                (TaskStatus::Running, 45u8),
                (TaskStatus::Running, 78u8),
                (TaskStatus::Completed, 100u8),
            ];

            for (status, progress) in checkpoints {
                sleep(Duration::from_secs(2)).await;

                let mut task_map = tasks.write().await;
                if let Some(task) = task_map.get_mut(&task_id) {
                    task.status = status;
                    task.progress_pct = progress;
                } else {
                    break;
                }
            }
        });
    }

    fn build_adaptive_parameters(
        &self,
        running_tasks: usize,
        queued_tasks: usize,
        active_nodes: usize,
    ) -> AdaptiveParameters {
        let pressure = running_tasks + queued_tasks;

        let (recommended_priority, recommended_parallel_jobs, recommended_batch_size, preferred_execution_mode) =
            if pressure >= 6 || active_nodes >= 5 {
                ("enterprise", 2, 16, "hybrid")
            } else if pressure >= 3 {
                ("high", 4, 32, "cloud")
            } else {
                ("low", 6, 64, "local")
            };

        AdaptiveParameters {
            recommended_priority: recommended_priority.to_string(),
            recommended_parallel_jobs,
            recommended_batch_size,
            preferred_execution_mode: preferred_execution_mode.to_string(),
        }
    }

    fn seed_demo_tasks(&self) {
        let demo_tasks = HashMap::from([
            (
                4_821,
                StoredTask {
                    task_id: 4_821,
                    task_type: "render".to_string(),
                    input_files: vec!["/samples/trailer_scene.mov".to_string()],
                    parameters: "{\"quality\":\"ultra\",\"format\":\"mp4\"}".to_string(),
                    priority: crate::models::tasks::TaskPriority::High,
                    execution_mode: ExecutionMode::Cloud,
                    status: TaskStatus::Running,
                    progress_pct: 72,
                    route: tasks_router::route_task(&crate::models::tasks::TaskPriority::High),
                    output_files: vec!["zephost_render_4821_1".to_string()],
                },
            ),
            (
                4_820,
                StoredTask {
                    task_id: 4_820,
                    task_type: "inference".to_string(),
                    input_files: vec!["/samples/model-input.json".to_string()],
                    parameters: "{\"model\":\"vision-xl\",\"batch\":4}".to_string(),
                    priority: crate::models::tasks::TaskPriority::Enterprise,
                    execution_mode: ExecutionMode::Hybrid,
                    status: TaskStatus::Completed,
                    progress_pct: 100,
                    route: tasks_router::route_task(&crate::models::tasks::TaskPriority::Enterprise),
                    output_files: vec![
                        "zephost_inference_4820_1".to_string(),
                        "zephost_inference_4820_2".to_string(),
                    ],
                },
            ),
        ]);

        if let Ok(mut tasks) = self.tasks.try_write() {
            tasks.extend(demo_tasks);
        }
    }
}
