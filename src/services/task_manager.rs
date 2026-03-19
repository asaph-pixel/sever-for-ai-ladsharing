use crate::models::tasks::TaskRequest;

pub async fn assign_task(task: TaskRequest) -> Result<u64, String> {
    Ok(42)
}