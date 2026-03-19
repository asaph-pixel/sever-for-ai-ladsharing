use actix_web::{get, post, web, HttpResponse, Responder};
use crate::services::task_manager;
use crate::models::tasks::TaskRequest;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(get_status)
       .service(start_task);
}

#[get("/ai/status")]
async fn get_status() -> impl Responder {
    HttpResponse::Ok().body("AI service online")
}

#[post("/ai/start")]
async fn start_task(task: web::Json<TaskRequest>) -> impl Responder {
    match task_manager::assign_task(task.into_inner()).await {
        Ok(id) => HttpResponse::Ok().body(format!("Task assigned: {}", id)),
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}