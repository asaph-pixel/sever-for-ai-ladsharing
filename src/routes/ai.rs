use actix_web::{get, post, web, HttpResponse, Responder};

use crate::models::tasks::{LocalTestRequest, TaskRequest};
use crate::services::task_manager::TaskStore;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(get_status)
        .service(list_tasks)
        .service(get_task)
        .service(run_local_test)
        .service(start_task);
}

#[get("/ai/status")]
async fn get_status(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(task_store.summary().await)
}

#[get("/ai/tasks")]
async fn list_tasks(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(task_store.list_tasks().await)
}

#[get("/ai/tasks/{task_id}")]
async fn get_task(task_id: web::Path<u64>, task_store: web::Data<TaskStore>) -> impl Responder {
    match task_store.get_task(task_id.into_inner()).await {
        Some(task) => HttpResponse::Ok().json(task),
        None => HttpResponse::NotFound().body("Task not found"),
    }
}

#[post("/ai/test/local")]
async fn run_local_test(local_test: web::Json<LocalTestRequest>, task_store: web::Data<TaskStore>) -> impl Responder {
    match task_store.run_local_test(local_test.into_inner()).await {
        Ok(created_task) => HttpResponse::Created().json(created_task),
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}

#[post("/ai/start")]
async fn start_task(task: web::Json<TaskRequest>, task_store: web::Data<TaskStore>) -> impl Responder {
    match task_store.assign_task(task.into_inner()).await {
        Ok(created_task) => HttpResponse::Created().json(created_task),
        Err(error) => HttpResponse::BadRequest().body(error),
    }
}
