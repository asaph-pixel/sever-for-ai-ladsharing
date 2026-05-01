use actix_web::{get, post, web, HttpResponse, Responder};

use crate::models::tasks::{ApiError, CreateTaskRequest, FetchTaskResponse, SubmitResultRequest};
use crate::services::task_manager::TaskStore;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(create_task)
        .service(fetch_task)
        .service(submit_result)
        .service(status)
        .service(list_tasks);
}

#[post("/task")]
async fn create_task(
    task_store: web::Data<TaskStore>,
    request: web::Json<CreateTaskRequest>,
) -> impl Responder {
    match task_store.create_task(request.into_inner()).await {
        Ok(task) => HttpResponse::Created().json(task),
        Err(error) => HttpResponse::TooManyRequests().json(ApiError { error }),
    }
}

#[get("/task")]
async fn fetch_task(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(FetchTaskResponse {
        task: task_store.fetch_next_task().await,
    })
}

#[post("/result")]
async fn submit_result(
    task_store: web::Data<TaskStore>,
    request: web::Json<SubmitResultRequest>,
) -> impl Responder {
    match task_store.submit_result(request.into_inner()).await {
        Ok(task) => HttpResponse::Ok().json(task),
        Err(error) => HttpResponse::NotFound().json(ApiError { error }),
    }
}

#[get("/status")]
async fn status(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(task_store.status().await)
}

#[get("/tasks")]
async fn list_tasks(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(task_store.list_tasks().await)
}
