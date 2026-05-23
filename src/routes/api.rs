use std::time::Duration;

use actix::{Actor, ActorContext, AsyncContext, StreamHandler};
use actix_web::{get, post, web, HttpRequest, HttpResponse, Responder};
use actix_web_actors::ws;

use crate::models::tasks::{
    ApiError, CreateApiKeyRequest, CreateTaskRequest, FetchTaskResponse, HeartbeatRequest,
    LoginRequest, SubmitResultRequest, TaskFilterQuery, WaitlistRequest, WaitlistResponse,
};
use crate::services::task_manager::TaskStore;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(create_task)
        .service(create_task_upload)
        .service(fetch_task)
        .service(submit_result)
        .service(cancel_task)
        .service(heartbeat)
        .service(status)
        .service(list_tasks)
        .service(login)
        .service(create_api_key)
        .service(join_waitlist)
        .service(export_tasks)
        .service(ws_updates);
}

fn bearer_token(request: &HttpRequest) -> Option<String> {
    request
        .headers()
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|value| value.trim().to_string())
}

#[post("/task")]
async fn create_task(
    task_store: web::Data<TaskStore>,
    request_head: HttpRequest,
    request: web::Json<CreateTaskRequest>,
) -> impl Responder {
    let token = bearer_token(&request_head);
    let api_key = request_head
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());
    let Some(user_id) = task_store.user_for_access(token.as_deref(), api_key).await else {
        return HttpResponse::Unauthorized().json(ApiError {
            error: "login or api key required".to_string(),
        });
    };

    match task_store.create_task(user_id, request.into_inner()).await {
        Ok(task) => HttpResponse::Created().json(task),
        Err(error) => HttpResponse::BadRequest().json(ApiError { error }),
    }
}

#[post("/tasks")]
async fn create_task_upload(
    task_store: web::Data<TaskStore>,
    request_head: HttpRequest,
    request: web::Json<CreateTaskRequest>,
) -> impl Responder {
    let token = bearer_token(&request_head);
    let api_key = request_head
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());
    let Some(user_id) = task_store.user_for_access(token.as_deref(), api_key).await else {
        return HttpResponse::Unauthorized().json(ApiError {
            error: "login or api key required".to_string(),
        });
    };

    match task_store.create_task(user_id, request.into_inner()).await {
        Ok(task) => HttpResponse::Created().json(task),
        Err(error) => HttpResponse::BadRequest().json(ApiError { error }),
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

#[post("/tasks/{task_id}/cancel")]
async fn cancel_task(task_store: web::Data<TaskStore>, task_id: web::Path<u64>) -> impl Responder {
    match task_store.cancel_task(task_id.into_inner()).await {
        Ok(task) => HttpResponse::Ok().json(task),
        Err(error) => HttpResponse::BadRequest().json(ApiError { error }),
    }
}

#[post("/heartbeat")]
async fn heartbeat(
    task_store: web::Data<TaskStore>,
    request: web::Json<HeartbeatRequest>,
) -> impl Responder {
    match task_store.heartbeat(request.into_inner()).await {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => HttpResponse::TooManyRequests().json(ApiError { error }),
    }
}

#[get("/status")]
async fn status(task_store: web::Data<TaskStore>) -> impl Responder {
    HttpResponse::Ok().json(task_store.status().await)
}

#[get("/tasks")]
async fn list_tasks(
    task_store: web::Data<TaskStore>,
    query: web::Query<TaskFilterQuery>,
) -> impl Responder {
    HttpResponse::Ok().json(task_store.list_tasks(query.into_inner()).await)
}

#[post("/auth/login")]
async fn login(
    task_store: web::Data<TaskStore>,
    request: web::Json<LoginRequest>,
) -> impl Responder {
    match task_store.login(request.into_inner()).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(error) => HttpResponse::Unauthorized().json(ApiError { error }),
    }
}

#[post("/auth/api-keys")]
async fn create_api_key(
    task_store: web::Data<TaskStore>,
    request_head: HttpRequest,
    request: web::Json<CreateApiKeyRequest>,
) -> impl Responder {
    let token = match bearer_token(&request_head) {
        Some(token) => token,
        None => {
            return HttpResponse::Unauthorized().json(ApiError {
                error: "authorization token required".to_string(),
            })
        }
    };
    match task_store
        .create_api_key(&token, request.label.clone())
        .await
    {
        Ok(api_key) => HttpResponse::Created().json(api_key),
        Err(error) => HttpResponse::Unauthorized().json(ApiError { error }),
    }
}

#[post("/waitlist")]
async fn join_waitlist(
    task_store: web::Data<TaskStore>,
    request: web::Json<WaitlistRequest>,
) -> impl Responder {
    match task_store.join_waitlist(request.into_inner()).await {
        Ok(()) => HttpResponse::Created().json(WaitlistResponse {
            message: "You're on the Zephost access list.".to_string(),
        }),
        Err(error) => HttpResponse::BadRequest().json(ApiError { error }),
    }
}

#[get("/tasks/export")]
async fn export_tasks(
    task_store: web::Data<TaskStore>,
    query: web::Query<TaskFilterQuery>,
) -> impl Responder {
    let query = query.into_inner();
    if query.format.as_deref() == Some("csv") {
        let csv = task_store.export_tasks_csv(query).await;
        return HttpResponse::Ok()
            .content_type("text/csv; charset=utf-8")
            .body(csv);
    }
    HttpResponse::Ok().json(task_store.export_tasks_json(query).await)
}

#[get("/ws")]
async fn ws_updates(
    request: HttpRequest,
    stream: web::Payload,
    task_store: web::Data<TaskStore>,
) -> impl Responder {
    let actor = TaskWsSession {
        receiver: task_store.subscribe_events(),
    };
    ws::start(actor, &request, stream)
}

struct TaskWsSession {
    receiver: tokio::sync::broadcast::Receiver<String>,
}

impl Actor for TaskWsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |actor, ctx| {
            while let Ok(message) = actor.receiver.try_recv() {
                ctx.text(message);
            }
        });
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for TaskWsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(bytes)) => ctx.pong(&bytes),
            Ok(ws::Message::Close(_)) => ctx.stop(),
            Ok(ws::Message::Text(_)) | Ok(ws::Message::Binary(_)) | Ok(ws::Message::Pong(_)) => {}
            Ok(ws::Message::Continuation(_)) | Ok(ws::Message::Nop) | Err(_) => {}
        }
    }
}
