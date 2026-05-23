use actix_web::http::header;
use actix_web::middleware::DefaultHeaders;
use actix_web::{error, web, App, HttpResponse, HttpServer};
use dotenvy::dotenv;

mod models;
mod routes;
mod services;

use services::task_manager::TaskStore;

async fn preflight() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "ok": true }))
}

async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "service": "zephost",
        "status": "ok"
    }))
}

fn json_config() -> web::JsonConfig {
    web::JsonConfig::default().error_handler(|err, _req| {
        error::InternalError::from_response(
            err,
            HttpResponse::BadRequest().json(serde_json::json!({
                "error": "request body must be valid JSON"
            })),
        )
        .into()
    })
}

async fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "route not found"
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let task_store = TaskStore::new()
        .await
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))?;
    let cors_origin = std::env::var("CORS_ALLOWED_ORIGIN").unwrap_or_else(|_| "*".to_string());
    let bind_host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let bind_port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);

    HttpServer::new(move || {
        App::new()
            .wrap(
                DefaultHeaders::new()
                    .add((header::ACCESS_CONTROL_ALLOW_ORIGIN, cors_origin.clone()))
                    .add((header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"))
                    .add((
                        header::ACCESS_CONTROL_ALLOW_HEADERS,
                        "Content-Type, Authorization",
                    ))
                    .add((header::ACCESS_CONTROL_MAX_AGE, "86400")),
            )
            .app_data(web::Data::new(task_store.clone()))
            .app_data(json_config())
            .configure(routes::api::init_routes)
            .route("/", web::get().to(health))
            .route(
                "/{tail:.*}",
                web::method(actix_web::http::Method::OPTIONS).to(preflight),
            )
            .default_service(web::route().to(not_found))
    })
    .bind((bind_host.as_str(), bind_port))?
    .run()
    .await
}
