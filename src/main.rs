use actix_cors::Cors;
use actix_files::Files;
use actix_web::{web, App, HttpServer};
use dotenvy::dotenv;

mod models;
mod routes;
mod services;

use services::task_manager::TaskStore;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let task_store = TaskStore::new();
    let cors_origin = std::env::var("CORS_ALLOWED_ORIGIN").unwrap_or_else(|_| "*".to_string());

    HttpServer::new(move || {
        let cors = if cors_origin == "*" {
            Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
        } else {
            Cors::default()
                .allowed_origin(&cors_origin)
                .allow_any_method()
                .allow_any_header()
        };

        App::new()
            .wrap(cors)
            .app_data(web::Data::new(task_store.clone()))
            .configure(routes::ai::init_routes)
            .service(Files::new("/", "static").index_file("index.html"))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
