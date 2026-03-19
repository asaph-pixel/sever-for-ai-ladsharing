use actix_files::Files;
use actix_web::{web, App, HttpServer};
use tera::Tera;
use dotenv::dotenv;
mod routes;
mod services;
mod db;
mod utils;
mod models;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let pool = db::postgres::init().await.expect("Failed to connect to DB");

    let tera = Tera::new("templates/**/*.html").expect("Failed to load templates");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(tera.clone()))
            .configure(routes::ai::init_routes)
            .service(Files::new("/static", "static/").index_file("index.html"))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}