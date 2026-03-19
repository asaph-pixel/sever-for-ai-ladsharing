use sqlx::PgPool;

pub async fn init() -> Result<PgPool, sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env");
    
    PgPool::connect(&database_url).await
}
