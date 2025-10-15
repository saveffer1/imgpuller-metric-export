mod config;
mod db;
mod model;
mod error;
mod routes;
mod worker;

use std::{collections::HashMap, sync::Arc};
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web::middleware::{Logger, NormalizePath, TrailingSlash};
use tokio::sync::{Mutex, Semaphore};
use clap::Parser;
use config::AppConfig;
use db::init_pool;
use log::info;

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub global_pull_sem: Arc<Semaphore>,
    pub registry_sems: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}
#[derive(Parser, Debug)]
#[command(name = "imgpuller-metric-export", version, about = "Actix + SQLx metric exporter")]
struct CliArgs {
    #[arg(long)]
    init_db: bool,
}

impl AppState {
    pub async fn registry_sem(&self, registry: &str) -> Arc<Semaphore> {
        let mut map = self.registry_sems.lock().await;
        Arc::clone(
            map.entry(registry.to_string())
               .or_insert_with(|| Arc::new(Semaphore::new(self.config.per_registry_max)))
        )
    }
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().json(model::ApiResponse::ok(
        "Service is running",
        serde_json::json!({"status": "ok"}),
    ))
}

// 400 JSON limit/parse error
fn bad_request_json() -> HttpResponse {
    HttpResponse::BadRequest().json(model::ErrorResponse {
        success: false,
        status_code: 400,
        message: "Bad Request".into(),
        error: "Invalid JSON format or request payload size exceeded".into(),
    })
}

// 404
async fn not_found() -> impl Responder {
    HttpResponse::NotFound().json(model::ErrorResponse::new(
        404,
        "Not Found",
        "No route found",
    ))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = CliArgs::parse();
    let cfg = AppConfig::from_env();

    let app_state = AppState {
        global_pull_sem: Arc::new(Semaphore::new(cfg.max_concurrent_pulls)),
        registry_sems: Arc::new(Mutex::new(HashMap::new())),
        config: cfg.clone(),
    };

    if args.init_db {
        if let Some(path_str) = cfg.database_url.strip_prefix("sqlite://") {
            let mut path_str = path_str.to_string();
            if !path_str.starts_with('/') {
                path_str = format!("/{}", path_str);
            }
            let path = std::path::Path::new(&path_str);

            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    info!("📁 Creating directory for database: {}", parent.display());
                    std::fs::create_dir_all(parent)
                        .expect("❌ Failed to create directory for SQLite database");
                }
            }

            if path.exists() {
                info!("🗑️ Removing existing database file: {}", path.display());
                std::fs::remove_file(path)
                    .expect("❌ Failed to remove old DB file");
            }

            info!("🆕 Creating new database...");
        } else {
            info!("⚠️ --init-db works only with sqlite:// URLs");
        }
    }

    info!("🔧 Configuration: {:?}", cfg);

    let pool = init_pool(&cfg.database_url)
        .await
        .expect("❌ Failed to initialize database");

    let runner_pool = pool.clone();
        tokio::spawn(async move {
        worker::run_job_runner(
            runner_pool,                  // database pool
            cfg.max_concurrent_pulls,     // global semaphore
            cfg.per_registry_max,         // per registry semaphore
            300,                          // lease time in seconds
        )
        .await;
    });

    let addr = format!("0.0.0.0:{}", cfg.app_port);

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    info!("🚀 Server running at http://{addr}");

    HttpServer::new(move || {
        App::new()
            .wrap(NormalizePath::new(TrailingSlash::Trim))
            .wrap(Logger::default())
            .app_data(web::Data::new(app_state.clone()))
            .app_data(web::Data::new(pool.clone()))
            .app_data(
                web::JsonConfig::default()
                    .limit(4096)
                    .error_handler(|err, _req| {
                        actix_web::error::InternalError::from_response(err, bad_request_json()).into()
                    }),
            )
            .configure(routes::service_config)
            .service(health)
            .default_service(web::route().to(not_found))
    })
    .bind(addr)?
    .run()
    .await
}
