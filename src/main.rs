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
use log::info;

use crate::config::AppConfig;
use crate::db::{init_pool, init_db};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub global_pull_sem: Arc<Semaphore>,
    pub registry_sems: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

#[derive(Parser, Debug)]
#[command(name = "imgpuller-metric-export", version, about = "Actix + SQLx metric exporter")]
struct CliArgs {
    /// Initialize (create/reset) database schema and exit.
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
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = CliArgs::parse();
    let cfg = AppConfig::from_env();
    info!("🔧 Configuration: {:?}", cfg);

    // --init-db mode: เตรียมไฟล์/ไดเรกทอรี แล้วสร้างตาราง จากนั้นออกเลย
    if args.init_db {
        info!("--init-db with DATABASE_URL = {}", cfg.database_url);

        // รองรับเฉพาะไฟล์ (sqlite://...) ถ้าเป็น sqlite::memory: จะข้ามส่วนจัดการไฟล์
        if let Some(path_str) = cfg.database_url.strip_prefix("sqlite://") {
            let path = std::path::Path::new(path_str);

            // สร้างโฟลเดอร์เฉพาะกรณีมี parent และไม่ว่าง
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    info!("📁 Creating directory for database: {}", parent.display());
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        eprintln!("❌ Failed to create directory {}: {e}", parent.display());
                        return Ok(());
                    }
                }
            }

            // ลบไฟล์เดิม (ถ้ามี) ในตำแหน่ง relative เดิม (ไม่เติม '/')
            if path.exists() {
                info!("🗑️ Removing existing database file: {}", path.display());
                if let Err(e) = std::fs::remove_file(path) {
                    eprintln!("❌ Failed to remove old DB file {}: {e}", path.display());
                    return Ok(());
                }
            }

            info!("🆕 Creating new database file at {}", path.display());
        } else {
            info!("⚠️ --init-db works only with sqlite:// URLs (current: {})", cfg.database_url);
        }

        // สร้าง pool แล้ว init schema (แสดง error แทน panic)
        match init_pool(&cfg.database_url).await {
            Ok(pool) => {
                match init_db(&pool).await {
                    Ok(()) => {
                        info!("✅ Database schema initialized. Exiting per --init-db.");
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to initialize database schema: {e}");
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to initialize database (pool): {e}");
            }
        }

        return Ok(());
    }
    
    // normal server mode
    let pool = init_pool(&cfg.database_url)
        .await
        .expect("❌ Failed to initialize database");

    // เตรียม AppState
    let app_state = AppState {
        global_pull_sem: Arc::new(Semaphore::new(cfg.max_concurrent_pulls)),
        registry_sems: Arc::new(Mutex::new(HashMap::new())),
        config: cfg.clone(),
    };

    // ค่าไว้ใช้ใน worker โดยไม่จับ cfg ทั้งก้อน (กัน move)
    let max_concurrent_pulls = cfg.max_concurrent_pulls;
    let per_registry_max = cfg.per_registry_max;

    // start worker
    let runner_pool = pool.clone();
    tokio::spawn(async move {
        worker::run_job_runner(
            runner_pool,
            max_concurrent_pulls,
            per_registry_max,
            300, // lease time (secs)
        )
        .await;
    });

    let addr = format!("0.0.0.0:{}", cfg.app_port);
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
