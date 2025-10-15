use crate::model;
use actix_web::{web, get, HttpResponse, Responder};
use serde_json::json;

pub mod job;
pub use job::job_routes;

pub mod metric;
pub use metric::metrics_routes;

#[get("/health")]
async fn apiv1status() -> impl Responder {
    HttpResponse::Ok().json(model::ApiResponse::ok(
        "API v1 Service is operational",
        json!({"status": "running", "version": "v1.0"}),
    ))
}

pub fn service_config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope(
        "/api/v1"
    )
    .configure(job_routes)
    .configure(metrics_routes)
    .service(apiv1status));
}
