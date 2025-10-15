use actix_web::{web, get, HttpResponse, Responder, Result};
use serde::Serialize;
use sqlx::SqlitePool;

use crate::db;
use crate::error::AppError;

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: T,
}

#[get("/jobs/{id}/metrics")]
pub async fn get_job_metrics(
    pool: web::Data<SqlitePool>,
    path: web::Path<String>,
) -> Result<impl Responder, AppError> {
    let job_id = path.into_inner();
    let rows = db::get_metrics_by_job(&pool, &job_id).await.map_err(AppError::internal)?;
    let data: Vec<_> = rows.into_iter().map(|m| {
        serde_json::json!({
            "job_id": m.job_id,
            "key": m.key,
            "value": m.value,
            "unit": m.unit,
            "labels": m.labels_json.as_deref().and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
            "created_at": m.created_at,
        })
    }).collect();

    Ok(HttpResponse::Ok().json(ApiResponse { success: true, data }))
}

#[get("/metrics/recent")]
pub async fn get_recent_metrics(
    pool: web::Data<SqlitePool>,
    q: web::Query<std::collections::HashMap<String, String>>,
) -> Result<impl Responder, AppError> {
    let limit = q.get("limit").and_then(|s| s.parse::<i64>().ok()).unwrap_or(200);
    let rows = db::list_recent_metrics(&pool, limit).await.map_err(AppError::internal)?;
    let data: Vec<_> = rows.into_iter().map(|m| {
        serde_json::json!({
            "job_id": m.job_id,
            "key": m.key,
            "value": m.value,
            "unit": m.unit,
            "labels": m.labels_json.as_deref().and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
            "created_at": m.created_at,
        })
    }).collect();

    Ok(HttpResponse::Ok().json(ApiResponse { success: true, data }))
}

pub fn metrics_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(get_job_metrics)
       .service(get_recent_metrics);
}
