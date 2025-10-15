use actix_web::{get, post, web, HttpResponse, Responder, Result};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::db;
use crate::error::AppError;

use bollard::query_parameters::CreateImageOptions;
use bollard::image::RemoveImageOptions;
use bollard::Docker;
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Deserialize)]
pub struct JobInput {
    pub image: String,
}

#[derive(Serialize)]
pub struct JobResponse<T> {
    pub success: bool,
    pub data: T,
}

#[post("/jobs")]
async fn create_job(
    pool: web::Data<SqlitePool>,
    body: web::Json<JobInput>,
) -> Result<impl Responder, AppError> {
    let image = body.image.trim().to_string();
    if image.is_empty() {
        return Err(AppError::bad_request(anyhow::anyhow!("image must not be empty")));
    }

    let job_id = db::insert_job(&pool, &image)
        .await
        .map_err(AppError::internal)?;

    // keep this for HTTP response
    let job_id_resp = job_id.clone();

    // clones for the background task only
    // let pool_task = pool.clone();
    // let job_id_task = job_id.clone();
    // let image_task = image.clone(

    Ok(HttpResponse::Ok().json(JobResponse {
        success: true,
        data: serde_json::json!({ "id": job_id_resp, "status": "queued" }),
    }))
}


// Make it public and take &SqlitePool so runner can call it.
pub async fn pull_image_and_record_metrics(
    pool: &SqlitePool,
    job_id: &str,
    image: &str,
) -> anyhow::Result<()> {

    let docker = Docker::connect_with_unix_defaults()
        .map_err(|e| anyhow::anyhow!("docker connect error: {e}"))?;

    let registry_host = image
        .split('/')
        .next()
        .filter(|h| h.contains('.') || h.contains(':'))
        .unwrap_or("docker.io")
        .to_string();

    let started = Instant::now();

    let (repo, tag) = if let Some((r, t)) = image.split_once(':') {
        (r.to_string(), t.to_string())
    } else {
        (image.to_string(), "latest".to_string())
    };

    let full_ref = format!("{}:{}", repo, tag);

    remove_image_if_exists(&docker, &full_ref).await;

    let opts = CreateImageOptions {
        from_image: Some(repo),
        tag: Some(tag),
        ..Default::default()
    };

    let mut stream = docker.create_image(Some(opts), None, None);

    let mut layers: HashMap<String, (u64, u64)> = HashMap::new();
    let mut logs = String::new();

    while let Some(item) = stream.try_next().await? {
        if let Some(status) = item.status {
            logs.push_str(&status);
            if let Some(id) = item.id.as_ref() {
                logs.push_str(" [");
                logs.push_str(id);
                logs.push(']');
            }
            if let Some(progress) = item.progress {
                logs.push_str(" - ");
                logs.push_str(&progress);
            }
            logs.push('\n');
        }

        if let (Some(id), Some(detail)) = (item.id, item.progress_detail) {
            let cur_u64 = detail.current.unwrap_or(0).max(0) as u64;
            let tot_u64 = detail.total.unwrap_or(0).max(0) as u64;

            let entry = layers.entry(id).or_insert((0, 0));
            if cur_u64 > entry.0 {
                entry.0 = cur_u64;
            }
            if tot_u64 > entry.1 {
                entry.1 = tot_u64;
            }
        }
    }

    // Elapsed time in ms
    let elapsed_ms = started.elapsed().as_millis() as f64;

    // Sum across layers (fallback if inspect not available)
    let (sum_cur, sum_tot) = layers.values().fold((0u64, 0u64), |acc, &(c, t)| {
        (acc.0.saturating_add(c), acc.1.saturating_add(t))
    });

    let bytes_downloaded = if sum_tot > 0 { sum_tot } else { sum_cur };

    // Try to inspect image for actual size (covers cache-hit cases)
    let inspected_size_bytes = docker
        .inspect_image(image)
        .await
        .ok()
        .and_then(|ins| ins.size)
        .unwrap_or(0) as f64;

    // Determine cache hit (either log phrase or zero downloaded bytes)
    let cache_hit = logs.contains("Image is up to date") || bytes_downloaded == 0;

    // Prefer inspected size if we have it
    let image_size_bytes = if inspected_size_bytes > 0.0 {
        inspected_size_bytes
    } else {
        bytes_downloaded as f64
    };

    // Average speed in Mbps (only when real download happened)
    let avg_speed_mbps = if bytes_downloaded > 0 && elapsed_ms > 0.0 {
        ((bytes_downloaded as f64) * 8.0) / (elapsed_ms / 1000.0) / 1_000_000.0
    } else {
        0.0
    };

    // Store summary metrics (with units)
    db::insert_metric(pool, job_id, "download_time_ms", elapsed_ms, Some("ms")).await?;
    db::insert_metric(pool, job_id, "image_size_bytes", image_size_bytes, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "bytes_downloaded_total", bytes_downloaded as f64, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "image_size_reported_bytes", inspected_size_bytes, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "average_speed_mbps", avg_speed_mbps, Some("Mbps")).await?;
    db::insert_metric(pool, job_id, "cache_hit", if cache_hit {1.0} else {0.0}, None).await?;

    // Labels for layers observed (fix registry_host/image formatting)
    let labels = serde_json::json!({
        "image": image,
        "registry_host": registry_host,
        "layer_count": layers.len(),
    }).to_string();

    db::insert_metric_labeled(pool, job_id, "layers_observed", layers.len() as f64, None, Some(&labels)).await?;

    // Finalize job status
    db::complete_job(pool, job_id, Some(&logs)).await?;

    Ok(())
}

// Remove the image (tag) if it already exists locally.
// This forces a real network download on the next pull.
// Ignore errors intentionally to be best-effort.
async fn remove_image_if_exists(docker: &Docker, full_ref: &str) {
    if docker.inspect_image(full_ref).await.is_ok() {
        let _ = docker
            .remove_image(
                full_ref,
                Some(RemoveImageOptions { force: true, noprune: false }),
                None,
            )
            .await;
    }
}

#[get("/jobs")]
async fn list_jobs(pool: web::Data<SqlitePool>) -> Result<impl Responder, AppError> {
    let items = db::list_jobs(&pool).await.map_err(AppError::internal)?;
    let data: Vec<_> = items
        .into_iter()
        .map(|(id, image, status)| serde_json::json!({
            "id": id,
            "image": image,
            "status": status
        }))
        .collect();
    Ok(HttpResponse::Ok().json(JobResponse { success: true, data }))
}

#[get("/jobs/{id}")]
async fn get_job(
    pool: web::Data<SqlitePool>,
    path: web::Path<String>,
) -> Result<impl Responder, AppError> {
    let id = path.into_inner();
    let job = db::get_job_by_id(&pool, &id).await.map_err(AppError::internal)?;
    match job {
        Some((id, image, status, result)) => Ok(HttpResponse::Ok().json(JobResponse {
            success: true,
            data: serde_json::json!({
                "id": id,
                "image": image,
                "status": status,
                "result": result
            }),
        })),
        None => Err(AppError::not_found(anyhow::anyhow!("Job not found"))),
    }
}

pub fn job_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(create_job)
        .service(list_jobs)
        .service(get_job);
}
