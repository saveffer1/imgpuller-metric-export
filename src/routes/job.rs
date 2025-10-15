use actix_web::{get, post, web, HttpResponse, Responder, Result};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::db;
use crate::error::AppError;

use bollard::query_parameters::CreateImageOptions;
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
    let pool_task = pool.clone();
    let job_id_task = job_id.clone();
    let image_task = image.clone();

    tokio::spawn(async move {
        if let Err(e) = pull_image_and_record_metrics(pool_task.clone(), job_id_task.clone(), image_task.clone()).await {
            let _ = db::update_job_status(&pool_task, &job_id_task, "failed", Some(&format!("{e:#}"))).await;
        }
    });

    Ok(HttpResponse::Ok().json(JobResponse {
        success: true,
        data: serde_json::json!({ "id": job_id_resp, "status": "queued" }),
    }))
}


async fn pull_image_and_record_metrics(
    pool: web::Data<SqlitePool>,
    job_id: String,
    image: String,
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

    // bollard 0.19.3 query_parameters::CreateImageOptions
    let opts = CreateImageOptions {
        from_image: Some(image.clone()),
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

    let elapsed_ms = started.elapsed().as_millis() as f64;
    let (sum_cur, sum_tot) = layers.values().fold((0u64, 0u64), |acc, &(c, t)| {
        (acc.0.saturating_add(c), acc.1.saturating_add(t))
    });
    let bytes_downloaded = if sum_tot > 0 { sum_tot } else { sum_cur };

    let avg_speed_mbps = if elapsed_ms > 0.0 {
        (bytes_downloaded as f64 * 8.0) / (elapsed_ms / 1000.0) / 1_000_000.0
    } else {
        0.0
    };

    // store metrics (adjust db signatures if needed)
    db::insert_metric(&pool, &job_id, "download_time_ms", elapsed_ms, None).await?;
    db::insert_metric(
        &pool,
        &job_id,
        "image_size_bytes",
        bytes_downloaded as f64,
        None,
    )
    .await?;
    db::insert_metric(&pool, &job_id, "average_speed_mbps", avg_speed_mbps, None).await?;

    let labels = serde_json::json!({
        "image": image,
        "registry_host": registry_host,
        "layer_count": layers.len(),
    })
    .to_string();

    db::insert_metric_labeled(
        &pool,
        &job_id,
        "layers_observed",
        layers.len() as f64,
        None,
        Some(&labels),
    )
    .await?;

    db::update_job_status(&pool, &job_id, "completed", Some(&logs)).await?;

    Ok(())
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
