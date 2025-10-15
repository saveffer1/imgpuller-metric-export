use actix_web::{get, post, web, HttpResponse, Responder, Result};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::db;
use crate::error::AppError;

use bollard::query_parameters::{CreateImageOptions, RemoveImageOptions};
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


// Parse image reference into (registry_host, repo, tag).
// Rules:
// - If no '/', default registry is docker.io.
// - If first path segment contains '.' or ':' or equals 'localhost' AND there is at least one '/': treat it as registry.
// - If no tag provided, default to 'latest'.
// - For docker.io with single-segment images, prefix 'library/' for canonical repo name (optional for pulling, good for labels).
fn parse_image_ref(input: &str) -> (String, String, String) {
    let s = input.trim();
    // Separate tag using the last ':', but only in the path part (no '/': then it's tag, not host)
    // First, split by '/', decide registry presence.
    let mut host = "docker.io".to_string();
    let mut remainder = s.to_string();

    if let Some(pos) = s.find('/') {
        let first = &s[..pos];
        let rest = &s[pos + 1..];
        if first.contains('.') || first.contains(':') || first == "localhost" {
            host = first.to_string();
            remainder = rest.to_string();
        }
    }

    // Extract tag from remainder (repo[:tag])
    let (mut repo, tag) = if let Some((r, t)) = remainder.rsplit_once(':') {
        (r.to_string(), t.to_string())
    } else {
        (remainder, "latest".to_string())
    };

    // Canonicalize docker.io single-segment -> library/<name>
    if host == "docker.io" && !repo.contains('/') {
        repo = format!("library/{}", repo);
    }

    (host, repo, tag)
}

// Build the string for CreateImageOptions.from_image.
// For docker.io we can pass "repo" only (Docker daemon will resolve), but including host also works.
// To keep compatibility with your current behavior, only prefix host when non-docker.io.
fn build_from_image(host: &str, repo: &str) -> String {
    if host == "docker.io" {
        // return canonical name without host for compatibility
        // strip "library/" in display? for pulling it's fine either way
        repo.to_string()
    } else {
        format!("{}/{}", host, repo)
    }
}


// Make it public and take &SqlitePool so runner can call it.
pub async fn pull_image_and_record_metrics(
    pool: &SqlitePool,
    job_id: &str,
    image: &str,
) -> anyhow::Result<()> {

    let docker = Docker::connect_with_unix_defaults()
        .map_err(|e| anyhow::anyhow!("docker connect error: {e}"))?;
    let result: anyhow::Result<(f64, u64, String)> = async {
        let (registry_host, repo, _tag) = parse_image_ref(image);
        let from_image = build_from_image(&registry_host, &repo);
        let started = Instant::now();
        let (repo, tag) = if let Some((r, t)) = image.split_once(':') {
            (r.to_string(), t.to_string())
        } else {
            (image.to_string(), "latest".to_string())
        };

        let full_ref_with_host = format!("{}/{}:{}", registry_host, repo, tag);
        let full_ref_repo_tag  = format!("{}:{}", repo, tag);

        remove_image_if_exists(&docker, &full_ref_with_host).await;
        remove_image_if_exists(&docker, &full_ref_repo_tag).await;

        let opts = CreateImageOptions {
            from_image: Some(from_image.clone()),
            tag: Some(tag.clone()),
            ..Default::default()
        };

        let mut first_byte_at: Option<Instant> = None;
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

                if first_byte_at.is_none() && cur_u64 > 0 {
                    first_byte_at = Some(Instant::now());
                }

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
        let inspected_size_bytes = docker
            .inspect_image(image)
            .await
            .ok()
            .and_then(|ins| ins.size)
            .unwrap_or(0) as f64;

        let cache_hit = logs.contains("Image is up to date") || bytes_downloaded == 0;
        let image_size_bytes = if inspected_size_bytes > 0.0 {
            inspected_size_bytes
        } else {
            bytes_downloaded as f64
        };

        let download_elapsed_ms = if let Some(t0) = first_byte_at {
            t0.elapsed().as_millis() as f64
        } else {
            0.0
        };

        let avg_speed_mbps = if bytes_downloaded > 0 && elapsed_ms > 0.0 {
            ((bytes_downloaded as f64) * 8.0) / (elapsed_ms / 1000.0) / 1_000_000.0
        } else {
            0.0
        };

        // Record metrics
        db::insert_metric(pool, job_id, "time_to_first_byte_ms", download_elapsed_ms, Some("ms")).await?;
        db::insert_metric(pool, job_id, "download_time_ms", elapsed_ms, Some("ms")).await?;
        db::insert_metric(pool, job_id, "image_size_bytes", image_size_bytes, Some("bytes")).await?;
        db::insert_metric(pool, job_id, "bytes_downloaded_total", bytes_downloaded as f64, Some("bytes")).await?;
        db::insert_metric(pool, job_id, "average_speed_mbps", avg_speed_mbps, Some("Mbps")).await?;
        db::insert_metric(pool, job_id, "cache_hit", if cache_hit {1.0} else {0.0}, None).await?;

        let labels = serde_json::json!({
            "image": format!("{}:{}", repo, tag),
            "registry_host": registry_host,
            "layer_count": layers.len(),
        }).to_string();

        db::insert_metric_labeled(pool, job_id, "layers_observed", layers.len() as f64, None, Some(&labels)).await?;

        Ok((elapsed_ms, bytes_downloaded, logs))
    }
    .await;

    // handle success / failure
    match result {
        Ok((elapsed_ms, bytes_downloaded, logs)) => {
            db::update_job_result(
                pool,
                job_id,
                true,
                Some(bytes_downloaded as i64),
                Some(elapsed_ms as i64),
                None,
            ).await.ok();
            db::complete_job(pool, job_id, Some(&logs)).await?;
            Ok(())
        }
        Err(e) => {
            let err_msg = format!("{:?}", e);
            Err(anyhow::anyhow!("pull failed: {}", err_msg))
        }
    }
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
