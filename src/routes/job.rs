use std::collections::HashMap;
use std::time::Instant;

use actix_web::{get, post, web, HttpResponse, Responder};
use bollard::query_parameters::{CreateImageOptions, RemoveImageOptions};
use bollard::Docker;
use futures_util::TryStreamExt;
use log::warn;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

use crate::db;

// -----------------------------
// Public routes configurator
// -----------------------------
pub fn job_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(create_job)
        .service(list_jobs)
        .service(get_job);
}

// -----------------------------
// Request/Response types
// -----------------------------
#[derive(Deserialize)]
pub struct CreateJobRequest {
    pub image: String,
}

#[derive(Serialize)]
struct ApiResp<T> {
    success: bool,
    data: T,
}

#[derive(Serialize)]
struct JobListItem {
    id: String,
    image: String,
    status: String,
}

#[derive(Serialize)]
struct JobDetail {
    id: String,
    image: String,
    status: String,
    result: Option<String>,
    error_detail: Option<String>,
    retry_count: i64,
    created_at: String,
    finished_at: Option<String>,
}

// -----------------------------
// Routes
// -----------------------------

#[post("/jobs")]
pub async fn create_job(
    pool: web::Data<SqlitePool>,
    body: web::Json<CreateJobRequest>,
) -> actix_web::Result<impl Responder> {
    let image = body.image.trim();
    if image.is_empty() {
        return Ok(HttpResponse::BadRequest().body("image is required"));
    }

    // Cargo.toml: uuid = { version = "1", features = ["v4"] }
    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query(
        r#"
        INSERT INTO jobs (id, image, status)
        VALUES (?, ?, 'queued')
        "#,
    )
    .bind(&id)
    .bind(image)
    .execute(pool.get_ref())
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    let resp = ApiResp {
        success: true,
        data: JobListItem {
            id,
            image: image.to_string(),
            status: "queued".to_string(),
        },
    };
    Ok(HttpResponse::Ok().json(resp))
}

#[get("/jobs")]
pub async fn list_jobs(pool: web::Data<SqlitePool>) -> actix_web::Result<impl Responder> {
    let rows = sqlx::query(
        r#"
        SELECT id, image, status
          FROM jobs
      ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut items = Vec::with_capacity(rows.len());
    for r in rows {
        items.push(JobListItem {
            id: r.get::<String, _>("id"),
            image: r.get::<String, _>("image"),
            status: r.get::<String, _>("status"),
        });
    }

    Ok(HttpResponse::Ok().json(ApiResp { success: true, data: items }))
}

#[get("/jobs/{id}")]
pub async fn get_job(
    path: web::Path<String>,
    pool: web::Data<SqlitePool>,
) -> actix_web::Result<impl Responder> {
    let id = path.into_inner();

    let row_opt = sqlx::query(
        r#"
        SELECT id, image, status, result, error_detail, retry_count, created_at, finished_at
          FROM jobs
         WHERE id = ?
        "#,
    )
    .bind(&id)
    .fetch_optional(pool.get_ref())
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    match row_opt {
        None => Ok(HttpResponse::NotFound().body("Job not found")),
        Some(r) => {
            let result_raw: Option<String> = r.get::<Option<String>, _>("result");
            let result_short = result_raw.map(|s| truncate(&s, 500));

            let detail = JobDetail {
                id: r.get::<String, _>("id"),
                image: r.get::<String, _>("image"),
                status: r.get::<String, _>("status"),
                result: result_short,
                error_detail: r.get::<Option<String>, _>("error_detail"),
                retry_count: r.get::<i64, _>("retry_count"),
                created_at: r.get::<String, _>("created_at"),
                finished_at: r.get::<Option<String>, _>("finished_at"),
            };

            Ok(HttpResponse::Ok().json(ApiResp { success: true, data: detail }))
        }
    }
}

// -----------------------------
// Worker-facing function
// -----------------------------

/// Called by worker to pull image and record metrics.
/// Success: write short summary to jobs.result and set status='completed'.
/// Failure: return Err so worker can call db::set_job_error(..., true).
pub async fn pull_image_and_record_metrics(
    pool: &SqlitePool,
    job_id: &str,
    image: &str,
) -> anyhow::Result<()> {
    let docker = Docker::connect_with_unix_defaults()
        .map_err(|e| anyhow::anyhow!("docker connect error: {e}"))?;

    let (registry_host, _repo_from_parse, _tag_from_parse) = parse_image_ref(image);
    let (repo, tag) = split_repo_tag(image);
    let full_ref_repo_tag = format!("{}:{}", repo, tag);

    // pre-clean (ignore if absent)
    remove_image_if_exists(&docker, &format!("{}/{}", registry_host, full_ref_repo_tag)).await;
    remove_image_if_exists(&docker, &full_ref_repo_tag).await;

    let from_image = build_from_image(&registry_host, &repo);
    let started = Instant::now();

    let opts = CreateImageOptions {
        from_image: Some(from_image.clone()),
        tag: Some(tag.clone()),
        ..Default::default()
    };

    let mut stream = docker.create_image(Some(opts), None, None);
    let mut first_byte_at: Option<Instant> = None;
    let mut layers: HashMap<String, (u64, u64)> = HashMap::new();
    let mut logs = String::new();
    let mut digest: Option<String> = None;

    while let Some(item) = stream.try_next().await? {
        if let Some(status) = item.status.as_deref() {
            if status.starts_with("Digest:") {
                digest = Some(status.trim_start_matches("Digest:").trim().to_string());
            }
            logs.push_str(status);
            if let Some(id) = item.id.as_deref() {
                logs.push_str(" [");
                logs.push_str(id);
                logs.push(']');
            }
            if let Some(progress) = item.progress.as_deref() {
                logs.push_str(" - ");
                logs.push_str(progress);
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

    let (sum_cur, sum_tot) = layers
        .values()
        .fold((0u64, 0u64), |acc, &(c, t)| (acc.0.saturating_add(c), acc.1.saturating_add(t)));
    let bytes_downloaded = if sum_tot > 0 { sum_tot } else { sum_cur };

    let inspected_size_bytes = docker
        .inspect_image(&full_ref_repo_tag)
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

    let download_elapsed_ms = first_byte_at
        .map(|t0| t0.elapsed().as_millis() as f64)
        .unwrap_or(0.0);

    let avg_speed_mbps = if bytes_downloaded > 0 && elapsed_ms > 0.0 {
        ((bytes_downloaded as f64) * 8.0) / (elapsed_ms / 1000.0) / 1_000_000.0
    } else {
        0.0
    };

    // metrics (separate table)
    db::insert_metric(pool, job_id, "download_time_ms", elapsed_ms, Some("ms")).await?;
    db::insert_metric(pool, job_id, "image_size_bytes", image_size_bytes, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "bytes_downloaded_total", bytes_downloaded as f64, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "image_size_reported_bytes", inspected_size_bytes, Some("bytes")).await?;
    db::insert_metric(pool, job_id, "download_ttfb_ms", download_elapsed_ms, Some("ms")).await?;
    db::insert_metric(pool, job_id, "average_speed_mbps", avg_speed_mbps, Some("Mbps")).await?;
    db::insert_metric(pool, job_id, "cache_hit", if cache_hit { 1.0 } else { 0.0 }, None).await?;

    let labels = serde_json::json!({
        "image": format!("{}:{}", repo, tag),
        "registry_host": registry_host,
        "layer_count": layers.len(),
    })
    .to_string();
    db::insert_metric_labeled(pool, job_id, "layers_observed", layers.len() as f64, None, Some(&labels)).await?;

    // short summary (no long logs)
    let digest_str = digest.as_deref().unwrap_or("-");
    let summary = format!(
        "Pulled {} from {} • size ~{:.1} MB • layers {} • cache_hit={} • digest {}",
        full_ref_repo_tag,
        registry_host,
        image_size_bytes / 1_000_000.0,
        layers.len(),
        cache_hit,
        digest_str
    );

    db::update_job_status(pool, job_id, "completed", Some(&summary)).await?;

    Ok(())
}

// -----------------------------
// Helpers
// -----------------------------

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}… (+{} chars)", &s[..max], s.len() - max)
    }
}

fn parse_image_ref(image: &str) -> (String, String, String) {
    let mut parts = image.split('/');

    let first = parts.next().unwrap_or("");
    let (registry_host, remainder) = if first.contains('.') || first.contains(':') || first == "localhost" {
        (first.to_string(), parts.collect::<Vec<_>>().join("/"))
    } else {
        ("docker.io".to_string(), {
            if first.is_empty() {
                "".to_string()
            } else {
                let mut v = vec![first.to_string()];
                v.extend(parts.map(|s| s.to_string()));
                v.join("/")
            }
        })
    };

    let (repo, tag) = split_repo_tag(&remainder);
    (registry_host, repo, tag)
}

fn split_repo_tag(image: &str) -> (String, String) {
    if let Some((r, t)) = image.rsplit_once(':') {
        (r.to_string(), t.to_string())
    } else {
        (image.to_string(), "latest".to_string())
    }
}

async fn remove_image_if_exists(docker: &Docker, name: &str) {
    let opts = Some(RemoveImageOptions {
        force: true,
        noprune: false,
    });

    if let Err(e) = docker.remove_image(name, opts, None).await {
        #[cfg(debug_assertions)]
        warn!("remove_image_if_exists({}): {}", name, e);
    }
}

fn build_from_image(registry_host: &str, repo: &str) -> String {
    if registry_host == "docker.io" {
        if repo.contains('/') {
            repo.to_string()
        } else {
            format!("library/{}", repo)
        }
    } else {
        format!("{}/{}", registry_host, repo)
    }
}
