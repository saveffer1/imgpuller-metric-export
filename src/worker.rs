use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info, warn};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Duration};

use crate::db;
use crate::routes::job;

/// Parse registry host from an image reference.
/// If no explicit registry is provided, default to "docker.io".
fn parse_registry(image: &str) -> String {
    // Docker heuristic:
    // If the first path component contains '.' or ':' or equals "localhost", treat it as a registry.
    // Otherwise default to docker.io
    let first = image.split('/').next().unwrap_or("");
    if first.contains('.') || first.contains(':') || first == "localhost" {
        first.to_string()
    } else {
        "docker.io".to_string()
    }
}

/// Get or create a semaphore for a specific registry.
async fn get_or_create_reg_sem(
    map: &Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    registry: &str,
    per_registry_max: usize,
) -> Arc<Semaphore> {
    let mut guard = map.lock().await;
    guard
        .entry(registry.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(per_registry_max)))
        .clone()
}

/// Run the job runner loop.
///
/// - `pool`: database pool
/// - `concurrency`: global max concurrent pulls
/// - `per_registry_max`: max concurrent pulls per registry (e.g., docker.io, gcr.io)
/// - `lease_secs`: lease duration used by DB when claiming a job
pub async fn run_job_runner(
    pool: SqlitePool,
    concurrency: usize,
    per_registry_max: usize,
    lease_secs: i64,
) {
    let global_sem = Arc::new(Semaphore::new(concurrency));
    let reg_map: Arc<Mutex<HashMap<String, Arc<Semaphore>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Delays
    let idle_delay = Duration::from_millis(500);
    let error_delay = Duration::from_millis(1000);

    info!(
        "job-runner started: concurrency={}, per_registry_max={}, lease_secs={}",
        concurrency, per_registry_max, lease_secs
    );

    loop {
        // NOTE: FIX — pass lease_secs as the 2nd argument to match db.rs signature
        let claim = db::claim_next_job(&pool, lease_secs).await;

        match claim {
            Ok(Some((job_id, image))) => {
                // Acquire a global permit (limit overall concurrency)
                let Ok(global_permit) = global_sem.clone().acquire_owned().await else {
                    warn!("global semaphore closed; stopping runner loop");
                    break;
                };

                let pool_cloned = pool.clone();
                let reg_map_cloned = reg_map.clone();

                // Determine registry from image ref
                let registry = parse_registry(&image);
                let per_reg = per_registry_max;

                if let Err(e) = db::update_job_status(&pool, &job_id, "running", /* started_at */ None).await {
                    warn!("job {}: cannot mark running: {:#}", job_id, e);
                }

                tokio::spawn(async move {
                    // Acquire per-registry slot
                    let reg_sem = get_or_create_reg_sem(&reg_map_cloned, &registry, per_reg).await;
                    let Ok(_reg_permit) = reg_sem.acquire_owned().await else {
                        warn!("registry semaphore closed for {}; job {}", registry, job_id);
                        let _ = db::fail_job(&pool_cloned, &job_id, "registry semaphore closed").await;
                        drop(global_permit);
                        return;
                    };

                    info!(
                        "job {}: starting pull for image '{}' (registry: {})",
                        job_id, image, registry
                    );

                    let hb_pool = pool_cloned.clone();
                    let hb_job = job_id.clone();
                    let hb_interval = Duration::from_secs((lease_secs / 2).max(1) as u64);
                    let (hb_tx, mut hb_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

                    // heartbeat task
                    let hb_handle = tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                _ = sleep(hb_interval) => {
                                    if let Err(e) = db::heartbeat_job(&hb_pool, &hb_job, lease_secs).await {
                                        warn!("job {}: heartbeat failed: {:#}", hb_job, e);
                                        // ถ้า heartbeat ล้มเหลว อาจจะลองต่ออายุอีก 1-2 ครั้ง หรือตัดสินใจหยุด
                                    }
                                }
                                _ = hb_rx.recv() => {
                                    break; // stop heartbeat when job ends
                                }
                            }
                        }
                    });

                    // Actual pull (success path completes the job inside this function)
                    let pull_res = job::pull_image_and_record_metrics(&pool_cloned, &job_id, &image).await;

                    let _ = hb_tx.send(());
                    let _ = hb_handle.await;

                    match pull_res {
                        Ok(()) => {
                            info!("job {}: completed successfully", job_id);
                            // Do NOT complete here again to avoid double-marking.
                        }
                        Err(e) => {
                            error!("job {}: failed: {:#}", job_id, e);
                            // Worker marks failed with detailed error message
                            let _ = db::fail_job(&pool_cloned, &job_id, &format!("{:#}", e)).await;
                        }
                    }

                    // When this task ends, both permits (global + per-registry) drop automatically.
                    drop(global_permit);
                });
            }

            Ok(None) => {
                // No job found; wait a bit
                sleep(idle_delay).await;
            }

            Err(e) => {
                warn!("claim_next_job error: {:#}", e);
                sleep(error_delay).await;
            }
        }
    }

    info!("job-runner stopped");
}
