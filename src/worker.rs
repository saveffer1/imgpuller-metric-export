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
        // claim_next_job ต้องรับ (pool, lease_secs)
        let claim = db::claim_next_job(&pool, lease_secs).await;

        match claim {
            Ok(Some((job_id, image))) => {
                // Global concurrency gate
                let Ok(global_permit) = global_sem.clone().acquire_owned().await else {
                    warn!("global semaphore closed; stopping runner loop");
                    break;
                };

                // mark job as running
                if let Err(e) = db::update_job_status(&pool, &job_id, "running", None).await {
                    warn!("job {}: cannot mark running: {:#}", job_id, e);
                }

                let pool_cloned = pool.clone();
                let reg_map_cloned = reg_map.clone();

                // Determine registry from image ref
                let registry = parse_registry(&image);
                let per_reg = per_registry_max;

                tokio::spawn(async move {
                    // Per-registry concurrency gate
                    let reg_sem = get_or_create_reg_sem(&reg_map_cloned, &registry, per_reg).await;
                    let Ok(_reg_permit) = reg_sem.acquire_owned().await else {
                        warn!("registry semaphore closed for {}; job {}", registry, job_id);
                        // บันทึก error_detail แล้วปิดงาน
                        let _ = db::set_job_error(&pool_cloned, &job_id, "registry semaphore closed", true).await;
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

                    let hb_handle = tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                _ = sleep(hb_interval) => {
                                    if let Err(e) = db::heartbeat_job(&hb_pool, &hb_job, lease_secs).await {
                                        warn!("job {}: heartbeat failed: {:#}", hb_job, e);
                                    }
                                }
                                _ = hb_rx.recv() => {
                                    break;
                                }
                            }
                        }
                    });

                    let pull_res = job::pull_image_and_record_metrics(&pool_cloned, &job_id, &image).await;

                    let _ = hb_tx.send(());
                    let _ = hb_handle.await;

                    match pull_res {
                        Ok(()) => {
                            info!("job {}: completed successfully", job_id);
                        }
                        Err(e) => {
                            error!("job {}: failed: {:#}", job_id, e);
                            let _ = db::set_job_error(&pool_cloned, &job_id, &format!("{:#}", e), true).await;
                        }
                    }

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
