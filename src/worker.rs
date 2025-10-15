use std::sync::Arc;
use sqlx::SqlitePool;
use tokio::time::{sleep, Duration};
use tokio::sync::Semaphore;
use log::{info, warn, error};

use crate::db;
use crate::routes::job;

/// Background job runner.
/// - `concurrency`: max number of concurrent pulls
/// - `lease_secs`: lease duration for each claimed job
pub async fn run_job_runner(pool: SqlitePool, concurrency: usize, lease_secs: i64) {
    let sem = Arc::new(Semaphore::new(concurrency));

    loop {
        // Recover any stale running jobs
        if let Ok(n) = db::recover_stale_jobs(&pool).await {
            if n > 0 { warn!("Recovered {} stale jobs", n); }
        }

        match db::claim_next_job(&pool, lease_secs).await {
            Ok(Some((job_id, image))) => {
                let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
                let pool_cloned = pool.clone();

                tokio::spawn(async move {
                    let _p = permit; // hold until task ends
                    info!("Running job {} for image {}", job_id, image);

                    // heartbeat loop (optional)
                    let hb_pool = pool_cloned.clone();
                    let hb_job = job_id.clone();
                    let hb = tokio::spawn(async move {
                        loop {
                            if let Err(e) = db::heartbeat_job(&hb_pool, &hb_job, lease_secs).await {
                                warn!("heartbeat failed for {}: {}", hb_job, e);
                            }
                            sleep(Duration::from_secs((lease_secs / 2).max(1) as u64)).await;
                        }
                    });

                    // run the pull
                    let res = job::pull_image_and_record_metrics(&pool_cloned, &job_id, &image).await;

                    // stop heartbeat
                    hb.abort();

                    if let Err(e) = res {
                        error!("job {} failed: {:#}", job_id, e);
                        let _ = db::fail_job(&pool_cloned, &job_id, &format!("{e:#}")).await;
                    } else {
                        info!("job {} completed", job_id);
                    }
                });
            }
            Ok(None) => {
                // no jobs; sleep briefly
                sleep(Duration::from_millis(800)).await;
            }
            Err(e) => {
                error!("claim_next_job error: {:#}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
