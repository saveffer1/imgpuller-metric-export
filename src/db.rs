use sqlx::{Row, SqlitePool};
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;
use std::path::Path;
use tokio::fs;
use log::info;

// ---------- DB bootstrap ----------

pub async fn init_pool(db_url: &str) -> Result<SqlitePool, sqlx::Error> {
    // Create sqlite file/directories if missing
    if let Some(path_str) = db_url.strip_prefix("sqlite://") {
        let mut path_str = path_str.to_string();
        if !path_str.starts_with('/') {
            let cwd = std::env::current_dir().expect("cannot get current dir");
            path_str = format!("{}/{}", cwd.display(), path_str);
        }
        let db_path = Path::new(&path_str);
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                info!("Creating directory for database: {}", parent.display());
                fs::create_dir_all(parent).await.expect("failed to create db dir");
            }
        }
        if !db_path.exists() {
            info!("Creating empty SQLite file: {}", db_path.display());
            fs::File::create(db_path).await.expect("failed to create db file");
        }
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await?;

    // Ensure foreign keys
    sqlx::query("PRAGMA foreign_keys = ON;").execute(&pool).await?;

    // New normalized tables
    // Jobs table (single row per image pull job)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id           TEXT PRIMARY KEY,
            image        TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'queued',
            result       TEXT,
            error_detail TEXT,
            retry_count  INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at  TEXT
        );
        "#
    ).execute(&pool).await?;

    // Add resilient columns if missing
    async fn ensure_column(pool: &SqlitePool, table: &str, name: &str, def_sql: &str) -> Result<(), sqlx::Error> {
        let cols: Vec<String> = sqlx::query(&format!("PRAGMA table_info({})", table))
            .fetch_all(pool).await?
            .into_iter()
            .map(|r| r.get::<String, _>("name"))
            .collect();

        if !cols.iter().any(|c| c == name) {
            let alter = format!("ALTER TABLE {} ADD COLUMN {}", table, def_sql);
            sqlx::query(&alter).execute(pool).await?;
        }
        Ok(())
    }

    ensure_column(&pool, "jobs", "started_at",       "started_at TEXT").await?;
    ensure_column(&pool, "jobs", "updated_at",       "updated_at TEXT").await?;
    ensure_column(&pool, "jobs", "lease_expires_at", "lease_expires_at TEXT").await?;
    ensure_column(&pool, "jobs", "last_heartbeat",   "last_heartbeat TEXT").await?;
    ensure_column(&pool, "jobs", "max_attempts",     "max_attempts INTEGER NOT NULL DEFAULT 3").await?;
    ensure_column(&pool, "jobs", "priority",         "priority INTEGER NOT NULL DEFAULT 0").await?;

    // Helpful indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)").execute(&pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(lease_expires_at)").execute(&pool).await?;


    // Metrics table (many rows per job, one per metric key)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS job_metrics (
            job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            key        TEXT NOT NULL,
            value      REAL,
            unit       TEXT,
            labels_json TEXT,                -- optional JSON labels
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(job_id, key)
        );
        "#
    ).execute(&pool).await?;

    // Useful indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_job_metrics_created_at ON job_metrics(created_at DESC);")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_job_metrics_job ON job_metrics(job_id);")
        .execute(&pool)
        .await?;

    info!("SQLite database initialized successfully");
    Ok(pool)
}

// ---------- Job ops ----------

pub async fn insert_job(pool: &SqlitePool, image: &str) -> Result<String, sqlx::Error> {
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO jobs (id, image, status) VALUES (?, ?, 'queued')")
        .bind(&id)
        .bind(image)
        .execute(pool)
        .await?;
    Ok(id)
}

pub async fn update_job_status(
    pool: &SqlitePool,
    id: &str,
    status: &str,
    result: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
           SET status = ?,
               result = COALESCE(?, result),
               finished_at = CASE WHEN ? IN ('completed', 'failed') THEN datetime('now') ELSE finished_at END
         WHERE id = ?
        "#
    )
    .bind(status)
    .bind(result)
    .bind(status)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_job_result(
    pool: &SqlitePool,
    job_id: &str,
    success: bool,
    bytes: Option<i64>,
    duration_ms: Option<i64>,
    error_message: Option<String>,
) -> sqlx::Result<()> {
    let status = if success { "success" } else { "failed" };
    let finished_at = chrono::Utc::now().timestamp();
    let last_update_ts = finished_at;

    sqlx::query(
        r#"
        UPDATE pull_jobs
        SET status = ?, finished_at = ?, duration_ms = ?, bytes = ?, error_message = ?, last_update_ts = ?
        WHERE id = ?
        "#
    )
    .bind(status)
    .bind(finished_at)
    .bind(duration_ms)
    .bind(bytes)
    .bind(error_message)
    .bind(last_update_ts)
    .bind(job_id)
    .execute(pool)
    .await?;

    Ok(())
}

#[allow(unused)]
// Mark job as failed with error detail
pub async fn set_job_error(
    pool: &SqlitePool,
    id: &str,
    error_detail: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET error_detail = ?, status = 'failed', finished_at = datetime('now') WHERE id = ?")
        .bind(error_detail)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_jobs(pool: &SqlitePool) -> Result<Vec<(String, String, String)>, sqlx::Error> {
    let rows = sqlx::query("SELECT id, image, status FROM jobs ORDER BY created_at DESC")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .iter()
        .map(|r| (
            r.get::<String, _>("id"),
            r.get::<String, _>("image"),
            r.get::<String, _>("status"),
        ))
        .collect())
}

pub async fn get_job_by_id(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<(String, String, String, Option<String>)>, sqlx::Error> {
    let row = sqlx::query("SELECT id, image, status, result FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(|r| (
        r.get::<String, _>("id"),
        r.get::<String, _>("image"),
        r.get::<String, _>("status"),
        r.try_get::<Option<String>, _>("result").unwrap_or(None),
    )))
}

// ---------- Metrics ops (normalized) ----------

pub async fn insert_metric(
    pool: &SqlitePool,
    job_id: &str,
    key: &str,
    value: f64,
    unit: Option<&str>,
) -> Result<(), sqlx::Error> {
    // Upsert by (job_id, key)
    sqlx::query(
        r#"
        INSERT INTO job_metrics (job_id, key, value, unit)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(job_id, key) DO UPDATE SET
            value = excluded.value,
            unit  = COALESCE(excluded.unit, job_metrics.unit),
            created_at = datetime('now')
        "#
    )
    .bind(job_id)
    .bind(key)
    .bind(value)
    .bind(unit)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_metric_labeled(
    pool: &SqlitePool,
    job_id: &str,
    key: &str,
    value: f64,
    unit: Option<&str>,
    labels_json: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO job_metrics (job_id, key, value, unit, labels_json)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_id, key) DO UPDATE SET
            value = excluded.value,
            unit  = COALESCE(excluded.unit, job_metrics.unit),
            labels_json = COALESCE(excluded.labels_json, job_metrics.labels_json),
            created_at = datetime('now')
        "#
    )
    .bind(job_id)
    .bind(key)
    .bind(value)
    .bind(unit)
    .bind(labels_json)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MetricRow {
    pub job_id: String,
    pub key: String,
    pub value: Option<f64>,
    pub unit: Option<String>,
    pub labels_json: Option<String>,
    pub created_at: String,
}

pub async fn get_metrics_by_job(
    pool: &SqlitePool,
    job_id: &str,
) -> Result<Vec<MetricRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT job_id, key, value, unit, labels_json, created_at
           FROM job_metrics
          WHERE job_id = ?
          ORDER BY created_at DESC"
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| MetricRow {
            job_id: r.get("job_id"),
            key: r.get("key"),
            value: r.try_get("value").ok(),
            unit: r.try_get("unit").ok(),
            labels_json: r.try_get("labels_json").ok(),
            created_at: r.get("created_at"),
        })
        .collect())
}


// Atomically claim one job (queued or expired running) using an IMMEDIATE transaction.
pub async fn claim_next_job(pool: &SqlitePool, lease_secs: i64) -> Result<Option<(String, String)>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // pick next eligible job
    let row = sqlx::query(
        r#"
        SELECT id, image
          FROM jobs
         WHERE status = 'queued'
            OR (status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at < datetime('now')))
         ORDER BY priority DESC, created_at ASC
         LIMIT 1
        "#
    ).fetch_optional(&mut *tx).await?;

    if let Some(row) = row {
        let id: String = row.get("id");
        let image: String = row.get("image");

        // mark as running + set lease
        sqlx::query(
            r#"
            UPDATE jobs
               SET status='running',
                   started_at = COALESCE(started_at, datetime('now')),
                   updated_at = datetime('now'),
                   lease_expires_at = datetime('now', printf('+%d seconds', ?))
             WHERE id = ?
            "#
        )
        .bind(lease_secs)
        .bind(&id)
        .execute(&mut *tx).await?;

        tx.commit().await?;
        return Ok(Some((id, image)));
    }

    tx.commit().await?;
    Ok(None)
}

// Extend lease / heartbeat
pub async fn heartbeat_job(pool: &SqlitePool, job_id: &str, lease_secs: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
           SET last_heartbeat = datetime('now'),
               updated_at     = datetime('now'),
               lease_expires_at = datetime('now', printf('+%d seconds', ?))
         WHERE id = ?
        "#
    )
    .bind(lease_secs)
    .bind(job_id)
    .execute(pool).await?;
    Ok(())
}

// Mark a running job as completed
pub async fn complete_job(pool: &SqlitePool, job_id: &str, result: Option<&str>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
           SET status='completed',
               result=COALESCE(?, result),
               updated_at=datetime('now'),
               finished_at=datetime('now')
         WHERE id=?
        "#
    ).bind(result).bind(job_id).execute(pool).await?;
    Ok(())
}

// Mark as failed and increment retry_count
pub async fn fail_job(pool: &SqlitePool, job_id: &str, err: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
           SET status='failed',
               error_detail=?,
               updated_at=datetime('now'),
               finished_at=datetime('now'),
               retry_count = retry_count + 1
         WHERE id=?
        "#
    ).bind(err).bind(job_id).execute(pool).await?;
    Ok(())
}

#[allow(unused)]
// Recover jobs that have been running but whose lease has expired
pub async fn recover_stale_jobs(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let res = sqlx::query(
        r#"
        UPDATE jobs
           SET status='failed',
               error_detail=COALESCE(error_detail, 'lease expired / worker died'),
               updated_at=datetime('now'),
               finished_at=datetime('now')
         WHERE status='running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < datetime('now')
        "#
    ).execute(pool).await?;

    Ok(res.rows_affected() as i64)
}

pub async fn list_recent_metrics(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<MetricRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT job_id, key, value, unit, labels_json, created_at
           FROM job_metrics
          ORDER BY created_at DESC
          LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| MetricRow {
            job_id: r.get("job_id"),
            key: r.get("key"),
            value: r.try_get("value").ok(),
            unit: r.try_get("unit").ok(),
            labels_json: r.try_get("labels_json").ok(),
            created_at: r.get("created_at"),
        })
        .collect())
}
