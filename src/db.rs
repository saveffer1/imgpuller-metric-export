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
