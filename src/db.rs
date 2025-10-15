use std::str::FromStr;
use std::time::Duration;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

/// ---------- Job row types ----------
#[derive(Debug, Clone)]
pub struct DbJobListItem {
    pub id: String,
    pub image: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct DbJobDetail {
    pub id: String,
    pub image: String,
    pub status: String,
    pub result: Option<String>,
    pub error_detail: Option<String>,
    pub retry_count: i64,
    pub created_at: String,
    pub finished_at: Option<String>,
}

/// ---------- Metric row type ----------
#[derive(Debug, Clone)]
pub struct MetricRow {
    pub job_id: String,
    pub key: String,
    pub value: f64,
    pub unit: Option<String>,
    pub labels_json: Option<String>,
    pub created_at: String,
}

/// Create a SqlitePool
pub async fn init_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let opts = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(30));

    SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await
}

/// Initialize schema (used by `--init-db`)
pub async fn init_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    // Jobs
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
        "#,
    )
    .execute(pool)
    .await?;

    // Metrics
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS metrics (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id       TEXT NOT NULL,
            key          TEXT NOT NULL,
            value        REAL NOT NULL,
            unit         TEXT,
            labels_json  TEXT,
            created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );
        "#,
    )
    .execute(pool)
    .await?;

    // Helpful index
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_metrics_job_created
            ON metrics(job_id, created_at DESC);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

//
// ---------------------- Jobs API ----------------------
//

/// Insert a new job (queued)
pub async fn insert_job(pool: &SqlitePool, id: &str, image: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO jobs (id, image, status)
        VALUES (?, ?, 'queued')
        "#,
    )
    .bind(id)
    .bind(image)
    .execute(pool)
    .await?;
    Ok(())
}

/// List jobs (short)
pub async fn list_jobs(pool: &SqlitePool) -> Result<Vec<DbJobListItem>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, image, status
          FROM jobs
      ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| DbJobListItem {
            id: r.get::<String, _>("id"),
            image: r.get::<String, _>("image"),
            status: r.get::<String, _>("status"),
        })
        .collect();

    Ok(items)
}

/// Get job detail
pub async fn get_job_by_id(pool: &SqlitePool, id: &str) -> Result<Option<DbJobDetail>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT id, image, status, result, error_detail, retry_count, created_at, finished_at
          FROM jobs
         WHERE id = ?
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| DbJobDetail {
        id: r.get("id"),
        image: r.get("image"),
        status: r.get("status"),
        result: r.get("result"),
        error_detail: r.get("error_detail"),
        retry_count: r.get("retry_count"),
        created_at: r.get("created_at"),
        finished_at: r.get("finished_at"),
    }))
}

/// Update status; if completed/failed, set finished_at
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
               finished_at = CASE WHEN ? IN ('completed', 'failed')
                                  THEN datetime('now')
                                  ELSE finished_at
                             END
         WHERE id = ?
        "#,
    )
    .bind(status)
    .bind(result)
    .bind(status)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Set error_detail; optionally mark failed
pub async fn set_job_error(
    pool: &SqlitePool,
    id: &str,
    error_detail: &str,
    mark_failed: bool,
) -> Result<(), sqlx::Error> {
    if mark_failed {
        sqlx::query(
            r#"
            UPDATE jobs
               SET error_detail = ?,
                   status = 'failed',
                   finished_at = COALESCE(finished_at, datetime('now'))
             WHERE id = ?
            "#,
        )
        .bind(error_detail)
        .bind(id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            r#"
            UPDATE jobs
               SET error_detail = ?
             WHERE id = ?
            "#,
        )
        .bind(error_detail)
        .bind(id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn complete_job(pool: &SqlitePool, id: &str, result: Option<&str>) -> Result<(), sqlx::Error> {
    update_job_status(pool, id, "completed", result).await
}

/// Optimistic claim: read one queued, then flip to running if still queued.
pub async fn claim_next_job(
    pool: &SqlitePool,
    _lease_secs: i64,
) -> Result<Option<(String, String)>, sqlx::Error> {
    loop {
        let row_opt = sqlx::query(
            r#"
            SELECT id, image
              FROM jobs
             WHERE status = 'queued'
          ORDER BY created_at ASC
             LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let id: String = row.get("id");
        let image: String = row.get("image");

        let res = sqlx::query(
            r#"
            UPDATE jobs
               SET status = 'running'
             WHERE id = ? AND status = 'queued'
            "#,
        )
        .bind(&id)
        .execute(pool)
        .await?;

        if res.rows_affected() == 1 {
            return Ok(Some((id, image)));
        }

        // Lost the race; loop again.
    }
}

/// No-op heartbeat to keep runner signature compatible
pub async fn heartbeat_job(_pool: &SqlitePool, _job_id: &str, _lease_secs: i64) -> Result<(), sqlx::Error> {
    Ok(())
}

//
// ---------------------- Metrics API ----------------------
//

pub async fn insert_metric(
    pool: &SqlitePool,
    job_id: &str,
    key: &str,
    value: f64,
    unit: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO metrics (job_id, key, value, unit, created_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        "#,
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
        INSERT INTO metrics (job_id, key, value, unit, labels_json, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        "#,
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

pub async fn get_metrics_by_job(pool: &SqlitePool, job_id: &str) -> Result<Vec<MetricRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT job_id, key, value, unit, labels_json, created_at
          FROM metrics
         WHERE job_id = ?
      ORDER BY created_at DESC
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| MetricRow {
            job_id: r.get("job_id"),
            key: r.get("key"),
            value: r.get("value"),
            unit: r.get("unit"),
            labels_json: r.get("labels_json"),
            created_at: r.get("created_at"),
        })
        .collect();

    Ok(items)
}

pub async fn list_recent_metrics(pool: &SqlitePool, limit: i64) -> Result<Vec<MetricRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT job_id, key, value, unit, labels_json, created_at
          FROM metrics
      ORDER BY created_at DESC
         LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| MetricRow {
            job_id: r.get("job_id"),
            key: r.get("key"),
            value: r.get("value"),
            unit: r.get("unit"),
            labels_json: r.get("labels_json"),
            created_at: r.get("created_at"),
        })
        .collect();

    Ok(items)
}
