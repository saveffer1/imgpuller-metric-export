use std::env;
use validator::{Validate, ValidationError};

#[derive(Debug, Validate, Clone)]
pub struct AppConfig {
    #[validate(length(min = 3))]
    pub app_env: String,

    #[validate(range(min = 1, max = 65535))]
    pub app_port: u16,

    #[validate(custom(function = "validate_db_url"))]
    pub database_url: String,
}

fn validate_db_url(url: &str) -> Result<(), ValidationError> {
    if !(url.starts_with("postgres://") || url.starts_with("sqlite://")) {
        return Err(ValidationError::new("invalid_database_url"));
    }
    Ok(())
}

impl AppConfig {
    pub fn from_env() -> Self {
        // ดึงค่าจาก environment ด้วย std::env
        let app_env = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let app_port = env::var("APP_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .expect("❌ APP_PORT must be a number between 1–65535");
        let database_url =
            env::var("DATABASE_URL").expect("❌ DATABASE_URL environment variable not set");

        let cfg = AppConfig {
            app_env,
            app_port,
            database_url,
        };

        cfg.validate().expect("❌ Invalid configuration values");
        cfg
    }
}
