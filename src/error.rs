use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    BadRequest(anyhow::Error),
    NotFound(anyhow::Error),
    Internal(anyhow::Error),
}

impl AppError {
    pub fn internal<E: Into<anyhow::Error>>(e: E) -> Self {
        AppError::Internal(e.into())
    }
    pub fn bad_request<E: Into<anyhow::Error>>(e: E) -> Self {
        AppError::BadRequest(e.into())
    }
    pub fn not_found<E: Into<anyhow::Error>>(e: E) -> Self {
        AppError::NotFound(e.into())
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    success: bool,
    status_code: u16,
    message: &'a str,
    error: String,
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::BadRequest(e) => write!(f, "BadRequest: {e}"),
            AppError::NotFound(e) => write!(f, "NotFound: {e}"),
            AppError::Internal(e) => write!(f, "Internal: {e}"),
        }
    }
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let is_dev = std::env::var("APP_ENV").unwrap_or_default() == "development";

        let (status, title, err_str) = match self {
            AppError::BadRequest(e) => (StatusCode::BAD_REQUEST, "Bad Request", format!("{e}")),
            AppError::NotFound(e) => (StatusCode::NOT_FOUND, "Not Found", format!("{e}")),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error", format!("{e}")),
        };

        let error_text = if is_dev {
            format!("Debug info: {err_str}")
        } else {
            match status {
                StatusCode::BAD_REQUEST => "Invalid request".to_string(),
                StatusCode::NOT_FOUND => "Resource not found".to_string(),
                _ => "An unexpected error occurred".to_string(),
            }
        };

        let body = ErrorBody {
            success: false,
            status_code: status.as_u16(),
            message: title,
            error: error_text,
        };

        HttpResponse::build(status)
            .content_type("application/json")
            .json(body)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Internal(e)
    }
}
