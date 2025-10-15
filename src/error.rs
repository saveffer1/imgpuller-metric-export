use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use crate::model::ErrorResponse;
use std::fmt::{self, Display};

#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl AppError {
    pub fn bad_request(msg: impl Into<String>) -> Self { Self::BadRequest(msg.into()) }
    pub fn not_found(msg: impl Into<String>) -> Self { Self::NotFound(msg.into()) }
    pub fn internal(msg: impl Into<String>) -> Self { Self::Internal(msg.into()) }
}

// --- Display (required by ResponseError supertrait) ---
impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::BadRequest(m) => write!(f, "bad request: {}", m),
            AppError::NotFound(m)  => write!(f, "not found: {}", m),
            AppError::Internal(m)  => write!(f, "internal error: {}", m),
        }
    }
}

// --- Mappings from common errors to AppError::Internal ---
impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self { Self::Internal(e.to_string()) }
}
impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self { Self::Internal(e.to_string()) }
}
impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self { Self::Internal(e.to_string()) }
}
// If you use bollard errors in routes/workers, this helps too:
impl From<bollard::errors::Error> for AppError {
    fn from(e: bollard::errors::Error) -> Self { Self::Internal(e.to_string()) }
}

// --- ResponseError: build uniform JSON using your ErrorResponse model ---
impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::NotFound(_)  => StatusCode::NOT_FOUND,
            AppError::Internal(_)  => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let (status, msg, err) = match self {
            AppError::BadRequest(m) => (StatusCode::BAD_REQUEST, "bad request", m.as_str()),
            AppError::NotFound(m)  => (StatusCode::NOT_FOUND, "not found", m.as_str()),
            AppError::Internal(m)  => (StatusCode::INTERNAL_SERVER_ERROR, "internal error", m.as_str()),
        };
        HttpResponse::build(status).json(ErrorResponse::new(status.as_u16(), msg, err))
    }
}
