use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    pub data: T,
}

impl<T> ApiResponse<T> {
    pub fn ok(message: &str, data: T) -> Self {
        Self {
            success: true,
            message: message.into(),
            data,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    pub success: bool,
    pub status_code: u16,
    pub message: String,
    pub error: String,
}

impl ErrorResponse {
    pub fn new(status_code: u16, message: &str, error: &str) -> Self {
        Self {
            success: false,
            status_code,
            message: message.into(),
            error: error.into(),
        }
    }
}
