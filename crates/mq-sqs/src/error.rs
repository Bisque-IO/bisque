use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum SqsError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("queue already exists: {0}")]
    QueueAlreadyExists(String),

    #[error("invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("missing parameter: {0}")]
    MissingParameter(String),

    #[error("invalid receipt handle")]
    InvalidReceiptHandle,

    #[error("invalid action: {0}")]
    InvalidAction(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("batcher error: {0}")]
    Batcher(#[from] bisque_mq::MqBatcherError),
}

#[derive(Serialize)]
struct SqsErrorResponse {
    #[serde(rename = "Type")]
    error_type: &'static str,
    #[serde(rename = "Code")]
    code: &'static str,
    #[serde(rename = "Message")]
    message: String,
}

impl SqsError {
    fn code(&self) -> &'static str {
        match self {
            Self::QueueNotFound(_) => "AWS.SimpleQueueService.NonExistentQueue",
            Self::QueueAlreadyExists(_) => "QueueAlreadyExists",
            Self::InvalidParameter(_) => "InvalidParameterValue",
            Self::MissingParameter(_) => "MissingParameter",
            Self::InvalidReceiptHandle => "ReceiptHandleIsInvalid",
            Self::InvalidAction(_) => "InvalidAction",
            Self::Internal(_) => "InternalError",
            Self::Batcher(_) => "InternalError",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::QueueNotFound(_) => StatusCode::BAD_REQUEST,
            Self::QueueAlreadyExists(_) => StatusCode::BAD_REQUEST,
            Self::InvalidParameter(_) => StatusCode::BAD_REQUEST,
            Self::MissingParameter(_) => StatusCode::BAD_REQUEST,
            Self::InvalidReceiptHandle => StatusCode::BAD_REQUEST,
            Self::InvalidAction(_) => StatusCode::BAD_REQUEST,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Batcher(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for SqsError {
    fn into_response(self) -> Response {
        let body = SqsErrorResponse {
            error_type: "Sender",
            code: self.code(),
            message: self.to_string(),
        };
        (self.status(), axum::Json(body)).into_response()
    }
}
