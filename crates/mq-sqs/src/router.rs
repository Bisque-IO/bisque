use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use tracing::debug;

use crate::error::SqsError;
use crate::handler::SqsState;
use crate::types::SqsAction;

/// Build an axum Router that serves the SQS-compatible API.
///
/// Mount this at any path prefix. The SQS API uses a single POST endpoint
/// with the action specified in the request body or `X-Amz-Target` header.
pub fn sqs_router(state: Arc<SqsState>) -> Router {
    Router::new()
        .route("/", post(sqs_dispatch))
        .with_state(state)
}

/// Unified SQS action dispatcher.
///
/// SQS clients send POST requests with either:
/// - `Action` field in the JSON body
/// - `X-Amz-Target` header
async fn sqs_dispatch(
    State(state): State<Arc<SqsState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // Try to parse the action from the body
    let action = match serde_json::from_slice::<SqsAction>(&body) {
        Ok(action) => action,
        Err(e) => {
            // Try extracting Action from X-Amz-Target header
            if let Some(target) = headers.get("X-Amz-Target").and_then(|v| v.to_str().ok()) {
                // X-Amz-Target format: "AmazonSQS.ActionName"
                let action_name = target.strip_prefix("AmazonSQS.").unwrap_or(target);
                // Re-parse body with injected Action field
                let mut body_map: serde_json::Map<String, serde_json::Value> =
                    match serde_json::from_slice(&body) {
                        Ok(m) => m,
                        Err(_) => serde_json::Map::new(),
                    };
                body_map.insert(
                    "Action".to_string(),
                    serde_json::Value::String(action_name.to_string()),
                );
                match serde_json::from_value::<SqsAction>(serde_json::Value::Object(body_map)) {
                    Ok(action) => action,
                    Err(e2) => {
                        return SqsError::InvalidAction(format!("{}: {}", action_name, e2))
                            .into_response();
                    }
                }
            } else {
                return SqsError::InvalidAction(e.to_string()).into_response();
            }
        }
    };

    debug!(?action, "SQS action dispatched");

    match action {
        SqsAction::CreateQueue(req) => match state.create_queue(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::DeleteQueue(req) => match state.delete_queue(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::GetQueueUrl(req) => match state.get_queue_url(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::ListQueues(_req) => {
            // TODO: implement list queues
            Json(crate::types::ListQueuesResponse {
                queue_urls: Vec::new(),
            })
            .into_response()
        }
        SqsAction::SendMessage(req) => match state.send_message(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::SendMessageBatch(req) => match state.send_message_batch(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::ReceiveMessage(req) => match state.receive_message(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::DeleteMessage(req) => match state.delete_message(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::DeleteMessageBatch(req) => match state.delete_message_batch(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::ChangeMessageVisibility(req) => {
            match state.change_message_visibility(req).await {
                Ok(resp) => resp.into_response(),
                Err(e) => e.into_response(),
            }
        }
        SqsAction::ChangeMessageVisibilityBatch(req) => {
            match state.change_message_visibility_batch(req).await {
                Ok(resp) => resp.into_response(),
                Err(e) => e.into_response(),
            }
        }
        SqsAction::PurgeQueue(req) => match state.purge_queue(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::GetQueueAttributes(req) => match state.get_queue_attributes(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => e.into_response(),
        },
        SqsAction::SetQueueAttributes(_req) => {
            // TODO: implement set queue attributes
            SqsError::InvalidAction("SetQueueAttributes not yet implemented".into()).into_response()
        }
    }
}
