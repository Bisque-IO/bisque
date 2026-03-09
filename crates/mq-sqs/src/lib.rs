//! SQS-compatible HTTP API adapter for bisque-mq.
//!
//! Exposes an AWS SQS-compatible JSON API that translates SQS actions
//! into bisque-mq `MqCommand`/`MqResponse` interactions.

pub mod error;
pub mod handler;
pub mod router;
pub mod types;
