//! MQTT 3.1.1/5.0 protocol adapter for bisque-mq.
//!
//! Translates MQTT protocol operations into bisque-mq MqCommand/MqResponse
//! interactions via the MqWriteBatcher.

pub mod codec;
pub mod server;
pub mod session;
pub mod types;
