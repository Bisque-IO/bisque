//! AMQP 1.0 protocol adapter for bisque-mq.
//!
//! Implements the AMQP 1.0 (ISO/IEC 19464:2014) protocol as a thin adapter
//! over bisque-mq's Raft-replicated topic/queue engine. AMQP 1.0 clients
//! connect to bisque-mq as a standard AMQP 1.0 broker.

pub mod broker;
pub mod codec;
pub mod connection;
pub mod link;
pub mod server;
pub mod transport;
pub mod types;
