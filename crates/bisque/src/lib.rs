//! bisque — Unified Server
//!
//! Composes bisque-meta (control plane), bisque-lance (analytics), and future
//! engines (mq, libsql) into a single authenticated server binary.
//!
//! # Architecture
//!
//! ```text
//! bisque (unified server)
//! ├── bisque-meta   — tenants, catalogs, API keys, routing
//! ├── bisque-lance  — S3 API, WebSocket, Flight SQL, OTLP
//! ├── bisque-mq     — message queue (topics, queues, actors, jobs)
//! └── bisque-libsql — SQLite w/ Postgres wire (future)
//! ```
//!
//! All engine APIs are protected by bearer token auth backed by bisque-meta's
//! token system. Engine crates stay auth-unaware.

pub mod auth;
pub mod config;
pub mod error;
pub mod mq;
pub mod server;
pub mod ws;

pub use auth::AuthContext;
pub use config::BisqueConfig;
pub use error::ApiError;
pub use server::ServerHandle;
