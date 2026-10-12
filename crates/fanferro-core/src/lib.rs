//! # fanferro-core
//!
//! The Rust core of [fanferro](https://github.com/kalaspuff/fanferro): SNS/SQS messaging
//! with pluggable envelopes, transparent large-payload offloading (claim checks),
//! declarative provisioning and a robust consumer engine (leases, heartbeats, batching,
//! dead-lettering, graceful shutdown).
//!
//! See `docs/architecture.md` in the repository for the layer diagram and invariants.

pub mod backend;
pub mod client;
pub mod consumer;
pub mod envelope;
pub mod error;
pub mod events;
pub mod metrics;
pub mod provision;
pub mod publish;
pub mod retry;
pub mod trace;
pub mod transport;
pub mod types;
pub mod util;

pub use client::{Client, ClientConfig};
pub use error::{AwsError, AwsErrorKind, Error, ResourceKind, Result};

/// Version of the core crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
