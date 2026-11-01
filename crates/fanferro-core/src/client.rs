//! The core client: backend, transport, events and metrics bundled together.

use std::sync::Arc;

use serde::Deserialize;

use crate::backend::Backend;
use crate::backend::aws::{self, AwsConfig};
use crate::error::Result;
use crate::events::EventBus;
use crate::metrics::ClientMetrics;
use crate::retry::RetrySettings;
use crate::transport::{FetchSettings, OffloadSettings, Transport};

fn default_event_capacity() -> usize {
    10_000
}

/// Client-wide settings (deserialized from the Python configuration).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// Large payload offloading for outgoing messages (`None` disables it).
    #[serde(default)]
    pub offload: Option<OffloadSettings>,
    /// How incoming payload pointers are resolved.
    #[serde(default)]
    pub fetch: FetchSettings,
    /// Retries for batch entries the service reports as failed (or leaves out of its response)
    /// and for checksum mismatches; failed batch calls are retried by the SDK only.
    #[serde(default)]
    pub publish_retry: RetrySettings,
    /// Verify the MD5 digests returned by SQS for sent and received messages (opt-in: TLS
    /// already protects messages in transit).
    #[serde(default)]
    pub verify_md5: bool,
    /// Split batch publishes to topics so that the entries of each `PublishBatch` call have the
    /// same trace header: SNS takes one `X-Amzn-Trace-Id` per call, so a call whose entries
    /// differ sends none. Off by default: one call per ten entries, as for queues.
    #[serde(default)]
    pub split_batches_by_trace_header: bool,
    /// Maximum number of buffered events before new ones are dropped.
    #[serde(default = "default_event_capacity")]
    pub event_capacity: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            offload: None,
            fetch: FetchSettings::default(),
            publish_retry: RetrySettings::default(),
            verify_md5: false,
            split_batches_by_trace_header: false,
            event_capacity: default_event_capacity(),
        }
    }
}

pub(crate) struct ClientInner {
    pub(crate) backend: Backend,
    pub(crate) transport: Transport,
    pub(crate) events: Arc<EventBus>,
    pub(crate) metrics: Arc<ClientMetrics>,
    pub(crate) config: ClientConfig,
}

/// Entry point of the core. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("backend", &self.inner.backend.kind)
            .field("region", &self.inner.backend.region)
            .finish()
    }
}

impl Client {
    /// Connects to AWS (or an AWS-compatible endpoint).
    pub async fn connect(aws_config: &AwsConfig, config: ClientConfig) -> Result<Self> {
        let backend = aws::connect(aws_config).await?;
        Self::with_backend(backend, config)
    }

    /// Creates a client over an existing backend (e.g. the in-memory broker).
    pub fn with_backend(backend: Backend, config: ClientConfig) -> Result<Self> {
        let transport = Transport::new(backend.clone(), config.offload.clone(), config.fetch.clone())?;
        config.publish_retry.backoff()?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                backend,
                transport,
                events: Arc::new(EventBus::new(config.event_capacity)),
                metrics: Arc::new(ClientMetrics::default()),
                config,
            }),
        })
    }

    /// The backend.
    pub fn backend(&self) -> &Backend {
        &self.inner.backend
    }

    /// The transport (with the client-wide offload settings).
    pub fn transport(&self) -> &Transport {
        &self.inner.transport
    }

    /// The event bus.
    pub fn events(&self) -> &Arc<EventBus> {
        &self.inner.events
    }

    /// Client metrics.
    pub fn metrics(&self) -> &Arc<ClientMetrics> {
        &self.inner.metrics
    }

    /// The configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    /// The region of the backend.
    pub fn region(&self) -> &str {
        &self.inner.backend.region
    }

    /// The account id of the caller (cached).
    pub async fn account_id(&self) -> Result<String> {
        self.inner.backend.identity.account_id().await
    }
}
