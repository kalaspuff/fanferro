//! `CoreClient`: publishing, provisioning, events and metrics; plus the in-memory broker.

use std::sync::Arc;
use std::time::Duration;

use fanferro_core::backend::BackendKind;
use fanferro_core::backend::aws::AwsConfig;
use fanferro_core::backend::memory::MemoryBroker;
use fanferro_core::consumer::{ConsumerConfig, ConsumerEngine};
use fanferro_core::envelope::EnvelopeSet;
use fanferro_core::events::{Event, EventKind, FieldValue};
use fanferro_core::provision::{
    Drift, QueueInfo, QueueSpec, Report, SubscriptionInfo, SubscriptionSpec, TopicInfo, TopicSpec,
};
use fanferro_core::publish::{Destination, OutgoingMessage, Receipt};
use fanferro_core::transport::claim_check::PointerFormat;
use fanferro_core::transport::{OffloadSettings, PayloadPointer, Transport};
use fanferro_core::{Client, ClientConfig};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pythonize::depythonize;

use crate::consumer::PyConsumerEngine;
use crate::convert::{attributes_to_py, seconds, string_map_to_py, time_to_py};
use crate::errors::{ConfigurationError, ShutdownError, ValidationError, to_py_err, to_py_err_with};
use crate::runtime::{convert, future_into_py, handle};
use crate::types::PyWireMessage;

fn config_error(what: &str, err: impl std::fmt::Display) -> PyErr {
    ConfigurationError::new_err(format!("invalid {what}: {err}"))
}

/// Builds a `dict` describing an offloaded payload pointer.
pub fn pointer_to_py<'py>(py: Python<'py>, pointer: &PayloadPointer) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("bucket", pointer.bucket.clone())?;
    dict.set_item("key", pointer.key.clone())?;
    dict.set_item("url", pointer.url.clone())?;
    dict.set_item("size", pointer.size)?;
    dict.set_item("sha256", pointer.sha256.clone())?;
    dict.set_item(
        "format",
        match pointer.format {
            PointerFormat::Fanferro => "fanferro",
            PointerFormat::AwsExtended => "aws_extended",
        },
    )?;
    Ok(dict)
}

fn receipt_to_py(py: Python<'_>, receipt: &Receipt) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("message_id", &receipt.message_id)?;
    dict.set_item("transport_message_id", &receipt.transport_message_id)?;
    dict.set_item("sequence_number", receipt.sequence_number.clone())?;
    dict.set_item("size", receipt.size)?;
    match &receipt.offloaded {
        Some(pointer) => dict.set_item("offloaded", pointer_to_py(py, pointer)?)?,
        None => dict.set_item("offloaded", py.None())?,
    }
    dict.into_py_any(py)
}

fn drift_to_py<'py>(py: Python<'py>, drift: &[Drift]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for item in drift {
        let dict = PyDict::new(py);
        dict.set_item("attribute", &item.attribute)?;
        dict.set_item("expected", &item.expected)?;
        dict.set_item("actual", item.actual.clone())?;
        dict.set_item("mutable", item.mutable)?;
        list.append(dict)?;
    }
    Ok(list)
}

fn report_to_py<T>(
    py: Python<'_>,
    report: &Report<T>,
    resource: impl Fn(Python<'_>, &T) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("kind", report.kind.as_str())?;
    dict.set_item("name", &report.name)?;
    dict.set_item("status", report.status.as_str())?;
    match &report.resource {
        Some(value) => dict.set_item("resource", resource(py, value)?)?,
        None => dict.set_item("resource", py.None())?,
    }
    dict.set_item("drift", drift_to_py(py, &report.drift)?)?;
    dict.set_item("actions", PyList::new(py, &report.actions)?)?;
    match &report.error {
        Some(error) => dict.set_item("error", to_py_err_with(py, error).into_value(py))?,
        None => dict.set_item("error", py.None())?,
    }
    match &report.dead_letter {
        Some(dead_letter) => dict.set_item("dead_letter", report_to_py(py, dead_letter, queue_info_to_py)?)?,
        None => dict.set_item("dead_letter", py.None())?,
    }
    dict.into_py_any(py)
}

fn queue_info_to_py(py: Python<'_>, queue: &QueueInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &queue.name)?;
    dict.set_item("url", &queue.url)?;
    dict.set_item("arn", &queue.arn)?;
    dict.set_item("fifo", queue.fifo)?;
    dict.set_item("attributes", string_map_to_py(py, &queue.attributes)?)?;
    dict.into_py_any(py)
}

fn topic_info_to_py(py: Python<'_>, topic: &TopicInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &topic.name)?;
    dict.set_item("arn", &topic.arn)?;
    dict.set_item("fifo", topic.fifo)?;
    dict.set_item("attributes", string_map_to_py(py, &topic.attributes)?)?;
    dict.into_py_any(py)
}

fn subscription_info_to_py(py: Python<'_>, subscription: &SubscriptionInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("arn", &subscription.arn)?;
    dict.set_item("topic_arn", &subscription.topic_arn)?;
    dict.set_item("queue_arn", &subscription.queue_arn)?;
    dict.set_item("attributes", string_map_to_py(py, &subscription.attributes)?)?;
    dict.into_py_any(py)
}

/// Converts an event into a `dict`.
pub fn event_to_py(py: Python<'_>, event: &Event) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("kind", event.kind.as_str())?;
    dict.set_item("level", event.level.as_str())?;
    dict.set_item("message", &event.message)?;
    dict.set_item("time", time_to_py(py, event.time)?)?;
    let fields = PyDict::new(py);
    for (name, value) in &event.fields {
        match value {
            FieldValue::Str(text) => fields.set_item(name, text)?,
            FieldValue::Int(number) => fields.set_item(name, number)?,
            FieldValue::Float(number) => fields.set_item(name, number)?,
            FieldValue::Bool(flag) => fields.set_item(name, flag)?,
        }
    }
    dict.set_item("fields", fields)?;
    match &event.error {
        Some(error) => {
            let info = PyDict::new(py);
            info.set_item("category", &error.category)?;
            info.set_item("message", &error.message)?;
            info.set_item("retryable", error.retryable)?;
            info.set_item("code", error.code.clone())?;
            info.set_item("request_id", error.request_id.clone())?;
            dict.set_item("error", info)?;
        }
        None => dict.set_item("error", py.None())?,
    }
    dict.into_py_any(py)
}

/// Transport settings for a destination (per-destination large payload overrides).
#[pyclass(name = "Transport", module = "fanferro._core", frozen)]
pub struct PyTransport {
    pub inner: Arc<Transport>,
}

/// Where messages go: a queue or a topic, converted once (see `CoreClient.destination`).
#[pyclass(name = "Destination", module = "fanferro._core", frozen)]
pub struct PyDestination {
    inner: Arc<Destination>,
}

#[pymethods]
impl PyDestination {
    /// The queue URL or topic ARN.
    #[getter]
    fn address(&self) -> &str {
        self.inner.address()
    }

    fn __repr__(&self) -> String {
        format!("Destination({:?})", self.inner.address())
    }
}

/// A destination object, or a destination mapping (converted on every call).
fn destination_from(destination: &Bound<'_, PyAny>) -> PyResult<Arc<Destination>> {
    if let Ok(destination) = destination.cast::<PyDestination>() {
        return Ok(destination.get().inner.clone());
    }
    depythonize(destination).map(Arc::new).map_err(|err| config_error("destination", err))
}

#[pymethods]
impl PyTransport {
    /// Whether large payload offloading is enabled.
    #[getter]
    fn offloading(&self) -> bool {
        self.inner.offload_settings().is_some()
    }
}

/// The in-memory broker (SQS/SNS/S3 semantics in-process, for tests).
#[pyclass(name = "MemoryBroker", module = "fanferro._core", frozen)]
pub struct PyMemoryBroker {
    pub inner: MemoryBroker,
    region: String,
    account_id: String,
}

#[pymethods]
impl PyMemoryBroker {
    #[new]
    #[pyo3(signature = (region="us-east-1".to_string(), account_id="000000000000".to_string()))]
    fn new(region: String, account_id: String) -> Self {
        Self { inner: MemoryBroker::new(region.clone(), account_id.clone()), region, account_id }
    }

    /// Region of the broker.
    #[getter]
    fn region(&self) -> &str {
        &self.region
    }

    /// Account id of the broker.
    #[getter]
    fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Messages currently stored in a queue (as dicts).
    fn queue_messages<'py>(&self, py: Python<'py>, queue_url: &str) -> PyResult<Bound<'py, PyList>> {
        let messages = self.inner.queue_messages(queue_url).map_err(to_py_err)?;
        let list = PyList::empty(py);
        for message in messages {
            let dict = PyDict::new(py);
            dict.set_item("message_id", message.message_id)?;
            dict.set_item("body", message.body)?;
            dict.set_item("attributes", attributes_to_py(py, &message.attributes)?)?;
            dict.set_item("receive_count", message.receive_count)?;
            dict.set_item("in_flight", message.in_flight)?;
            dict.set_item("group_id", message.group_id)?;
            list.append(dict)?;
        }
        Ok(list)
    }

    /// URLs of all queues.
    fn queue_urls(&self) -> Vec<String> {
        self.inner.queue_urls()
    }

    /// Deletes every queue, topic, subscription and bucket.
    fn reset(&self) {
        self.inner.reset();
    }
}

/// The core client.
#[pyclass(name = "CoreClient", module = "fanferro._core", frozen)]
pub struct PyCoreClient {
    client: Client,
    pid: u32,
}

impl PyCoreClient {
    fn client(&self) -> PyResult<Client> {
        if std::process::id() != self.pid {
            return Err(ShutdownError::new_err(
                "this fanferro client was created before fork(); create a new Client in the child process",
            ));
        }
        Ok(self.client.clone())
    }
}

fn message_from_kwargs(
    wire: &Bound<'_, PyWireMessage>,
    message_id: String,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    delay: Option<f64>,
    subject: Option<String>,
    trace_header: Option<String>,
) -> PyResult<OutgoingMessage> {
    Ok(OutgoingMessage {
        wire: wire.get().inner.clone(),
        message_id,
        group_id,
        deduplication_id,
        delay: seconds(delay, "delay")?,
        subject,
        trace_header,
    })
}

fn message_from_dict<'py>(item: &Bound<'py, PyAny>) -> PyResult<OutgoingMessage> {
    let py = item.py();
    let dict = item.cast::<PyDict>().map_err(|_| ValidationError::new_err("batch entries must be dicts"))?;
    // Interned keys: no string is created per lookup (this runs for every batch entry).
    let get = |key: &Bound<'py, PyString>| dict.get_item(key);
    let wire =
        get(pyo3::intern!(py, "wire"))?.ok_or_else(|| ValidationError::new_err("batch entry is missing 'wire'"))?;
    let wire = wire.cast::<PyWireMessage>().map_err(|_| ValidationError::new_err("'wire' must be a WireMessage"))?;
    let text = |key: &Bound<'py, PyString>| -> PyResult<Option<String>> {
        match get(key)? {
            Some(value) if !value.is_none() => value.extract().map(Some),
            _ => Ok(None),
        }
    };
    let delay = match get(pyo3::intern!(py, "delay"))? {
        Some(value) if !value.is_none() => Some(value.extract::<f64>()?),
        _ => None,
    };
    message_from_kwargs(
        wire,
        text(pyo3::intern!(py, "message_id"))?
            .ok_or_else(|| ValidationError::new_err("batch entry is missing 'message_id'"))?,
        text(pyo3::intern!(py, "group_id"))?,
        text(pyo3::intern!(py, "deduplication_id"))?,
        delay,
        text(pyo3::intern!(py, "subject"))?,
        text(pyo3::intern!(py, "trace_header"))?,
    )
}

#[pymethods]
impl PyCoreClient {
    /// Connects to AWS or an AWS-compatible endpoint. Returns an awaitable `CoreClient`.
    #[staticmethod]
    fn connect<'py>(
        py: Python<'py>,
        aws_config: &Bound<'py, PyAny>,
        client_config: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let aws: AwsConfig = depythonize(aws_config).map_err(|err| config_error("AWS configuration", err))?;
        let config: ClientConfig =
            depythonize(client_config).map_err(|err| config_error("client configuration", err))?;
        future_into_py(py, async move {
            Client::connect(&aws, config).await.map(|client| PyCoreClient { client, pid: std::process::id() })
        })
    }

    /// Creates a client backed by an in-memory broker.
    #[staticmethod]
    fn memory(broker: &Bound<'_, PyMemoryBroker>, client_config: &Bound<'_, PyAny>) -> PyResult<Self> {
        let config: ClientConfig =
            depythonize(client_config).map_err(|err| config_error("client configuration", err))?;
        let client = Client::with_backend(broker.get().inner.backend(), config).map_err(to_py_err)?;
        Ok(Self { client, pid: std::process::id() })
    }

    /// Region of the backend.
    #[getter]
    fn region(&self) -> String {
        self.client.region().to_string()
    }

    /// Custom endpoint, if any.
    #[getter]
    fn endpoint_url(&self) -> Option<String> {
        self.client.backend().endpoint_url.clone()
    }

    /// `"aws"` or `"memory"`.
    #[getter]
    fn backend(&self) -> &'static str {
        match self.client.backend().kind {
            BackendKind::Aws => "aws",
            BackendKind::Memory => "memory",
        }
    }

    /// Account id of the caller (awaitable).
    fn account_id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.account_id().await })
    }

    /// Builds a transport with destination-specific offload settings (`None` disables).
    fn transport(&self, offload: &Bound<'_, PyAny>) -> PyResult<PyTransport> {
        let client = self.client()?;
        let offload: Option<OffloadSettings> = if offload.is_none() {
            None
        } else {
            Some(depythonize(offload).map_err(|err| config_error("large payload settings", err))?)
        };
        let inner = client.transport().with_offload(offload).map_err(to_py_err)?;
        Ok(PyTransport { inner: Arc::new(inner) })
    }

    /// Converts a destination mapping once, for calls that publish to it again and again.
    fn destination(&self, destination: &Bound<'_, PyAny>) -> PyResult<PyDestination> {
        Ok(PyDestination { inner: destination_from(destination)? })
    }

    /// Sends/publishes one message (awaitable receipt dict).
    #[pyo3(signature = (destination, wire, message_id, *, group_id=None, deduplication_id=None, delay=None, subject=None, trace_header=None, transport=None))]
    #[allow(clippy::too_many_arguments)]
    fn publish<'py>(
        &self,
        py: Python<'py>,
        destination: &Bound<'py, PyAny>,
        wire: &Bound<'py, PyWireMessage>,
        message_id: String,
        group_id: Option<String>,
        deduplication_id: Option<String>,
        delay: Option<f64>,
        subject: Option<String>,
        trace_header: Option<String>,
        transport: Option<&Bound<'py, PyTransport>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let destination = destination_from(destination)?;
        let message = message_from_kwargs(wire, message_id, group_id, deduplication_id, delay, subject, trace_header)?;
        let transport = transport.map(|transport| transport.get().inner.clone());
        future_into_py(py, async move {
            let receipt = match &transport {
                Some(transport) => client.publish_with(transport, &destination, message).await,
                None => client.publish(&destination, message).await,
            };
            convert(receipt, |py, receipt| receipt_to_py(py, &receipt))
        })
    }

    /// Sends/publishes many messages; resolves to a list of receipt dicts or exceptions.
    #[pyo3(signature = (destination, messages, *, transport=None))]
    fn publish_batch<'py>(
        &self,
        py: Python<'py>,
        destination: &Bound<'py, PyAny>,
        messages: &Bound<'py, PyAny>,
        transport: Option<&Bound<'py, PyTransport>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let destination = destination_from(destination)?;
        let messages = messages.try_iter()?.map(|item| message_from_dict(&item?)).collect::<PyResult<Vec<_>>>()?;
        let transport = transport.map(|transport| transport.get().inner.clone());
        future_into_py(py, async move {
            let results = match &transport {
                Some(transport) => client.publish_batch_with(transport, &destination, messages).await,
                None => client.publish_batch(&destination, messages).await,
            };
            convert(results, |py, results| {
                let list = PyList::empty(py);
                for result in &results {
                    match result {
                        Ok(receipt) => list.append(receipt_to_py(py, receipt)?)?,
                        Err(error) => list.append(to_py_err_with(py, error).into_value(py))?,
                    }
                }
                list.into_py_any(py)
            })
        })
    }

    /// Provisions a queue (awaitable report dict).
    fn ensure_queue<'py>(&self, py: Python<'py>, spec: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let spec: QueueSpec = depythonize(spec).map_err(|err| config_error("queue spec", err))?;
        future_into_py(py, async move {
            let report = client.ensure_queue(&spec).await;
            convert(report, |py, report| report_to_py(py, &report, queue_info_to_py))
        })
    }

    /// Provisions a topic (awaitable report dict).
    fn ensure_topic<'py>(&self, py: Python<'py>, spec: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let spec: TopicSpec = depythonize(spec).map_err(|err| config_error("topic spec", err))?;
        future_into_py(py, async move {
            let report = client.ensure_topic(&spec).await;
            convert(report, |py, report| report_to_py(py, &report, topic_info_to_py))
        })
    }

    /// Provisions a subscription (awaitable report dict).
    fn ensure_subscription<'py>(&self, py: Python<'py>, spec: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let spec: SubscriptionSpec = depythonize(spec).map_err(|err| config_error("subscription spec", err))?;
        future_into_py(py, async move {
            let report = client.ensure_subscription(&spec).await;
            convert(report, |py, report| report_to_py(py, &report, subscription_info_to_py))
        })
    }

    /// Ensures a bucket exists (awaitable report dict).
    fn ensure_bucket<'py>(&self, py: Python<'py>, bucket: String, create: bool) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move {
            let report = client.ensure_bucket(&bucket, create).await;
            convert(report, |py, report| report_to_py(py, &report, |py, name| name.into_py_any(py)))
        })
    }

    /// Deletes a queue.
    fn delete_queue<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.delete_queue(&url).await })
    }

    /// Deletes a topic.
    fn delete_topic<'py>(&self, py: Python<'py>, arn: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.delete_topic(&arn).await })
    }

    /// Removes a subscription.
    fn unsubscribe<'py>(&self, py: Python<'py>, arn: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.unsubscribe(&arn).await })
    }

    /// Deletes all messages of a queue.
    fn purge_queue<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.purge_queue(&url).await })
    }

    /// Reads queue attributes (awaitable dict).
    fn queue_attributes<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.queue_attributes(&url).await })
    }

    /// Reads topic attributes (awaitable dict).
    fn topic_attributes<'py>(&self, py: Python<'py>, arn: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.topic_attributes(&arn).await })
    }

    /// Deletes an offloaded payload object.
    fn delete_payload<'py>(&self, py: Python<'py>, bucket: String, key: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        future_into_py(py, async move { client.backend().objects.delete_object(&bucket, &key).await })
    }

    /// Creates a consumer engine for a queue (call `start()` on it). With `envelopes` (built-in
    /// envelopes only), the engine unwraps received messages while decoding them (see
    /// `Delivery.parts`).
    #[pyo3(signature = (config, envelopes=None))]
    fn consumer(
        &self,
        config: &Bound<'_, PyAny>,
        envelopes: Option<Vec<Bound<'_, PyAny>>>,
    ) -> PyResult<PyConsumerEngine> {
        let client = self.client()?;
        let config: ConsumerConfig = depythonize(config).map_err(|err| config_error("consumer configuration", err))?;
        let envelopes = envelopes
            .map(|envelopes| {
                envelopes
                    .iter()
                    .map(|envelope| {
                        crate::envelopes::builtin(envelope).ok_or_else(|| {
                            ValidationError::new_err("the engine only unwraps built-in envelopes (CloudEvents, Raw...)")
                        })
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?
            .map(EnvelopeSet::new);
        let _guard = handle().enter();
        let engine = ConsumerEngine::with_envelopes(&client, config, envelopes).map_err(to_py_err)?;
        Ok(PyConsumerEngine::new(engine, self.pid))
    }

    /// Enables exactly the given event kinds (by dotted name) for emission from Rust.
    fn set_event_filter(&self, kinds: Vec<String>) -> PyResult<()> {
        let parsed = kinds
            .iter()
            .map(|name| {
                EventKind::parse(name).ok_or_else(|| ValidationError::new_err(format!("unknown event kind {name:?}")))
            })
            .collect::<PyResult<Vec<_>>>()?;
        self.client.events().set_enabled(&parsed);
        Ok(())
    }

    /// Waits up to `timeout` seconds for events; resolves to a list of event dicts.
    #[pyo3(signature = (max=256, timeout=1.0))]
    fn next_events<'py>(&self, py: Python<'py>, max: usize, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client()?;
        let timeout = seconds(Some(timeout), "timeout")?.unwrap_or(Duration::from_secs(1));
        future_into_py(py, async move {
            let events = client.events().next_batch(max, timeout).await;
            convert(Ok(events), |py, events| {
                let list = PyList::empty(py);
                for event in &events {
                    list.append(event_to_py(py, event)?)?;
                }
                list.into_py_any(py)
            })
        })
    }

    /// Returns buffered events without waiting.
    #[pyo3(signature = (max=1024))]
    fn drain_events<'py>(&self, py: Python<'py>, max: usize) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for event in self.client.events().drain(max) {
            list.append(event_to_py(py, &event)?)?;
        }
        Ok(list)
    }

    /// Makes a pending `next_events` call (or the next one) return right away.
    fn wake_events(&self) {
        self.client.events().wake();
    }

    /// Number of events dropped because the buffer was full since the last call.
    fn take_dropped_events(&self) -> u64 {
        self.client.events().take_dropped()
    }

    /// Client metrics snapshot.
    fn metrics(&self) -> std::collections::BTreeMap<&'static str, i64> {
        self.client.metrics().snapshot()
    }

    fn __repr__(&self) -> String {
        format!("CoreClient(backend={:?}, region={:?})", self.backend(), self.client.region())
    }
}
