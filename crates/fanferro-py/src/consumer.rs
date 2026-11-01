//! `ConsumerEngine` and `Delivery` bindings.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use fanferro_core::consumer::records::SqsEventRecord;
use fanferro_core::consumer::{ConsumerEngine, Delivery, Unwrapped};
use fanferro_core::types::reserved;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::client::pointer_to_py;
use crate::convert::{Repr, attributes_to_py, seconds, time_to_py};
use crate::errors::{EnvelopeError, ShutdownError, ValidationError, to_py_err, to_py_err_with};
use crate::runtime::{Convert, future_into_py, handle, pending_future};
use crate::types::{PyAttributeValue, PyPayload, wire_to_py};

/// Receives messages from one queue.
#[pyclass(name = "ConsumerEngine", module = "fanferro._core", frozen)]
pub struct PyConsumerEngine {
    engine: ConsumerEngine,
    pid: u32,
}

impl PyConsumerEngine {
    pub fn new(engine: ConsumerEngine, pid: u32) -> Self {
        Self { engine, pid }
    }

    fn engine(&self) -> PyResult<ConsumerEngine> {
        if std::process::id() != self.pid {
            return Err(ShutdownError::new_err(
                "this consumer was created before fork(); create a new one in the child process",
            ));
        }
        Ok(self.engine.clone())
    }
}

fn deliveries_to_py(py: Python<'_>, deliveries: Vec<Delivery>) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for delivery in deliveries {
        list.append(Py::new(py, PyDelivery { inner: Arc::new(delivery) })?)?;
    }
    list.into_py_any(py)
}

#[pymethods]
impl PyConsumerEngine {
    /// Starts receiving.
    fn start(&self) -> PyResult<()> {
        let engine = self.engine()?;
        let _guard = handle().enter();
        engine.start().map_err(to_py_err)
    }

    /// Waits up to `timeout` seconds for deliveries; resolves to a list of `Delivery`.
    #[pyo3(signature = (max, timeout=1.0))]
    fn next_batch<'py>(&self, py: Python<'py>, max: usize, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        let timeout = seconds(Some(timeout), "timeout")?.unwrap_or(Duration::from_secs(1));
        future_into_py(py, async move {
            let deliveries = engine.next_batch(max, timeout).await;
            Convert(move |py: Python<'_>| deliveries_to_py(py, deliveries))
        })
    }

    /// Leases and decodes the records of an SQS event an AWS Lambda event source mapping
    /// received (`event["Records"]`); resolves to their deliveries, in order (engines configured
    /// with `external` only).
    fn accept_records<'py>(&self, py: Python<'py>, records: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        let records: Vec<SqsEventRecord> = pythonize::depythonize(records)
            .map_err(|err| ValidationError::new_err(format!("invalid SQS event records: {err}")))?;
        let messages = records.into_iter().map(SqsEventRecord::into_received).collect::<Result<Vec<_>, _>>();
        let messages = messages.map_err(|error| to_py_err_with(py, &error))?;
        future_into_py(py, async move {
            let deliveries = engine.accept_records(messages).await;
            Convert(move |py: Python<'_>| match deliveries {
                Ok(deliveries) => deliveries_to_py(py, deliveries),
                Err(error) => Err(to_py_err_with(py, &error)),
            })
        })
    }

    /// Takes the receipt handles of the accepted records settled with a delete since the last
    /// call (acked, dead-lettered, requeued).
    fn take_deleted(&self) -> Vec<String> {
        self.engine.take_deleted()
    }

    /// Waits until settlements running in the background completed (awaitable).
    fn wait_settlements<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        future_into_py(py, async move {
            engine.wait_settlements().await;
            fanferro_core::Result::Ok(())
        })
    }

    /// Returns buffered deliveries without waiting.
    fn drain_buffered<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        deliveries_to_py(py, engine.drain_buffered()).map(|list| list.into_bound(py))
    }

    /// Pauses receiving.
    fn pause(&self) {
        self.engine.pause();
    }

    /// Resumes receiving.
    fn resume(&self) {
        self.engine.resume();
    }

    /// Whether receiving is paused.
    #[getter]
    fn paused(&self) -> bool {
        self.engine.is_paused()
    }

    /// `"created"`, `"running"`, `"stopping"` or `"stopped"`.
    #[getter]
    fn state(&self) -> &'static str {
        self.engine.state().as_str()
    }

    /// The queue URL.
    #[getter]
    fn queue_url(&self) -> String {
        self.engine.queue_url().to_string()
    }

    /// Stops receiving (awaitable). `abort=True` also aborts a long poll in flight.
    #[pyo3(signature = (abort=false))]
    fn stop_receiving<'py>(&self, py: Python<'py>, abort: bool) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        future_into_py(py, async move {
            engine.stop_receiving(abort).await;
            fanferro_core::Result::Ok(())
        })
    }

    /// From now on, deliveries dropped without a settlement are left to time out instead of
    /// released.
    fn leave_unsettled(&self) {
        self.engine.leave_unsettled();
    }

    /// Closes the engine (awaitable); releases unsettled messages when requested.
    fn close<'py>(&self, py: Python<'py>, release_unsettled: bool) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine()?;
        future_into_py(py, async move {
            engine.close(release_unsettled).await;
            fanferro_core::Result::Ok(())
        })
    }

    /// Statistics snapshot.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = self.engine.stats();
        let dict = PyDict::new(py);
        dict.set_item("state", stats.state.as_str())?;
        dict.set_item("paused", stats.paused)?;
        dict.set_item("leases", stats.leases)?;
        dict.set_item("available_permits", stats.available_permits)?;
        dict.set_item("metrics", stats.metrics)?;
        Ok(dict)
    }

    /// How receiving goes: `last_receive_at`, `receive_gap` (seconds), `consecutive_failures`,
    /// `last_error` (the exception) and `last_error_at`.
    fn receive_health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let health = self.engine.receive_health();
        let dict = PyDict::new(py);
        dict.set_item("last_receive_at", health.last_receive_at.map(|at| time_to_py(py, at)).transpose()?)?;
        dict.set_item("receive_gap", health.receive_gap.as_secs_f64())?;
        dict.set_item("consecutive_failures", health.consecutive_failures)?;
        match health.last_error {
            Some(failure) => {
                dict.set_item("last_error", to_py_err_with(py, &failure.error).into_value(py))?;
                dict.set_item("last_error_at", time_to_py(py, failure.at)?)?;
            }
            None => {
                dict.set_item("last_error", py.None())?;
                dict.set_item("last_error_at", py.None())?;
            }
        }
        Ok(dict)
    }
}

/// A received message and its lease.
#[pyclass(name = "Delivery", module = "fanferro._core", frozen)]
pub struct PyDelivery {
    inner: Arc<Delivery>,
}

impl PyDelivery {
    /// Starts a settlement in the background (tracked by the engine) and returns an asyncio
    /// future that is resolved with its outcome, batched with other completed settlements.
    fn settle<'py, Settle, Fut>(&self, py: Python<'py>, settle: Settle) -> PyResult<Bound<'py, PyAny>>
    where
        Settle: FnOnce(Arc<Delivery>) -> Fut + Send + 'static,
        Fut: Future<Output = fanferro_core::Result<()>> + Send + 'static,
    {
        let (future, pending) = pending_future(py)?;
        self.inner.spawn_settlement(settle, move |result| pending.complete(result));
        Ok(future)
    }
}

#[pymethods]
impl PyDelivery {
    /// SQS message id.
    #[getter]
    fn sqs_message_id(&self) -> &str {
        &self.inner.message().message_id
    }

    /// Receipt handle of this receive.
    #[getter]
    fn receipt_handle(&self) -> &str {
        &self.inner.message().receipt_handle
    }

    /// The SQS body exactly as received.
    #[getter]
    fn raw_body(&self) -> &str {
        &self.inner.message().body
    }

    /// The SQS message attributes exactly as received.
    #[getter]
    fn raw_attributes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        attributes_to_py(py, &self.inner.message().attributes)
    }

    /// `ApproximateReceiveCount`.
    #[getter]
    fn receive_count(&self) -> u32 {
        self.inner.message().system.approximate_receive_count
    }

    /// When the message was sent.
    #[getter]
    fn sent_at<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.inner.message().system.sent_timestamp.map(|t| time_to_py(py, t)).transpose()
    }

    /// When the message was first received.
    #[getter]
    fn first_received_at<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.inner.message().system.approximate_first_receive_timestamp.map(|t| time_to_py(py, t)).transpose()
    }

    /// When this delivery was received by the consumer.
    #[getter]
    fn received_at<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        time_to_py(py, self.inner.received_at)
    }

    /// FIFO message group id.
    #[getter]
    fn group_id(&self) -> Option<&str> {
        self.inner.message().system.message_group_id.as_deref()
    }

    /// FIFO deduplication id.
    #[getter]
    fn deduplication_id(&self) -> Option<&str> {
        self.inner.message().system.message_deduplication_id.as_deref()
    }

    /// FIFO sequence number.
    #[getter]
    fn sequence_number(&self) -> Option<&str> {
        self.inner.message().system.sequence_number.as_deref()
    }

    /// Sender principal.
    #[getter]
    fn sender_id(&self) -> Option<&str> {
        self.inner.message().system.sender_id.as_deref()
    }

    /// X-Ray trace header (`AWSTraceHeader`).
    #[getter]
    fn aws_trace_header(&self) -> Option<&str> {
        self.inner.message().system.aws_trace_header.as_deref()
    }

    /// Source queue ARN when SQS moved the message to a dead-letter queue.
    #[getter]
    fn dead_letter_source_arn(&self) -> Option<&str> {
        self.inner.message().system.dead_letter_queue_source_arn.as_deref()
    }

    /// SNS metadata when the message arrived as an SNS notification.
    #[getter]
    fn sns<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(info) = &self.inner.sns else { return Ok(None) };
        let dict = PyDict::new(py);
        dict.set_item("message_id", &info.message_id)?;
        dict.set_item("topic_arn", &info.topic_arn)?;
        dict.set_item("subject", info.subject.clone())?;
        dict.set_item("timestamp", info.timestamp.map(|t| time_to_py(py, t)).transpose()?)?;
        dict.set_item("sequence_number", info.sequence_number.clone())?;
        Ok(Some(dict))
    }

    /// The decoded wire message (`None` if decoding failed).
    #[getter]
    fn wire(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.inner.decoded {
            Ok(decoded) => wire_to_py(py, decoded.wire.clone()).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// What a handler needs: `(payload, envelope, attributes, visible attributes)`. `payload` and
    /// `envelope` (the position of the envelope that recognised the message) are `None` unless the
    /// engine unwraps envelopes; the visible attributes leave out fanferro's internal ones.
    ///
    /// Raises the decoding error, or the error of the envelope that recognised the message.
    fn parts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let decoded = self.inner.decoded.as_ref().map_err(|error| to_py_err_with(py, error))?;
        let (payload, envelope) = match &self.inner.unwrapped {
            Unwrapped::Payload { payload, envelope } => (
                Py::new(py, PyPayload { inner: payload.clone() })?.into_any(),
                envelope.into_pyobject(py)?.into_any().unbind(),
            ),
            Unwrapped::NotAttempted => (py.None(), py.None()),
            Unwrapped::Failed(error) => return Err(to_py_err_with(py, error)),
            Unwrapped::Unrecognised { tried } => {
                // Worded (and raised) as when Python unwraps the message.
                return Err(EnvelopeError::new_err(format!(
                    "no configured envelope recognised the message (tried {})",
                    if tried.is_empty() { "none" } else { tried }
                )));
            }
        };
        let attributes = PyDict::new(py);
        let visible = PyDict::new(py);
        for (name, value) in &decoded.wire.attributes {
            let value = Py::new(py, PyAttributeValue { inner: value.clone() })?;
            if !reserved::is_internal(name) {
                visible.set_item(name, &value)?;
            }
            attributes.set_item(name, value)?;
        }
        PyTuple::new(py, [payload, envelope, attributes.into_any().unbind(), visible.into_any().unbind()])
    }

    /// The decoding error, if decoding failed.
    #[getter]
    fn decode_error(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        match &self.inner.decoded {
            Ok(_) => None,
            Err(error) => Some(to_py_err_with(py, error).into_value(py).into_any()),
        }
    }

    /// The claim-check pointer when the payload was offloaded.
    #[getter]
    fn pointer<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self.inner.decoded {
            Ok(decoded) => decoded.pointer.as_ref().map(|pointer| pointer_to_py(py, pointer)).transpose(),
            Err(_) => Ok(None),
        }
    }

    /// Lease state: buffered, active, settling, lost, expired or settled.
    #[getter]
    fn lease_status(&self) -> &'static str {
        self.inner.lease.status().as_str()
    }

    /// Whether settlement has happened (or started).
    #[getter]
    fn settled(&self) -> bool {
        self.inner.lease.is_settled()
    }

    /// Seconds until the message becomes visible to other consumers.
    #[getter]
    fn remaining_visibility(&self) -> f64 {
        self.inner.lease.remaining_visibility().as_secs_f64()
    }

    /// Marks the message as being processed.
    fn mark_active(&self) {
        self.inner.lease.mark_active();
    }

    /// Notes that a handler (or a FIFO message group) waits for the next settlement;
    /// `waiting_handlers`: how many settlements are awaited when everything that could settle
    /// waits (the batch is then sent once it holds all of them).
    #[pyo3(signature = (waiting_handlers=None))]
    fn mark_awaited(&self, waiting_handlers: Option<usize>) {
        self.inner.lease.mark_awaited(waiting_handlers);
    }

    /// Stops protecting the message without settling it: heartbeats stop and it becomes visible
    /// again when its visibility timeout runs out (it can still be settled until then).
    fn leave(&self) {
        self.inner.lease.leave();
    }

    /// Deletes the message in the background (not awaitable; the engine tracks the delete and
    /// reports failures as events and metrics).
    fn ack_in_background(&self) -> PyResult<()> {
        self.inner.ack_in_background().map_err(to_py_err)
    }

    /// Deletes the message (awaitable).
    fn ack<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (future, pending) = pending_future(py)?;
        self.inner.spawn_ack(move |result| pending.complete(result));
        Ok(future)
    }

    /// Makes the message visible again after `delay` seconds (awaitable).
    fn nack<'py>(&self, py: Python<'py>, delay: f64) -> PyResult<Bound<'py, PyAny>> {
        let delay = seconds(Some(delay), "delay")?.unwrap_or_default();
        self.settle(py, move |delivery| async move { delivery.lease.nack(delay).await })
    }

    /// Makes the message visible again immediately (awaitable).
    fn release<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.settle(py, |delivery| async move { delivery.lease.release().await })
    }

    /// Sets the visibility timeout to `timeout` seconds from now (awaitable).
    fn extend<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = seconds(Some(timeout), "timeout")?.unwrap_or_default();
        self.settle(py, move |delivery| async move { delivery.lease.extend(timeout).await })
    }

    /// Forwards the message to the dead-letter queue, then deletes it (awaitable).
    #[pyo3(signature = (reason, details=None))]
    fn dead_letter<'py>(
        &self,
        py: Python<'py>,
        reason: String,
        details: Option<BTreeMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let details = details.unwrap_or_default();
        self.settle(py, move |delivery| async move { delivery.lease.dead_letter(&reason, &details).await })
    }

    /// Re-enqueues a copy with `fanferro.attempt=attempt`, then deletes the original (awaitable).
    #[pyo3(signature = (attempt, delay=None))]
    fn requeue<'py>(&self, py: Python<'py>, attempt: u32, delay: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let delay = seconds(delay, "delay")?;
        self.settle(py, move |delivery| async move { delivery.lease.requeue(delay, attempt).await })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let message = self.inner.message();
        Ok(Repr::new(py, "Delivery")
            .field("sqs_message_id", message.message_id.as_str())?
            .field("receive_count", message.system.approximate_receive_count)?
            .field("lease_status", self.inner.lease.status().as_str())?
            .finish())
    }
}
