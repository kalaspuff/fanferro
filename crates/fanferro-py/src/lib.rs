//! `fanferro._core`: the native module behind the `fanferro` Python package.
//!
//! This crate is a thin binding layer over `fanferro-core`. Keep logic in the core crate
//! and keep this crate to conversions, async bridging and Python ergonomics. Every public
//! item must be reflected in `python/fanferro/_core.pyi` (CI checks the names).

mod client;
mod consumer;
mod convert;
mod envelopes;
mod errors;
mod runtime;
mod types;

use pyo3::prelude::*;

/// Returns a new unique, time-ordered message id (UUIDv7).
#[pyfunction]
fn new_message_id() -> String {
    fanferro_core::types::new_message_id()
}

/// Converts a W3C `traceparent` into an AWS X-Ray trace header.
#[pyfunction]
fn xray_from_traceparent(traceparent: &str) -> Option<String> {
    fanferro_core::trace::xray_from_traceparent(traceparent)
}

/// Converts an AWS X-Ray trace header into a W3C `traceparent`.
#[pyfunction]
fn traceparent_from_xray(header: &str) -> Option<String> {
    fanferro_core::trace::traceparent_from_xray(header)
}

/// Whether `text` may be sent as an SQS/SNS body without base64 encoding.
#[pyfunction]
fn is_sqs_safe_text(text: &str) -> bool {
    fanferro_core::types::text::is_sqs_safe_text(text)
}

/// Checks the attributes of a wire message as a publish does before sending it inline (raises
/// `ValidationError`).
#[pyfunction]
fn validate_wire(wire: &types::PyWireMessage) -> PyResult<()> {
    fanferro_core::transport::validate_wire(&wire.inner).map_err(errors::to_py_err)
}

/// All event kinds as `(name, default level, description)` tuples.
#[pyfunction]
fn event_kinds() -> Vec<(&'static str, &'static str, &'static str)> {
    fanferro_core::events::EventKind::ALL
        .iter()
        .map(|k| (k.as_str(), k.default_level().as_str(), k.description()))
        .collect()
}

/// Names of message attributes that fanferro reads or writes.
#[pyfunction]
fn reserved_attributes() -> Vec<&'static str> {
    fanferro_core::types::reserved::ALL.to_vec()
}

/// Forgets the runtime inherited from a parent process (registered with `os.register_at_fork`).
#[pyfunction]
fn _reset_after_fork() {
    runtime::reset_after_fork();
}

/// Stops the Tokio runtime before the interpreter finalizes (registered with `atexit`).
///
/// Afterwards every awaitable raises `ShutdownError`. Only for interpreter exit: there is
/// no way to restart the runtime in the same process.
#[pyfunction]
fn _shutdown(py: Python<'_>) {
    runtime::shutdown(py);
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    errors::register(m)?;
    m.add_class::<types::PyAttributeValue>()?;
    m.add_class::<types::PyPayload>()?;
    m.add_class::<types::PyWireMessage>()?;
    m.add_class::<envelopes::PyCloudEvents>()?;
    m.add_class::<envelopes::PyCloudEventsProtobuf>()?;
    m.add_class::<envelopes::PyRaw>()?;
    m.add_class::<envelopes::PyTomodachiJson>()?;
    m.add_class::<envelopes::PyTomodachiProtobuf>()?;
    m.add_class::<client::PyCoreClient>()?;
    m.add_class::<client::PyTransport>()?;
    m.add_class::<client::PyDestination>()?;
    m.add_class::<client::PyMemoryBroker>()?;
    m.add_class::<consumer::PyConsumerEngine>()?;
    m.add_class::<consumer::PyDelivery>()?;
    m.add_function(wrap_pyfunction!(new_message_id, m)?)?;
    m.add_function(wrap_pyfunction!(xray_from_traceparent, m)?)?;
    m.add_function(wrap_pyfunction!(traceparent_from_xray, m)?)?;
    m.add_function(wrap_pyfunction!(is_sqs_safe_text, m)?)?;
    m.add_function(wrap_pyfunction!(validate_wire, m)?)?;
    m.add_function(wrap_pyfunction!(event_kinds, m)?)?;
    m.add_function(wrap_pyfunction!(reserved_attributes, m)?)?;
    m.add_function(wrap_pyfunction!(_reset_after_fork, m)?)?;
    let shutdown = wrap_pyfunction!(_shutdown, m)?;
    m.add_function(shutdown.clone())?;
    // Runs before the interpreter finalizes (and after atexit hooks registered later, which
    // may still use fanferro). See runtime.rs.
    m.py().import("atexit")?.call_method1("register", (shutdown,))?;
    Ok(())
}
