//! Built-in envelopes exposed to Python. They implement the same protocol as custom
//! Python envelopes: `name`, `wrap(payload) -> WireMessage`, `unwrap(wire) -> Payload | None`.
//! Envelopes with the same settings are equal (declaring a resource again compares them).

use std::sync::Arc;

use fanferro_core::envelope::{CloudEventsJson, CloudEventsProtobuf, Envelope, IMPLICIT_CONTENT_TYPE, RawEnvelope};
use pyo3::prelude::*;

use crate::errors::to_py_err;
use crate::types::{PyPayload, PyWireMessage};

fn wrap(envelope: &dyn Envelope, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
    envelope.wrap(&payload.get().inner).map(|inner| PyWireMessage { inner }).map_err(to_py_err)
}

fn unwrap(envelope: &dyn Envelope, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
    envelope.unwrap(&wire.get().inner).map(|payload| payload.map(|inner| PyPayload { inner })).map_err(to_py_err)
}

/// The Rust envelope behind a built-in envelope object (`None` for envelopes implemented in Python).
pub fn builtin(envelope: &Bound<'_, PyAny>) -> Option<Arc<dyn Envelope>> {
    if let Ok(envelope) = envelope.cast::<PyCloudEvents>() {
        return Some(Arc::new(envelope.get().inner.clone()));
    }
    if let Ok(envelope) = envelope.cast::<PyCloudEventsProtobuf>() {
        return Some(Arc::new(envelope.get().inner.clone()));
    }
    if let Ok(envelope) = envelope.cast::<PyRaw>() {
        return Some(Arc::new(envelope.get().inner.clone()));
    }
    None
}

/// CloudEvents 1.0 structured JSON envelope.
#[pyclass(name = "CloudEventsEnvelope", module = "fanferro._core", frozen, eq, hash)]
#[derive(PartialEq, Eq, Hash)]
pub struct PyCloudEvents {
    inner: CloudEventsJson,
}

#[pymethods]
impl PyCloudEvents {
    #[new]
    #[pyo3(signature = (*, content_type_attribute=true, type_attribute=Some("fanferro.type".to_string()), default_source="fanferro".to_string()))]
    fn new(content_type_attribute: bool, type_attribute: Option<String>, default_source: String) -> Self {
        Self { inner: CloudEventsJson { content_type_attribute, type_attribute, default_source } }
    }

    /// `"cloudevents"`.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Whether the `content-type` attribute is written.
    #[getter]
    fn content_type_attribute(&self) -> bool {
        self.inner.content_type_attribute
    }

    /// Attribute the message type is promoted to (`None` disables it).
    #[getter]
    fn type_attribute(&self) -> Option<&str> {
        self.inner.type_attribute.as_deref()
    }

    /// Source used when a payload has none.
    #[getter]
    fn default_source(&self) -> &str {
        &self.inner.default_source
    }

    /// Wraps a payload.
    fn wrap(&self, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
        wrap(&self.inner, payload)
    }

    /// Unwraps a wire message if it is a CloudEvent.
    fn unwrap(&self, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
        unwrap(&self.inner, wire)
    }

    fn __repr__(&self) -> String {
        format!(
            "CloudEventsEnvelope(content_type_attribute={}, type_attribute={:?}, default_source={:?})",
            self.inner.content_type_attribute, self.inner.type_attribute, self.inner.default_source
        )
    }
}

/// CloudEvents 1.0 protobuf (binary) envelope.
#[pyclass(name = "CloudEventsProtobufEnvelope", module = "fanferro._core", frozen, eq, hash)]
#[derive(PartialEq, Eq, Hash)]
pub struct PyCloudEventsProtobuf {
    inner: CloudEventsProtobuf,
}

#[pymethods]
impl PyCloudEventsProtobuf {
    #[new]
    #[pyo3(signature = (*, type_attribute=Some("fanferro.type".to_string()), default_source="fanferro".to_string()))]
    fn new(type_attribute: Option<String>, default_source: String) -> Self {
        Self { inner: CloudEventsProtobuf { type_attribute, default_source } }
    }

    /// `"cloudevents-protobuf"`.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Attribute the message type is promoted to (`None` disables it).
    #[getter]
    fn type_attribute(&self) -> Option<&str> {
        self.inner.type_attribute.as_deref()
    }

    /// Source used when a payload has none.
    #[getter]
    fn default_source(&self) -> &str {
        &self.inner.default_source
    }

    /// Wraps a payload.
    fn wrap(&self, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
        wrap(&self.inner, payload)
    }

    /// Unwraps a wire message if it is a protobuf CloudEvent.
    fn unwrap(&self, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
        unwrap(&self.inner, wire)
    }

    fn __repr__(&self) -> String {
        format!(
            "CloudEventsProtobufEnvelope(type_attribute={:?}, default_source={:?})",
            self.inner.type_attribute, self.inner.default_source
        )
    }
}

/// Raw envelope (the default): the payload is the body; metadata travels in attributes.
#[pyclass(name = "RawEnvelope", module = "fanferro._core", frozen, eq, hash)]
#[derive(PartialEq, Eq, Hash)]
pub struct PyRaw {
    inner: RawEnvelope,
}

#[pymethods]
impl PyRaw {
    #[new]
    #[pyo3(signature = (
        *,
        content_type_attribute=Some("content-type".to_string()),
        type_attribute=Some("fanferro.type".to_string()),
        id_attribute=Some("fanferro.id".to_string()),
        implicit_content_type=Some(IMPLICIT_CONTENT_TYPE.to_string()),
    ))]
    fn new(
        content_type_attribute: Option<String>,
        type_attribute: Option<String>,
        id_attribute: Option<String>,
        implicit_content_type: Option<String>,
    ) -> Self {
        Self { inner: RawEnvelope { content_type_attribute, type_attribute, id_attribute, implicit_content_type } }
    }

    /// `"raw"`.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Attribute carrying the content type.
    #[getter]
    fn content_type_attribute(&self) -> Option<&str> {
        self.inner.content_type_attribute.as_deref()
    }

    /// Attribute carrying the message type.
    #[getter]
    fn type_attribute(&self) -> Option<&str> {
        self.inner.type_attribute.as_deref()
    }

    /// Attribute carrying the message id.
    #[getter]
    fn id_attribute(&self) -> Option<&str> {
        self.inner.id_attribute.as_deref()
    }

    /// Content type sent without the content-type attribute (`None`: always written).
    #[getter]
    fn implicit_content_type(&self) -> Option<&str> {
        self.inner.implicit_content_type.as_deref()
    }

    /// Wraps a payload.
    fn wrap(&self, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
        wrap(&self.inner, payload)
    }

    /// Unwraps any wire message.
    fn unwrap(&self, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
        unwrap(&self.inner, wire)
    }

    fn __repr__(&self) -> String {
        format!(
            "RawEnvelope(content_type_attribute={:?}, type_attribute={:?}, id_attribute={:?}, implicit_content_type={:?})",
            self.inner.content_type_attribute,
            self.inner.type_attribute,
            self.inner.id_attribute,
            self.inner.implicit_content_type
        )
    }
}
