//! Built-in envelopes exposed to Python. They implement the same protocol as custom
//! Python envelopes: `name`, `wrap(payload) -> WireMessage`, `unwrap(wire) -> Payload | None`.
//! Envelopes with the same settings are equal (declaring a resource again compares them).

use std::sync::Arc;

use fanferro_core::envelope::{
    AwsNotifications, CloudEventsJson, CloudEventsProtobuf, Envelope, IMPLICIT_CONTENT_TYPE, RawEnvelope,
    TOMODACHI_COMPRESS_THRESHOLD, TomodachiDestination, TomodachiJson, TomodachiProtobuf,
};
use pyo3::prelude::*;

use crate::convert::Repr;
use crate::errors::{ConfigurationError, to_py_err};
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
    if let Ok(envelope) = envelope.cast::<PyTomodachiJson>() {
        return Some(Arc::new(envelope.get().inner.clone()));
    }
    if let Ok(envelope) = envelope.cast::<PyTomodachiProtobuf>() {
        return Some(Arc::new(envelope.get().inner.clone()));
    }
    if envelope.cast::<PyAwsNotifications>().is_ok() {
        return Some(Arc::new(AwsNotifications));
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

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(Repr::new(py, "CloudEventsEnvelope")
            .field("content_type_attribute", self.inner.content_type_attribute)?
            .field("type_attribute", self.inner.type_attribute.as_deref())?
            .field("default_source", self.inner.default_source.as_str())?
            .finish())
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

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(Repr::new(py, "CloudEventsProtobufEnvelope")
            .field("type_attribute", self.inner.type_attribute.as_deref())?
            .field("default_source", self.inner.default_source.as_str())?
            .finish())
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

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(Repr::new(py, "RawEnvelope")
            .field("content_type_attribute", self.inner.content_type_attribute.as_deref())?
            .field("type_attribute", self.inner.type_attribute.as_deref())?
            .field("id_attribute", self.inner.id_attribute.as_deref())?
            .field("implicit_content_type", self.inner.implicit_content_type.as_deref())?
            .finish())
    }
}

/// The destination `kind` and `name` of `wrap_for`.
fn tomodachi_destination<'a>(kind: &str, name: &'a str) -> PyResult<TomodachiDestination<'a>> {
    match kind {
        "topic" => Ok(TomodachiDestination::Topic(name)),
        "queue" => Ok(TomodachiDestination::Queue),
        _ => Err(ConfigurationError::new_err(format!("destination kind must be 'topic' or 'queue', not {kind:?}"))),
    }
}

/// Default `type_attribute` of the tomodachi envelopes.
fn type_attr() -> Option<String> {
    Some("fanferro.type".to_string())
}

/// Default `compress_threshold` of the tomodachi envelopes.
fn threshold() -> Option<i64> {
    i64::try_from(TOMODACHI_COMPRESS_THRESHOLD).ok()
}

/// Checks the compression threshold of a tomodachi envelope.
fn checked_compress_threshold(envelope: &str, threshold: Option<i64>) -> PyResult<Option<usize>> {
    threshold
        .map(|threshold| {
            usize::try_from(threshold).map_err(|_| {
                ConfigurationError::new_err(format!(
                    "{envelope} compress_threshold must be 0 or more bytes (None never compresses), not {threshold}"
                ))
            })
        })
        .transpose()
}

macro_rules! tomodachi_envelope {
    ($py_name:literal, $rust:ident, $inner:ident, $doc:literal, $format:literal) => {
        #[doc = $doc]
        #[pyclass(name = $py_name, module = "fanferro._core", frozen, eq, hash)]
        #[derive(PartialEq, Eq, Hash)]
        pub struct $rust {
            inner: $inner,
        }

        #[pymethods]
        impl $rust {
            #[new]
            #[pyo3(signature = (*, service_name=None, service_uuid=None, type_attribute=type_attr(), compress_threshold=threshold()))]
            fn new(
                service_name: Option<String>,
                service_uuid: Option<String>,
                type_attribute: Option<String>,
                compress_threshold: Option<i64>,
            ) -> PyResult<Self> {
                let compress_threshold = checked_compress_threshold($py_name, compress_threshold)?;
                Ok(Self { inner: $inner { service_name, service_uuid, type_attribute, compress_threshold } })
            }

            #[doc = concat!("`\"", $format, "\"`.")]
            #[getter]
            fn name(&self) -> &str {
                self.inner.name()
            }

            /// `service.name` of published messages (`None`: the message's source).
            #[getter]
            fn service_name(&self) -> Option<&str> {
                self.inner.service_name.as_deref()
            }

            /// `service.uuid` of published messages (`None`: one generated per process).
            #[getter]
            fn service_uuid(&self) -> Option<&str> {
                self.inner.service_uuid.as_deref()
            }

            /// Attribute the message type is promoted to (`None` disables it).
            #[getter]
            fn type_attribute(&self) -> Option<&str> {
                self.inner.type_attribute.as_deref()
            }

            /// Size in bytes from which data is compressed like tomodachi does (`None`: never).
            #[getter]
            fn compress_threshold(&self) -> Option<usize> {
                self.inner.compress_threshold
            }

            /// Wraps a payload (as published to a topic, without naming it).
            fn wrap(&self, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
                wrap(&self.inner, payload)
            }

            /// Wraps a payload for a destination: a topic (named in `metadata.topic`) or a queue
            /// (in tomodachi's queue message document).
            fn wrap_for(&self, payload: &Bound<'_, PyPayload>, kind: &str, name: &str) -> PyResult<PyWireMessage> {
                let destination = tomodachi_destination(kind, name)?;
                self.inner
                    .wrap_for(&payload.get().inner, destination)
                    .map(|inner| PyWireMessage { inner })
                    .map_err(to_py_err)
            }

            /// Unwraps a wire message if it is in this tomodachi envelope format.
            fn unwrap(&self, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
                unwrap(&self.inner, wire)
            }

            fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
                Ok(Repr::new(py, $py_name)
                    .field("service_name", self.inner.service_name.as_deref())?
                    .field("service_uuid", self.inner.service_uuid.as_deref())?
                    .field("type_attribute", self.inner.type_attribute.as_deref())?
                    .field("compress_threshold", self.inner.compress_threshold)?
                    .finish())
            }
        }
    };
}

tomodachi_envelope!(
    "TomodachiJSONEnvelope",
    PyTomodachiJson,
    TomodachiJson,
    "The JSON envelope of tomodachi services (`tomodachi.envelope.JsonBase`).",
    "tomodachi-json"
);

tomodachi_envelope!(
    "TomodachiProtobufEnvelope",
    PyTomodachiProtobuf,
    TomodachiProtobuf,
    "The protobuf envelope of tomodachi services (`tomodachi.envelope.ProtobufBase`).",
    "tomodachi-protobuf"
);

/// Notifications of AWS services (S3 events, SES notifications), recognised by their body.
#[pyclass(name = "AwsNotificationsEnvelope", module = "fanferro._core", frozen, eq, hash)]
#[derive(PartialEq, Eq, Hash)]
pub struct PyAwsNotifications {
    inner: AwsNotifications,
}

#[pymethods]
impl PyAwsNotifications {
    #[new]
    fn new() -> Self {
        Self { inner: AwsNotifications }
    }

    /// `"aws"`.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Wraps a payload as AWS services send their notifications: the document, without attributes.
    fn wrap(&self, payload: &Bound<'_, PyPayload>) -> PyResult<PyWireMessage> {
        wrap(&self.inner, payload)
    }

    /// Unwraps a wire message if it is an S3 event or SES notification, giving it its message type.
    fn unwrap(&self, wire: &Bound<'_, PyWireMessage>) -> PyResult<Option<PyPayload>> {
        unwrap(&self.inner, wire)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(Repr::new(py, "AwsNotificationsEnvelope").finish())
    }
}
