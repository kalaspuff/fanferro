//! `AttributeValue`, `Payload` and `WireMessage`: the value types shared by envelopes,
//! the publisher and the consumer.

use fanferro_core::types::{AttributeData, AttributeValue, BaseType, Payload, WireMessage};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::convert::{
    attribute_value_to_py, attributes_from_py, attributes_to_py, bytes_from_py, headers_from_py, headers_to_py,
    time_from_py, time_to_py,
};
use crate::errors::{ValidationError, to_py_err};

/// A message attribute value with its SQS/SNS data type.
#[pyclass(name = "AttributeValue", module = "fanferro._core", frozen, eq, hash, skip_from_py_object)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyAttributeValue {
    pub inner: AttributeValue,
}

#[pymethods]
impl PyAttributeValue {
    /// Creates an attribute with an explicit data type such as `"Number.int"`.
    #[new]
    fn new(data_type: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let base = fanferro_core::types::attributes::parse_base_type(data_type).map_err(to_py_err)?;
        let data = match base {
            BaseType::Binary => AttributeData::Binary(bytes_from_py(value)?),
            BaseType::String | BaseType::Number => {
                if value.is_instance_of::<PyBytes>() {
                    return Err(ValidationError::new_err(format!("{data_type} attributes take str values")));
                }
                AttributeData::Text(value.str()?.to_str()?.to_string())
            }
        };
        let inner = AttributeValue::with_type(data_type, data).map_err(to_py_err)?;
        Ok(Self { inner })
    }

    /// A `String` attribute.
    #[staticmethod]
    fn string(value: &str) -> Self {
        Self { inner: AttributeValue::string(value) }
    }

    /// A `Number` attribute (from `int`, `float`, `Decimal` or numeric `str`).
    #[staticmethod]
    fn number(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: AttributeValue::number(value.str()?.to_str()?) })
    }

    /// A `Binary` attribute.
    #[staticmethod]
    fn binary(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: AttributeValue::binary(bytes_from_py(value)?) })
    }

    /// The full data type, e.g. `"String"` or `"Number.float"`.
    #[getter]
    fn data_type(&self) -> &str {
        self.inner.data_type()
    }

    /// `"String"`, `"Number"` or `"Binary"`.
    #[getter]
    fn base_type(&self) -> &'static str {
        self.inner.base_type().as_str()
    }

    /// The value: `str` for String/Number, `bytes` for Binary.
    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        attribute_value_to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        match self.inner.data() {
            AttributeData::Text(text) => format!("AttributeValue({:?}, {text:?})", self.inner.data_type()),
            AttributeData::Binary(bytes) => {
                format!("AttributeValue({:?}, <{} bytes>)", self.inner.data_type(), bytes.len())
            }
        }
    }
}

/// An encoded message plus metadata (the input of `Envelope.wrap`, output of `unwrap`).
#[pyclass(name = "Payload", module = "fanferro._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyPayload {
    pub inner: Payload,
}

fn payload_from_kwargs(base: Payload, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Payload> {
    let mut payload = base;
    let Some(kwargs) = kwargs else { return Ok(payload) };
    for (key, value) in kwargs.iter() {
        let key: String = key.extract()?;
        let text = |value: &Bound<'_, PyAny>| -> PyResult<Option<String>> {
            if value.is_none() { Ok(None) } else { value.extract().map(Some) }
        };
        match key.as_str() {
            "data" => payload.data = bytes_from_py(&value)?,
            "content_type" => payload.content_type = text(&value)?,
            "message_type" => payload.message_type = text(&value)?,
            "id" => payload.id = text(&value)?,
            "time" => payload.time = if value.is_none() { None } else { Some(time_from_py(&value)?) },
            "source" => payload.source = text(&value)?,
            "subject" => payload.subject = text(&value)?,
            "data_schema" => payload.data_schema = text(&value)?,
            "headers" => payload.headers = headers_from_py(Some(&value))?,
            other => {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!("unexpected keyword argument {other:?}")));
            }
        }
    }
    Ok(payload)
}

#[pymethods]
impl PyPayload {
    #[new]
    #[pyo3(signature = (
        data,
        *,
        content_type=None,
        message_type=None,
        id=None,
        time=None,
        source=None,
        subject=None,
        data_schema=None,
        headers=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        data: &Bound<'_, PyAny>,
        content_type: Option<String>,
        message_type: Option<String>,
        id: Option<String>,
        time: Option<&Bound<'_, PyAny>>,
        source: Option<String>,
        subject: Option<String>,
        data_schema: Option<String>,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let time = match time {
            Some(time) if !time.is_none() => Some(time_from_py(time)?),
            _ => None,
        };
        Ok(Self {
            inner: Payload {
                data: bytes_from_py(data)?,
                content_type,
                message_type,
                id,
                time,
                source,
                subject,
                data_schema,
                headers: headers_from_py(headers)?,
            },
        })
    }

    /// Encoded message bytes.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.data)
    }

    /// Media type of `data`.
    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.inner.content_type.as_deref()
    }

    /// Logical message type.
    #[getter]
    fn message_type(&self) -> Option<&str> {
        self.inner.message_type.as_deref()
    }

    /// Message id.
    #[getter]
    fn id(&self) -> Option<&str> {
        self.inner.id.as_deref()
    }

    /// Creation time (aware UTC `datetime`).
    #[getter]
    fn time<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.inner.time.map(|time| time_to_py(py, time)).transpose()
    }

    /// Producer URI reference.
    #[getter]
    fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }

    /// Subject.
    #[getter]
    fn subject(&self) -> Option<&str> {
        self.inner.subject.as_deref()
    }

    /// Schema reference.
    #[getter]
    fn data_schema(&self) -> Option<&str> {
        self.inner.data_schema.as_deref()
    }

    /// Extension headers.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        headers_to_py(py, &self.inner.headers)
    }

    /// Returns a copy with the given fields replaced.
    #[pyo3(signature = (**kwargs))]
    fn replace(&self, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        Ok(Self { inner: payload_from_kwargs(self.inner.clone(), kwargs)? })
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Self>().is_ok_and(|other| other.get().inner == self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "Payload(<{} bytes>, content_type={:?}, message_type={:?}, id={:?})",
            self.inner.data.len(),
            self.inner.content_type,
            self.inner.message_type,
            self.inner.id
        )
    }
}

/// A message as transported: a body and message attributes.
#[pyclass(name = "WireMessage", module = "fanferro._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyWireMessage {
    pub inner: WireMessage,
}

#[pymethods]
impl PyWireMessage {
    #[new]
    #[pyo3(signature = (body, attributes=None))]
    fn new(body: &Bound<'_, PyAny>, attributes: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        Ok(Self { inner: WireMessage::new(bytes_from_py(body)?, attributes_from_py(attributes)?) })
    }

    /// Body bytes.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.body)
    }

    /// Body as text when it is valid UTF-8.
    #[getter]
    fn body_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.inner.body).ok()
    }

    /// Message attributes.
    #[getter]
    fn attributes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        attributes_to_py(py, &self.inner.attributes)
    }

    /// Returns a copy with a different body and/or attributes.
    #[pyo3(signature = (*, body=None, attributes=None))]
    fn replace(&self, body: Option<&Bound<'_, PyAny>>, attributes: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut inner = self.inner.clone();
        if let Some(body) = body {
            inner.body = bytes_from_py(body)?;
        }
        if let Some(attributes) = attributes {
            inner.attributes = attributes_from_py(Some(attributes))?;
        }
        Ok(Self { inner })
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Self>().is_ok_and(|other| other.get().inner == self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "WireMessage(<{} bytes>, attributes={:?})",
            self.inner.body.len(),
            self.inner.attributes.keys().collect::<Vec<_>>()
        )
    }
}

/// Wraps a core wire message into a Python object.
pub fn wire_to_py(py: Python<'_>, wire: WireMessage) -> PyResult<Py<PyAny>> {
    Py::new(py, PyWireMessage { inner: wire })?.into_py_any(py)
}
