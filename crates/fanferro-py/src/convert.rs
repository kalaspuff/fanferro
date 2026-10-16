//! Conversions between Python objects and core types.

use bytes::Bytes;
use fanferro_core::types::{AttributeData, AttributeValue, HeaderValue, Headers, MessageAttributes};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyMemoryView, PyString};
use time::OffsetDateTime;

use crate::errors::ValidationError;
use crate::types::PyAttributeValue;

static DATETIME: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static UTC: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static EPOCH: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn datetime_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    Ok(DATETIME.get_or_try_init(py, || py.import("datetime")?.getattr("datetime").map(Bound::unbind))?.bind(py))
}

fn utc(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    Ok(UTC
        .get_or_try_init(py, || py.import("datetime")?.getattr("timezone")?.getattr("utc").map(Bound::unbind))?
        .bind(py))
}

fn epoch(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    Ok(EPOCH
        .get_or_try_init(py, || {
            let kwargs = PyDict::new(py);
            kwargs.set_item("tzinfo", utc(py)?)?;
            datetime_type(py)?.call((1970, 1, 1), Some(&kwargs)).map(Bound::unbind)
        })?
        .bind(py))
}

fn is_decimal(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = value.py();
    let decimal = DECIMAL.get_or_try_init(py, || py.import("decimal")?.getattr("Decimal").map(Bound::unbind))?.bind(py);
    value.is_instance(decimal)
}

/// Converts a UTC timestamp to an aware `datetime` (one constructor call).
pub fn time_to_py<'py>(py: Python<'py>, time: OffsetDateTime) -> PyResult<Bound<'py, PyAny>> {
    let time = time.to_offset(time::UtcOffset::UTC);
    datetime_type(py)?.call1((
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
        time.microsecond(),
        utc(py)?,
    ))
}

/// Converts a `datetime` (naive values are taken as UTC) or a POSIX timestamp.
pub fn time_from_py(value: &Bound<'_, PyAny>) -> PyResult<OffsetDateTime> {
    let py = value.py();
    if value.is_instance_of::<PyFloat>() || (value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>()) {
        let seconds: f64 = value.extract()?;
        #[allow(clippy::cast_possible_truncation)]
        let nanos = (seconds * 1e9) as i128;
        return OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map_err(|err| ValidationError::new_err(err.to_string()));
    }
    if !value.is_instance(datetime_type(py)?)? {
        return Err(ValidationError::new_err("time must be a datetime or a POSIX timestamp"));
    }
    let aware = if value.getattr("tzinfo")?.is_none() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("tzinfo", utc(py)?)?;
        value.call_method("replace", (), Some(&kwargs))?
    } else {
        value.clone()
    };
    let delta = aware.call_method1("__sub__", (epoch(py)?,))?;
    let days: i64 = delta.getattr("days")?.extract()?;
    let seconds: i64 = delta.getattr("seconds")?.extract()?;
    let micros: i64 = delta.getattr("microseconds")?.extract()?;
    let nanos = (i128::from(days) * 86_400 + i128::from(seconds)) * 1_000_000_000 + i128::from(micros) * 1_000;
    OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|err| ValidationError::new_err(err.to_string()))
}

/// Reads bytes from `bytes`, `bytearray`, `memoryview` or `str` (UTF-8).
pub fn bytes_from_py(value: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(bytes.as_bytes()));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(Bytes::copy_from_slice(text.to_str()?.as_bytes()));
    }
    if value.is_instance_of::<PyByteArray>() || value.is_instance_of::<PyMemoryView>() {
        let bytes = value.call_method0("__bytes__").or_else(|_| value.py().get_type::<PyBytes>().call1((value,)))?;
        return Ok(Bytes::copy_from_slice(bytes.cast::<PyBytes>()?.as_bytes()));
    }
    Err(ValidationError::new_err(format!("expected bytes or str, got {}", value.get_type().name()?)))
}

/// Converts a Python attribute value (an `AttributeValue`, `str`, `bytes`, `bool`, `int`,
/// `float` or `Decimal`) into a core attribute.
pub fn attribute_from_py(value: &Bound<'_, PyAny>) -> PyResult<AttributeValue> {
    if let Ok(attribute) = value.cast::<PyAttributeValue>() {
        return Ok(attribute.get().inner.clone());
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(AttributeValue::string(if value.extract::<bool>()? { "true" } else { "false" }));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(AttributeValue::string(text.to_str()?));
    }
    if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() || is_decimal(value)? {
        return Ok(AttributeValue::number(value.str()?.to_str()?));
    }
    if value.is_instance_of::<PyBytes>()
        || value.is_instance_of::<PyByteArray>()
        || value.is_instance_of::<PyMemoryView>()
    {
        return Ok(AttributeValue::binary(bytes_from_py(value)?));
    }
    Err(ValidationError::new_err(format!(
        "unsupported attribute value of type {}; use str, int, float, Decimal, bool, bytes or AttributeValue",
        value.get_type().name()?
    )))
}

/// Converts a mapping of attributes.
pub fn attributes_from_py(value: Option<&Bound<'_, PyAny>>) -> PyResult<MessageAttributes> {
    let mut attributes = MessageAttributes::new();
    let Some(value) = value else { return Ok(attributes) };
    if value.is_none() {
        return Ok(attributes);
    }
    if let Ok(dict) = value.cast_exact::<PyDict>() {
        for (name, attribute) in dict.iter() {
            attributes.insert(name.extract()?, attribute_from_py(&attribute)?);
        }
        return Ok(attributes);
    }
    let items = value.call_method0("items")?;
    for item in items.try_iter()? {
        let (name, attribute): (String, Bound<'_, PyAny>) = item?.extract()?;
        attributes.insert(name, attribute_from_py(&attribute)?);
    }
    Ok(attributes)
}

/// Converts attributes into a `dict[str, AttributeValue]`.
pub fn attributes_to_py<'py>(py: Python<'py>, attributes: &MessageAttributes) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, value) in attributes {
        dict.set_item(name, Py::new(py, PyAttributeValue { inner: value.clone() })?)?;
    }
    Ok(dict)
}

/// The Python value of an attribute (`str` for String/Number, `bytes` for Binary).
pub fn attribute_value_to_py<'py>(py: Python<'py>, value: &AttributeValue) -> PyResult<Bound<'py, PyAny>> {
    match value.data() {
        AttributeData::Text(text) => text.into_bound_py_any(py),
        AttributeData::Binary(bytes) => PyBytes::new(py, bytes).into_bound_py_any(py),
    }
}

/// Converts headers (`str`, `int` or `bool` values).
pub fn headers_from_py(value: Option<&Bound<'_, PyAny>>) -> PyResult<Headers> {
    let mut headers = Headers::new();
    let Some(value) = value else { return Ok(headers) };
    if value.is_none() {
        return Ok(headers);
    }
    if let Ok(dict) = value.cast_exact::<PyDict>() {
        for (name, header) in dict.iter() {
            let name: String = name.extract()?;
            let header = header_from_py(&name, &header)?;
            headers.insert(name, header);
        }
        return Ok(headers);
    }
    for item in value.call_method0("items")?.try_iter()? {
        let (name, header): (String, Bound<'_, PyAny>) = item?.extract()?;
        let header = header_from_py(&name, &header)?;
        headers.insert(name, header);
    }
    Ok(headers)
}

fn header_from_py(name: &str, header: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
    if header.is_instance_of::<PyBool>() {
        Ok(HeaderValue::Boolean(header.extract()?))
    } else if header.is_instance_of::<PyInt>() {
        Ok(HeaderValue::Integer(header.extract()?))
    } else if let Ok(text) = header.cast::<PyString>() {
        Ok(HeaderValue::String(text.to_str()?.to_string()))
    } else {
        Err(ValidationError::new_err(format!("header {name:?} must be str, int or bool")))
    }
}

/// Converts headers into a `dict`.
pub fn headers_to_py<'py>(py: Python<'py>, headers: &Headers) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, value) in headers {
        match value {
            HeaderValue::String(text) => dict.set_item(name, text)?,
            HeaderValue::Integer(number) => dict.set_item(name, number)?,
            HeaderValue::Boolean(flag) => dict.set_item(name, flag)?,
        }
    }
    Ok(dict)
}

/// Converts a string map into a `dict[str, str]`.
pub fn string_map_to_py<'py>(
    py: Python<'py>,
    map: &std::collections::BTreeMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        dict.set_item(key, value)?;
    }
    Ok(dict)
}

/// Reads optional seconds as a `Duration`.
pub fn seconds(value: Option<f64>, what: &str) -> PyResult<Option<std::time::Duration>> {
    let Some(secs) = value else { return Ok(None) };
    if !(secs.is_finite() && secs >= 0.0) {
        return Err(ValidationError::new_err(format!("{what} must be a non-negative number of seconds, got {secs}")));
    }
    std::time::Duration::try_from_secs_f64(secs)
        .map(Some)
        .map_err(|_| ValidationError::new_err(format!("{what} is out of range: {secs} seconds")))
}
