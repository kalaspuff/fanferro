//! Python exception hierarchy and conversion from core errors.
//!
//! ```text
//! FanferroError
//! ├── ConfigurationError               (also a ValueError)
//! ├── ValidationError
//! │   └── MessageTooLargeError        (.size, .limit)
//! ├── AwsError                         (.service .operation .kind .code .aws_message .request_id .status)
//! │   ├── ResourceNotFoundError
//! │   ├── AccessDeniedError
//! │   └── ThrottledError
//! ├── TransportError                   (network, HTTP, timeouts, checksum mismatches)
//! ├── PayloadError
//! │   └── PayloadIntegrityError
//! ├── EnvelopeError
//! ├── LeaseError                       (also a RuntimeError)
//! └── ShutdownError
//! ```
//!
//! Every instance has a `retryable` attribute. Python-only errors (provisioning, codecs,
//! routing, ...) are defined in `fanferro/errors.py` and also derive from `FanferroError`.
//! The classes below name `fanferro.errors` as their module too: it re-exports them, so
//! tracebacks show the public name and `pickle` finds them there. `create_exception!` takes one
//! base class, so the two that also derive from a built-in exception (`except ValueError` keeps
//! catching invalid options) are created by calling `type(name, bases, namespace)`.

use fanferro_core::error::{AwsErrorKind, Error};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

create_exception!(fanferro.errors, FanferroError, PyException, "Base class of all fanferro errors.");
create_exception!(
    fanferro.errors,
    ValidationError,
    FanferroError,
    "A value failed validation before any network call."
);
create_exception!(
    fanferro.errors,
    MessageTooLargeError,
    ValidationError,
    "The encoded message exceeds the destination limit and offloading is disabled."
);
create_exception!(fanferro.errors, AwsError, FanferroError, "An AWS (or emulator) API call failed.");
create_exception!(
    fanferro.errors,
    ResourceNotFoundError,
    AwsError,
    "The addressed queue, topic, subscription or object does not exist."
);
create_exception!(fanferro.errors, AccessDeniedError, AwsError, "Credentials are missing or lack permissions.");
create_exception!(fanferro.errors, ThrottledError, AwsError, "The request was throttled.");
create_exception!(fanferro.errors, TransportError, FanferroError, "A network, HTTP, timeout or checksum failure.");
create_exception!(fanferro.errors, PayloadError, FanferroError, "Storing or fetching a large payload failed.");
create_exception!(
    fanferro.errors,
    PayloadIntegrityError,
    PayloadError,
    "A fetched payload did not match its recorded size or checksum."
);
create_exception!(fanferro.errors, EnvelopeError, FanferroError, "A message envelope could not be produced or parsed.");
create_exception!(fanferro.errors, ShutdownError, FanferroError, "The consumer or client is shut down.");

/// An exception class deriving from `FanferroError` and a built-in exception, created once.
struct TwoBaseException {
    class: PyOnceLock<Py<PyType>>,
    name: &'static str,
    doc: &'static str,
    builtin: fn(Python<'_>) -> Bound<'_, PyType>,
}

impl TwoBaseException {
    fn class<'py>(&self, py: Python<'py>) -> &Bound<'py, PyType> {
        self.class
            .get_or_init(py, || {
                let created = (|| -> PyResult<Py<PyType>> {
                    let bases = PyTuple::new(py, [py.get_type::<FanferroError>(), (self.builtin)(py)])?;
                    let namespace = PyDict::new(py);
                    namespace.set_item("__module__", "fanferro.errors")?;
                    namespace.set_item("__doc__", self.doc)?;
                    let class = py.get_type::<PyType>().call1((self.name, bases, namespace))?;
                    Ok(class.cast_into::<PyType>()?.unbind())
                })();
                created.unwrap_or_else(|err| panic!("cannot create fanferro.errors.{}: {err}", self.name))
            })
            .bind(py)
    }

    fn new_err(&self, message: String) -> PyErr {
        Python::attach(|py| PyErr::from_type(self.class(py).clone(), message))
    }
}

static CONFIGURATION_ERROR: TwoBaseException = TwoBaseException {
    class: PyOnceLock::new(),
    name: "ConfigurationError",
    doc: "Invalid configuration (also a ValueError).",
    builtin: |py| py.get_type::<PyValueError>(),
};

static LEASE_ERROR: TwoBaseException = TwoBaseException {
    class: PyOnceLock::new(),
    name: "LeaseError",
    doc: "The message was already settled or its lease is no longer valid (also a RuntimeError).",
    builtin: |py| py.get_type::<PyRuntimeError>(),
};

/// `fanferro.errors.ConfigurationError`: invalid configuration (also a `ValueError`).
pub struct ConfigurationError;

impl ConfigurationError {
    /// Creates the error with `message`.
    pub fn new_err(message: impl Into<String>) -> PyErr {
        CONFIGURATION_ERROR.new_err(message.into())
    }
}

/// `fanferro.errors.LeaseError`: the message was already settled or its lease is no longer valid
/// (also a `RuntimeError`).
pub struct LeaseError;

impl LeaseError {
    /// Creates the error with `message`.
    pub fn new_err(message: impl Into<String>) -> PyErr {
        LEASE_ERROR.new_err(message.into())
    }
}

/// Converts a core error into a Python exception with structured attributes.
pub fn to_py_err(error: Error) -> PyErr {
    Python::attach(|py| to_py_err_with(py, &error))
}

/// Converts a core error while already attached to the interpreter.
pub fn to_py_err_with(py: Python<'_>, error: &Error) -> PyErr {
    let message = error.to_string();
    let err = match error {
        Error::Config(_) => ConfigurationError::new_err(message),
        Error::Validation(_) => ValidationError::new_err(message),
        Error::MessageTooLarge { .. } => MessageTooLargeError::new_err(message),
        Error::NotFound { .. } => ResourceNotFoundError::new_err(message),
        Error::Aws(aws) => match aws.kind {
            AwsErrorKind::NotFound => ResourceNotFoundError::new_err(message),
            AwsErrorKind::AccessDenied => AccessDeniedError::new_err(message),
            AwsErrorKind::Throttling => ThrottledError::new_err(message),
            _ => AwsError::new_err(message),
        },
        Error::Payload(_) => PayloadError::new_err(message),
        Error::PayloadIntegrity(_) => PayloadIntegrityError::new_err(message),
        Error::Envelope(_) => EnvelopeError::new_err(message),
        Error::Lease(_) => LeaseError::new_err(message),
        Error::Shutdown(_) => ShutdownError::new_err(message),
        Error::Http(_) | Error::Timeout(_) | Error::Checksum(_) => TransportError::new_err(message),
        _ => FanferroError::new_err(message),
    };
    let value = err.value(py);
    let _ = value.setattr("retryable", error.is_retryable());
    let _ = value.setattr("category", error.category());
    match error {
        Error::MessageTooLarge { size, limit } => {
            let _ = value.setattr("size", *size);
            let _ = value.setattr("limit", *limit);
        }
        Error::Aws(aws) => {
            let _ = value.setattr("service", aws.service);
            let _ = value.setattr("operation", aws.operation);
            let _ = value.setattr("kind", aws.kind.as_str());
            let _ = value.setattr("code", aws.code.clone());
            let _ = value.setattr("aws_message", aws.message.clone());
            let _ = value.setattr("request_id", aws.request_id.clone());
            let _ = value.setattr("status", aws.status);
        }
        _ => {}
    }
    err
}

/// Registers the exception classes on the module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    for (name, class) in [
        ("FanferroError", py.get_type::<FanferroError>()),
        ("ConfigurationError", CONFIGURATION_ERROR.class(py).clone()),
        ("ValidationError", py.get_type::<ValidationError>()),
        ("MessageTooLargeError", py.get_type::<MessageTooLargeError>()),
        ("AwsError", py.get_type::<AwsError>()),
        ("ResourceNotFoundError", py.get_type::<ResourceNotFoundError>()),
        ("AccessDeniedError", py.get_type::<AccessDeniedError>()),
        ("ThrottledError", py.get_type::<ThrottledError>()),
        ("TransportError", py.get_type::<TransportError>()),
        ("PayloadError", py.get_type::<PayloadError>()),
        ("PayloadIntegrityError", py.get_type::<PayloadIntegrityError>()),
        ("EnvelopeError", py.get_type::<EnvelopeError>()),
        ("LeaseError", LEASE_ERROR.class(py).clone()),
        ("ShutdownError", py.get_type::<ShutdownError>()),
    ] {
        // Errors raised from Rust default to non-retryable; instances override this.
        class.setattr("retryable", false)?;
        class.setattr("category", "fanferro")?;
        m.add(name, class)?;
    }
    Ok(())
}
