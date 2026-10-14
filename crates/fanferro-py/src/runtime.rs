//! The Tokio runtime behind every awaitable returned by `fanferro._core`, and the bridge
//! that turns Rust futures into asyncio futures.
//!
//! The runtime is created lazily and is fork-aware: a process forked from a parent that
//! already started the runtime gets a fresh runtime (the parent's threads do not exist in
//! the child; the inherited runtime is leaked instead of dropped, because dropping it
//! would wait for threads that are gone). Clients created before a fork refuse to work in
//! the child (see `CoreClient::check_process`).
//!
//! The runtime is shut down by an `atexit` hook ([`shutdown`]), before the interpreter
//! starts finalizing: Tokio threads attach to Python to deliver results, and a thread that
//! attaches while the interpreter finalizes is killed by CPython 3.12 (`pthread_exit`),
//! which crashes the process.

use std::cell::OnceCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyList;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::generic::{self, ContextExt, Runtime as GenericRuntime};
use tokio::runtime::{Builder, Handle, Runtime};

use crate::errors::{ShutdownError, to_py_err_with};

/// The runtime and the process that created it.
static RUNTIME: Mutex<Option<(u32, Runtime)>> = Mutex::new(None);
/// After [`shutdown`]: the handle of the stopped runtime (spawning on it cancels the task).
static STOPPED: Mutex<Option<Handle>> = Mutex::new(None);
static SHUT_DOWN: AtomicBool = AtomicBool::new(false);
/// How long [`shutdown`] waits for running tasks (they stop at their next await point).
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

fn worker_threads() -> usize {
    std::env::var("FANFERRO_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 4)))
}

/// A handle to the current process's runtime (created on first use).
///
/// After [`shutdown`] this is the handle of the stopped runtime: tasks spawned on it are
/// cancelled immediately instead of starting new threads.
pub fn handle() -> Handle {
    let pid = std::process::id();
    let mut guard = RUNTIME.lock();
    if let Some((owner, runtime)) = guard.as_ref()
        && *owner == pid
    {
        return runtime.handle().clone();
    }
    if let Some((_, inherited)) = guard.take() {
        // Inherited from the parent across fork(): its worker threads do not exist here.
        std::mem::forget(inherited);
    }
    if SHUT_DOWN.load(Ordering::Acquire)
        && let Some(stopped) = STOPPED.lock().clone()
    {
        return stopped;
    }
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads())
        .thread_name("fanferro-worker")
        .build()
        .expect("failed to start the fanferro Tokio runtime");
    let handle = runtime.handle().clone();
    *guard = Some((pid, runtime));
    handle
}

/// Whether [`shutdown`] has run (the interpreter is exiting).
pub fn is_shut_down() -> bool {
    SHUT_DOWN.load(Ordering::Acquire)
}

/// Stops the runtime: running tasks are dropped at their next await point and queued work
/// is discarded. Waits (without holding the GIL, so that tasks delivering results to
/// Python can finish) up to [`SHUTDOWN_TIMEOUT`]. Registered with `atexit` on import.
pub fn shutdown(py: Python<'_>) {
    SHUT_DOWN.store(true, Ordering::Release);
    let taken = RUNTIME.lock().take();
    let Some((owner, runtime)) = taken else {
        return;
    };
    if owner != std::process::id() {
        std::mem::forget(runtime);
        return;
    }
    *STOPPED.lock() = Some(runtime.handle().clone());
    py.detach(|| runtime.shutdown_timeout(SHUTDOWN_TIMEOUT));
}

/// Forgets the runtime inherited from a parent process (called after `fork()`).
pub fn reset_after_fork() {
    let pid = std::process::id();
    let mut guard = RUNTIME.lock();
    if guard.as_ref().is_some_and(|(owner, _)| *owner != pid)
        && let Some((_, inherited)) = guard.take()
    {
        std::mem::forget(inherited);
    }
}

struct FanferroRuntime;

tokio::task_local! {
    static TASK_LOCALS: OnceCell<TaskLocals>;
}

impl GenericRuntime for FanferroRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        handle().spawn(fut)
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        // pyo3-async-runtimes delivers results with `Python::attach` in a blocking task.
        // Once shutdown has begun the result has no event loop to go to: leak the closure
        // (it owns Python references, and nothing may touch Python from here any more).
        handle().spawn_blocking(move || {
            if is_shut_down() {
                std::mem::forget(f);
            } else {
                f();
            }
        })
    }
}

impl ContextExt for FanferroRuntime {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = OnceCell::new();
        let _ = cell.set(locals);
        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS.try_with(|cell| cell.get().cloned()).unwrap_or_default()
    }
}

/// Converts a Rust future into an asyncio awaitable bound to the running event loop.
///
/// Cancelling the asyncio future drops the Rust future at its next await point.
pub fn future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    if is_shut_down() {
        return Err(ShutdownError::new_err("fanferro is shut down: the Python interpreter is exiting"));
    }
    generic::future_into_py::<FanferroRuntime, F, T>(py, fut)
}

/// Asyncio futures that are resolved in batches (used for settlements).
///
/// [`future_into_py`] resolves every awaitable on its own: a runtime thread attaches to the
/// interpreter and calls `call_soon_threadsafe` once per awaitable. Settlements complete in
/// batches (one batch call settles up to ten messages), so their futures are resolved together:
/// the first completion schedules one delivery, which attaches to the interpreter once and hands
/// everything that completed in the meantime to the event loop in a single callback. Deliveries
/// follow the same shutdown rules as [`future_into_py`]: after [`shutdown`] nothing touches
/// Python any more.
#[derive(Default)]
pub struct Completions {
    pending: Mutex<Vec<Completion>>,
    scheduled: AtomicBool,
}

struct Completion {
    event_loop: Py<PyAny>,
    future: Py<PyAny>,
    result: fanferro_core::Result<()>,
}

/// A future created by [`Completions::future`], waiting for its result.
pub struct PendingFuture {
    completions: Arc<Completions>,
    event_loop: Py<PyAny>,
    future: Py<PyAny>,
}

impl PendingFuture {
    /// Resolves the future (soon, together with other completions).
    pub fn complete(self, result: fanferro_core::Result<()>) {
        let Self { completions, event_loop, future } = self;
        completions.push(Completion { event_loop, future, result });
    }
}

impl Completions {
    /// A new asyncio future of the running event loop and the handle that resolves it.
    pub fn future<'py>(self: &Arc<Self>, py: Python<'py>) -> PyResult<(Bound<'py, PyAny>, PendingFuture)> {
        if is_shut_down() {
            return Err(ShutdownError::new_err("fanferro is shut down: the Python interpreter is exiting"));
        }
        let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
        let future = event_loop.call_method0(pyo3::intern!(py, "create_future"))?;
        let pending = PendingFuture {
            completions: self.clone(),
            event_loop: event_loop.unbind(),
            future: future.clone().unbind(),
        };
        Ok((future, pending))
    }

    fn push(self: &Arc<Self>, completion: Completion) {
        self.pending.lock().push(completion);
        if !self.scheduled.swap(true, Ordering::AcqRel) {
            let completions = self.clone();
            handle().spawn_blocking(move || completions.deliver());
        }
    }

    fn deliver(&self) {
        if is_shut_down() {
            // No event loop to go to: leak the futures (nothing may touch Python any more).
            std::mem::forget(std::mem::take(&mut *self.pending.lock()));
            return;
        }
        Python::attach(|py| {
            // Completions pushed from now on schedule another delivery.
            self.scheduled.store(false, Ordering::Release);
            let batch = std::mem::take(&mut *self.pending.lock());
            let mut by_loop: Vec<(Py<PyAny>, Bound<'_, PyList>)> = Vec::new();
            for Completion { event_loop, future, result } in batch {
                let error = match &result {
                    Ok(()) => py.None(),
                    Err(error) => to_py_err_with(py, error).into_value(py).into_any(),
                };
                let index = match by_loop.iter().position(|(known, _)| known.is(&event_loop)) {
                    Some(index) => index,
                    None => {
                        by_loop.push((event_loop, PyList::empty(py)));
                        by_loop.len() - 1
                    }
                };
                let _ = by_loop[index].1.append((future, error));
            }
            let Ok(resolve) = resolver(py) else { return };
            for (event_loop, items) in by_loop {
                // Fails only if the loop is closed: then nobody waits for these futures.
                let _ = event_loop.bind(py).call_method1(pyo3::intern!(py, "call_soon_threadsafe"), (resolve, items));
            }
        });
    }
}

/// Runs on the event loop: resolves `(future, exception or None)` pairs (skipping cancelled ones).
/// A future that cannot be resolved does not keep the others waiting.
#[pyfunction]
fn resolve_settlements(items: &Bound<'_, PyList>) -> PyResult<()> {
    let mut first_error = None;
    for item in items {
        if let Err(error) = resolve_settlement(&item) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn resolve_settlement(item: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = item.py();
    let (future, error): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item.extract()?;
    if future.call_method0(pyo3::intern!(py, "done"))?.is_truthy()? {
        return Ok(());
    }
    if error.is_none() {
        future.call_method1(pyo3::intern!(py, "set_result"), (py.None(),))?;
    } else {
        future.call_method1(pyo3::intern!(py, "set_exception"), (error,))?;
    }
    Ok(())
}

fn resolver(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static RESOLVER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    RESOLVER
        .get_or_try_init(py, || wrap_pyfunction!(resolve_settlements, py).map(|f| f.into_any().unbind()))
        .map(|resolve| resolve.bind(py))
}
