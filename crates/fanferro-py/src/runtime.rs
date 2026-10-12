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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::generic::{self, ContextExt, Runtime as GenericRuntime};
use tokio::runtime::{Builder, Handle, Runtime};

use crate::errors::ShutdownError;

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
