//! The Tokio runtime behind every awaitable returned by `fanferro._core`, and the bridge that
//! hands the outcomes of Rust futures to asyncio.
//!
//! The runtime is created lazily and is fork-aware: a process forked from a parent that
//! already started the runtime gets a fresh runtime (the parent's threads do not exist in
//! the child; the inherited runtime is leaked instead of dropped, because dropping it
//! would wait for threads that are gone). Clients created before a fork refuse to work in
//! the child (see `CoreClient::check_process`).
//!
//! The runtime is shut down by an `atexit` hook ([`shutdown`]), before the interpreter
//! starts finalizing: a thread that attaches to the interpreter while it finalizes is killed by
//! CPython 3.12 (`pthread_exit`), which crashes the process.
//!
//! # The bridge
//!
//! An awaitable is a [`RustFuture`](RUST_FUTURE) (an `asyncio.Future` whose `cancel()` also
//! aborts the Rust work) driven by one Tokio task. Outcomes are converted into Python objects on
//! the event loop's thread, in batches: a completed task queues its outcome on the
//! [`LoopBridge`] of the future's event loop, and the first outcome of a batch wakes the loop.
//!
//! * On Unix the loop watches one end of a socket pair (`loop.add_reader`), and a Tokio thread
//!   wakes it by writing a byte to the other end: runtime threads never attach to the
//!   interpreter, so they never wait for the GIL and never compete with the event loop for it.
//! * Elsewhere (the proactor event loop on Windows, loops without `add_reader`), a blocking task
//!   attaches once per batch and schedules the batch with `loop.call_soon_threadsafe`.
//!
//! Either way the loop then resolves every future of the batch in one callback.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt as _;
use parking_lot::Mutex;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use tokio::runtime::{Builder, Handle, Runtime};

use crate::errors::{FanferroError, ShutdownError, to_py_err_with};

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

fn shut_down_error() -> PyErr {
    ShutdownError::new_err("fanferro is shut down: the Python interpreter is exiting")
}

/// Stops the runtime: running tasks are dropped at their next await point and queued work
/// is discarded. Waits (without holding the GIL, so that blocking tasks attached to the
/// interpreter can finish) up to [`SHUTDOWN_TIMEOUT`]. Registered with `atexit` on import.
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

/// Forgets the runtime and the event loop bridges inherited from a parent process (called after
/// `fork()`).
pub fn reset_after_fork() {
    let pid = std::process::id();
    let mut guard = RUNTIME.lock();
    if guard.as_ref().is_some_and(|(owner, _)| *owner != pid)
        && let Some((_, inherited)) = guard.take()
    {
        std::mem::forget(inherited);
    }
    drop(guard);
    // The parent's bridges belong to its event loops and its runtime threads. (`LOOPS` itself is
    // only locked by threads attached to the interpreter, so no other thread held it at fork().)
    let inherited: Vec<Registered> = LOOPS.lock().drain(..).collect();
    for registered in inherited {
        registered.abandon();
    }
}

/// What a Rust future hands to its awaitable: converted into a Python object on the event loop's
/// thread, while it is attached to the interpreter anyway.
pub trait IntoPyResult: Send + 'static {
    /// The awaitable's result (or exception).
    fn into_py_result(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl<T> IntoPyResult for fanferro_core::Result<T>
where
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    fn into_py_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Ok(value) => value.into_py_any(py),
            Err(error) => Err(to_py_err_with(py, &error)),
        }
    }
}

/// A custom conversion of a future's output (runs on the event loop's thread).
pub struct Convert<F>(pub F);

impl<F> IntoPyResult for Convert<F>
where
    F: FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send + 'static,
{
    fn into_py_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        (self.0)(py)
    }
}

/// Converts a successful outcome with `convert`; errors become fanferro exceptions.
pub fn convert<T, F>(result: fanferro_core::Result<T>, convert: F) -> impl IntoPyResult
where
    T: Send + 'static,
    F: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    Convert(move |py: Python<'_>| match result {
        Ok(value) => convert(py, value),
        Err(error) => Err(to_py_err_with(py, &error)),
    })
}

/// Converts a Rust future into an asyncio awaitable of the running event loop.
///
/// Cancelling the awaitable aborts the Rust future at its next await point.
pub fn future_into_py<F>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future + Send + 'static,
    F::Output: IntoPyResult,
{
    if is_shut_down() {
        return Err(shut_down_error());
    }
    let bridge = LoopBridge::running(py)?;
    let future = rust_future_type(py)?.call0()?;
    let target = future.clone().unbind();
    let task = handle().spawn(async move {
        let outcome: Outcome = match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(output) => Box::new(move |py| output.into_py_result(py)),
            Err(panic) => {
                let message = panic_message(panic.as_ref());
                Box::new(move |_| Err(FanferroError::new_err(format!("rust future panicked: {message}"))))
            }
        };
        bridge.complete(Completion { future: target, outcome });
    });
    future.setattr(pyo3::intern!(py, "_fanferro_abort"), Abort(task.abort_handle()))?;
    Ok(future)
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = panic.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = panic.downcast_ref::<String>() {
        text.clone()
    } else {
        "unknown error".to_string()
    }
}

/// An awaitable of the running event loop whose outcome is delivered later through
/// [`PendingFuture::complete`] (used for settlements, whose work runs on its own: cancelling the
/// awaitable does not stop it).
pub fn pending_future(py: Python<'_>) -> PyResult<(Bound<'_, PyAny>, PendingFuture)> {
    if is_shut_down() {
        return Err(shut_down_error());
    }
    let bridge = LoopBridge::running(py)?;
    let future = rust_future_type(py)?.call0()?;
    let pending = PendingFuture { bridge, future: future.clone().unbind() };
    Ok((future, pending))
}

/// A future created by [`pending_future`], waiting for its outcome.
pub struct PendingFuture {
    bridge: Arc<LoopBridge>,
    future: Py<PyAny>,
}

impl PendingFuture {
    /// Resolves the future (soon, together with other outcomes of its event loop).
    pub fn complete(self, result: fanferro_core::Result<()>) {
        let Self { bridge, future } = self;
        bridge.complete(Completion { future, outcome: Box::new(move |py| result.into_py_result(py)) });
    }
}

/// Python source of `RustFuture`: an `asyncio.Future` whose `cancel()` also aborts the Rust work
/// (instead of a done callback per awaitable, which would cost an extra loop callback each).
const RUST_FUTURE: &std::ffi::CStr = c"
import asyncio


class RustFuture(asyncio.Future):
    __slots__ = ('_fanferro_abort',)

    def cancel(self, msg=None):
        if not super().cancel(msg):
            return False
        abort = getattr(self, '_fanferro_abort', None)
        self._fanferro_abort = None
        if abort is not None:
            abort()
        return True
";

fn rust_future_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    TYPE.get_or_try_init(py, || {
        let globals = PyDict::new(py);
        globals.set_item("__name__", "fanferro._core")?;
        py.run(RUST_FUTURE, Some(&globals), None)?;
        globals
            .get_item("RustFuture")?
            .map(Bound::unbind)
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("RustFuture was not defined"))
    })
    .map(|class| class.bind(py))
}

fn get_running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    GET_RUNNING_LOOP
        .get_or_try_init(py, || py.import("asyncio")?.getattr("get_running_loop").map(Bound::unbind))?
        .bind(py)
        .call0()
}

/// Aborts the Tokio task behind a `RustFuture` (called by its `cancel()`).
#[pyclass(frozen)]
struct Abort(tokio::task::AbortHandle);

#[pymethods]
impl Abort {
    fn __call__(&self) {
        self.0.abort();
    }
}

type Outcome = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

/// A finished future's outcome, waiting to be handed to its awaitable.
struct Completion {
    future: Py<PyAny>,
    outcome: Outcome,
}

impl Completion {
    fn resolve(self, py: Python<'_>) {
        let Self { future, outcome } = self;
        let future = future.into_bound(py);
        // Cancelled awaitables (and futures that cannot be inspected) take no outcome.
        if !matches!(future.call_method0(pyo3::intern!(py, "done")).and_then(|done| done.is_truthy()), Ok(false)) {
            return;
        }
        // Fails only when the loop is closed: then nobody waits for the outcome.
        let _ = match outcome(py) {
            Ok(value) => future.call_method1(pyo3::intern!(py, "set_result"), (value,)),
            Err(error) => future.call_method1(pyo3::intern!(py, "set_exception"), (error.into_value(py),)),
        };
    }
}

/// How a [`LoopBridge`] wakes its event loop.
enum Wake {
    /// The loop watches `reader` (`loop.add_reader`); writing a byte to `writer` wakes it.
    #[cfg(unix)]
    Socket { reader: std::os::unix::net::UnixStream, writer: std::os::unix::net::UnixStream },
    /// A blocking task schedules the batch with `loop.call_soon_threadsafe`.
    Threadsafe { event_loop: Py<PyAny> },
}

/// Hands outcomes of Rust futures to one event loop, in batches.
struct LoopBridge {
    pending: Mutex<Vec<Completion>>,
    /// Whether the loop has been woken for the pending outcomes (only the first outcome of a batch
    /// wakes it).
    signaled: AtomicBool,
    /// The loop is closed (or belongs to a parent process): outcomes are dropped.
    closed: AtomicBool,
    wake: Wake,
}

/// An event loop that awaitables were created on, and its bridge.
struct Registered {
    pid: u32,
    event_loop: Py<PyAny>,
    bridge: Arc<LoopBridge>,
}

impl Registered {
    /// Closes a bridge inherited from the parent process, without its lock: a runtime thread of
    /// the parent may have held it at fork() (it queues finished futures there), and that thread
    /// does not exist here to release it. The bridge is leaked: its queue may be in any state.
    fn abandon(self) {
        self.bridge.closed.store(true, Ordering::Release);
        std::mem::forget(self);
    }
}

/// Bridges by event loop. Entries keep their loop alive, so a loop's address identifies its
/// entry; entries of closed loops are dropped when the next loop registers.
static LOOPS: Mutex<Vec<Registered>> = Mutex::new(Vec::new());

impl LoopBridge {
    fn new(wake: Wake) -> Self {
        Self { pending: Mutex::new(Vec::new()), signaled: AtomicBool::new(false), closed: AtomicBool::new(false), wake }
    }

    /// The bridge of the running event loop (registered on the loop's first awaitable).
    fn running(py: Python<'_>) -> PyResult<Arc<Self>> {
        let event_loop = get_running_loop(py)?;
        let pid = std::process::id();
        if let Some(registered) =
            LOOPS.lock().iter().find(|registered| registered.pid == pid && registered.event_loop.is(&event_loop))
        {
            return Ok(registered.bridge.clone());
        }
        Self::register(py, &event_loop, pid)
    }

    fn register(py: Python<'_>, event_loop: &Bound<'_, PyAny>, pid: u32) -> PyResult<Arc<Self>> {
        // Python is called without holding `LOOPS`: a thread switch in between could otherwise let
        // another thread wait for the lock while holding the GIL this thread needs.
        let known: Vec<Py<PyAny>> = LOOPS.lock().iter().map(|registered| registered.event_loop.clone_ref(py)).collect();
        let closed: Vec<Py<PyAny>> = known
            .into_iter()
            .filter(|known| {
                !matches!(
                    known.bind(py).call_method0(pyo3::intern!(py, "is_closed")).and_then(|closed| closed.is_truthy()),
                    Ok(false)
                )
            })
            .collect();
        let bridge = Self::connect(py, event_loop);
        let removed: Vec<Registered> = {
            let mut loops = LOOPS.lock();
            let (keep, removed) = loops.drain(..).partition(|registered: &Registered| {
                registered.pid == pid
                    && !registered.event_loop.is(event_loop)
                    && !closed.iter().any(|closed| registered.event_loop.is(closed))
            });
            *loops = keep;
            loops.push(Registered { pid, event_loop: event_loop.clone().unbind(), bridge: bridge.clone() });
            removed
        };
        for registered in removed {
            if registered.pid == pid {
                registered.bridge.close();
            } else {
                registered.abandon();
            }
        }
        Ok(bridge)
    }

    /// Creates the bridge of `event_loop`: a socket the loop watches where possible, otherwise
    /// `call_soon_threadsafe`.
    fn connect(py: Python<'_>, event_loop: &Bound<'_, PyAny>) -> Arc<Self> {
        #[cfg(unix)]
        if let Some(bridge) = Self::connect_socket(py, event_loop) {
            return bridge;
        }
        #[cfg(not(unix))]
        let _ = py;
        Arc::new(Self::new(Wake::Threadsafe { event_loop: event_loop.clone().unbind() }))
    }

    #[cfg(unix)]
    fn connect_socket(py: Python<'_>, event_loop: &Bound<'_, PyAny>) -> Option<Arc<Self>> {
        use std::os::fd::AsRawFd;

        let (reader, writer) = std::os::unix::net::UnixStream::pair().ok()?;
        reader.set_nonblocking(true).ok()?;
        writer.set_nonblocking(true).ok()?;
        let fd = reader.as_raw_fd();
        let bridge = Arc::new(Self::new(Wake::Socket { reader, writer }));
        let drain = Py::new(py, Drain { bridge: bridge.clone() }).ok()?;
        // Not every loop supports file descriptors (e.g. the proactor event loop on Windows).
        event_loop.call_method1(pyo3::intern!(py, "add_reader"), (fd, drain)).ok()?;
        Some(bridge)
    }

    /// Queues the outcome of a finished future and wakes the loop for the first one of a batch.
    fn complete(self: &Arc<Self>, completion: Completion) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.pending.lock().push(completion);
        if !self.signaled.swap(true, Ordering::SeqCst) {
            self.wake();
        }
    }

    fn wake(self: &Arc<Self>) {
        match &self.wake {
            #[cfg(unix)]
            Wake::Socket { writer, .. } => {
                // Non-blocking: a full socket holds wake-ups enough; a closed one has no loop left.
                let _ = std::io::Write::write(&mut &*writer, &[1]);
            }
            Wake::Threadsafe { .. } => {
                let bridge = self.clone();
                handle().spawn_blocking(move || bridge.schedule());
            }
        }
    }

    /// Schedules [`LoopBridge::drain`] on the loop (fallback mode, on a blocking thread).
    fn schedule(self: Arc<Self>) {
        if is_shut_down() {
            // No event loop to go to: leak the futures (nothing may touch Python any more).
            std::mem::forget(std::mem::take(&mut *self.pending.lock()));
            return;
        }
        Python::attach(|py| {
            let event_loop = match &self.wake {
                Wake::Threadsafe { event_loop } => event_loop.bind(py),
                #[cfg(unix)]
                Wake::Socket { .. } => return,
            };
            let scheduled = Py::new(py, Drain { bridge: self.clone() })
                .and_then(|drain| event_loop.call_method1(pyo3::intern!(py, "call_soon_threadsafe"), (drain,)));
            if scheduled.is_err() {
                // The loop is closed: nobody waits for these futures any more.
                self.close();
            }
        });
    }

    /// Resolves every pending future (on the loop's thread).
    fn drain(&self, py: Python<'_>) {
        if self.closed.load(Ordering::Acquire) {
            // Nothing to resolve; for a bridge inherited across fork(), the queue and the socket
            // still belong to the parent (and the queue's lock may never be released here).
            return;
        }
        #[cfg(unix)]
        if let Wake::Socket { reader, .. } = &self.wake {
            // Consume the wake-ups before taking the batch: an outcome queued after this point
            // wakes the loop again (`signaled` is reset first).
            let mut buffer = [0u8; 64];
            while matches!(std::io::Read::read(&mut &*reader, &mut buffer), Ok(read) if read > 0) {}
        }
        self.signaled.store(false, Ordering::SeqCst);
        let batch = std::mem::take(&mut *self.pending.lock());
        for completion in batch {
            completion.resolve(py);
        }
    }

    /// Drops pending and future outcomes (the loop is closed).
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // Released after the lock: dropping Python objects can run arbitrary code.
        let pending = std::mem::take(&mut *self.pending.lock());
        drop(pending);
    }
}

/// Resolves the pending futures of a [`LoopBridge`] (called by the event loop).
#[pyclass(frozen)]
struct Drain {
    bridge: Arc<LoopBridge>,
}

#[pymethods]
impl Drain {
    fn __call__(&self, py: Python<'_>) {
        self.bridge.drain(py);
    }
}
