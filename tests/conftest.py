"""Shared fixtures.

* ``memory_client``: a client on the in-memory broker (fast, no network).
* ``ministack_endpoint``: URL of an AWS-compatible emulator. Uses ``FANFERRO_TEST_ENDPOINT_URL``
  when set; otherwise starts a MiniStack process for the session (installed with the test group).
* ``backend_client``: parametrized over ``memory`` and ``ministack`` so integration scenarios run
  against both the in-memory broker and a real AWS protocol implementation.

``FANFERRO_TEST_EVENT_LOOP=uvloop`` runs every async test on uvloop's event loop instead of
asyncio's (tests that create loops themselves, with ``asyncio.run``, keep asyncio's).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import pytest

import fanferro
from fanferro import testing
from tests.global_state import GlobalState


@pytest.fixture(autouse=True)
def _leaves_global_state_alone() -> Iterator[None]:
    """Fails a test that leaves a global tracer or meter provider or a changed root logger behind.

    Such state changes what later tests see, so results depended on test order (#168).
    """
    before = GlobalState.capture()
    yield
    after = GlobalState.capture()
    if changes := before.changes(after):
        before.restore()
        pytest.fail("the test changed process-wide state: " + "; ".join(changes), pytrace=False)


_EVENT_LOOP = os.environ.get("FANFERRO_TEST_EVENT_LOOP", "asyncio")

if _EVENT_LOOP == "uvloop":
    try:
        import uvloop
    except ImportError as error:
        raise pytest.UsageError("FANFERRO_TEST_EVENT_LOOP=uvloop needs uvloop (in the test group)") from error

    def pytest_asyncio_loop_factories(
        config: pytest.Config, item: pytest.Item
    ) -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
        """Runs every async test on uvloop's event loop."""
        return {"uvloop": uvloop.new_event_loop}

elif _EVENT_LOOP != "asyncio":
    raise pytest.UsageError(f"FANFERRO_TEST_EVENT_LOOP must be 'asyncio' or 'uvloop', not {_EVENT_LOOP!r}")


def pytest_report_header(config: pytest.Config) -> str:
    return f"event loop of async tests: {_EVENT_LOOP} (FANFERRO_TEST_EVENT_LOOP)"


def unique_name(prefix: str = "t") -> str:
    """A resource name that does not collide between tests."""
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def name() -> Callable[[str], str]:
    return unique_name


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_healthy(url: str, process: subprocess.Popen[bytes] | None, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"MiniStack exited with code {process.returncode}")
        try:
            with urllib.request.urlopen(f"{url}/_ministack/health", timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"MiniStack at {url} did not become healthy within {timeout} seconds")


@pytest.fixture(scope="session")
def ministack_endpoint(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    configured = os.environ.get("FANFERRO_TEST_ENDPOINT_URL")
    if configured:
        yield configured.rstrip("/")
        return
    executable = shutil.which("ministack") or str(Path(sys.executable).with_name("ministack"))
    if not Path(executable).exists():
        pytest.skip("MiniStack is not installed and FANFERRO_TEST_ENDPOINT_URL is not set")
    port = _free_port()
    log = tmp_path_factory.mktemp("ministack") / "ministack.log"
    env = {**os.environ, "GATEWAY_PORT": str(port), "MINISTACK_REGION": "us-east-1"}
    with log.open("wb") as output:
        process = subprocess.Popen([executable], env=env, stdout=output, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_healthy(url, process)
        yield url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


def emulator_client(endpoint: str, **options: object) -> fanferro.Client:
    """A client for the emulator; resources are created on demand by default."""
    options.setdefault("provisioning", fanferro.Provisioning.ensure())
    return fanferro.Client(
        region="us-east-1",
        endpoint_url=endpoint,
        credentials=fanferro.Credentials("test", "test"),
        account_id="000000000000",
        **options,  # type: ignore[arg-type]
    )


@pytest.fixture
async def memory_client() -> AsyncIterator[fanferro.Client]:
    client = testing.memory_client()
    yield client
    await client.close()


type ClientFactory = Callable[..., fanferro.Client]


@pytest.fixture(params=["memory", pytest.param("ministack", marks=pytest.mark.e2e)])
def backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
async def client_factory(backend: str, request: pytest.FixtureRequest) -> AsyncIterator[ClientFactory]:
    """Creates clients on the parametrized backend (closing them after the test)."""
    created: list[fanferro.Client] = []
    broker = fanferro.MemoryBroker()
    endpoint = request.getfixturevalue("ministack_endpoint") if backend == "ministack" else None

    def factory(**options: object) -> fanferro.Client:
        if endpoint is None:
            client = testing.memory_client(broker=broker, **options)  # type: ignore[arg-type]
        else:
            client = emulator_client(endpoint, **options)
        created.append(client)
        return client

    yield factory
    for client in created:
        await client.close()
