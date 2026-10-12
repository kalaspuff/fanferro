"""Process-level behaviour: interpreter exit and fork() with the Tokio runtime running.

Each scenario runs in a fresh interpreter, because it ends (or forks) the process.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

PUBLISH_AND_EXIT = """
import asyncio
from fanferro.testing import memory_client

async def main():
    client = memory_client()
    topic = client.topic("t")
    async with client:
        await topic.publish({"n": 1}, type="t")

asyncio.run(main())
print("done")
"""


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-X", "faulthandler", "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_exiting_right_after_awaiting_does_not_crash() -> None:
    # Regression: Tokio threads delivered results to Python while CPython 3.12 finalized and were
    # killed with pthread_exit (segfault at exit, most runs). The runtime now stops in atexit.
    for _ in range(5):
        result = run_python(PUBLISH_AND_EXIT)
        assert (result.returncode, result.stdout.strip()) == (0, "done"), result.stderr


def test_exiting_with_a_running_consumer_does_not_crash() -> None:
    result = run_python("""
        import asyncio
        import fanferro
        from fanferro.testing import memory_client

        async def main():
            client = memory_client()
            queue = client.queue("q")
            consumer = client.consumer(queue, wait_time=5)

            @consumer.default
            async def handle(message: fanferro.Message[bytes]) -> None:
                await asyncio.sleep(10)

            await client.__aenter__()
            await queue.send(b"x", type="x")
            asyncio.get_running_loop().create_task(consumer.run(install_signal_handlers=False))
            await asyncio.sleep(0.5)
            # Return without stopping the consumer or closing the client.

        asyncio.run(main())
        print("done")
    """)
    assert (result.returncode, result.stdout.strip()) == (0, "done"), result.stderr


def test_awaitables_fail_cleanly_after_shutdown() -> None:
    result = run_python("""
        import asyncio
        import fanferro
        from fanferro import _core
        from fanferro.testing import memory_client

        async def main():
            client = memory_client()
            topic = client.topic("t")
            async with client:
                await topic.publish({"n": 1}, type="t")
                _core._shutdown()  # what atexit does
                try:
                    await topic.publish({"n": 2}, type="t")
                except fanferro.ShutdownError as error:
                    print("ShutdownError:", error)

        asyncio.run(main())
    """)
    assert result.returncode == 0, result.stderr
    assert "ShutdownError: fanferro is shut down" in result.stdout, result.stdout + result.stderr


@pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(os, "fork"),
    reason="fork() of a multi-threaded process is only dependable on Linux",
)
def test_fork_child_gets_a_fresh_runtime() -> None:
    result = run_python("""
        import asyncio
        import os
        import warnings
        import fanferro
        from fanferro.testing import memory_client

        warnings.simplefilter("ignore", DeprecationWarning)  # "multi-threaded process" fork warning

        async def publish(client, label):
            topic = client.topic("t")
            async with client:
                await topic.publish({"n": 1}, type="t")
            print(label, "published", flush=True)

        parent_client = memory_client()
        asyncio.run(publish(parent_client, "parent"))

        pid = os.fork()
        if pid == 0:
            try:
                asyncio.run(publish(parent_client, "child-with-parent-client"))
            except fanferro.ShutdownError as error:
                print("child refused the parent's client:", "before fork()" in str(error), flush=True)
            asyncio.run(publish(memory_client(), "child"))
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        print("child exit status", os.waitstatus_to_exitcode(status))
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "parent published",
        "child refused the parent's client: True",
        "child published",
        "child exit status 0",
    ], result.stdout + result.stderr
