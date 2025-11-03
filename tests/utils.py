"""Utils for tests."""

import asyncio
import contextlib
import time

from haolib.entrypoints import HAO
from haolib.entrypoints.abstract import AbstractEntrypoint


def ensure_successful_run(hao: HAO, entrypoints: list[AbstractEntrypoint]) -> None:
    """Ensure successful run of the entrypoints."""
    loop = asyncio.new_event_loop()

    task = loop.create_task(hao.run_entrypoints(entrypoints))

    time.sleep(0.1)

    task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        loop.run_until_complete(task)
