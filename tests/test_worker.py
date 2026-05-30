"""Integration tests for the Worker run loop, against an in-memory fake client."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from tests.conftest import VALID_UUID

from sepp import _convert, errors
from sepp._pb import queue_pb2 as pb
from sepp.client import RetryDirective
from sepp.types import Job
from sepp.worker import (
    DuplicateHandlerError,
    HandlerError,
    Worker,
    _default_worker_id,
    _heartbeat_interval,
)


class FakeClient:
    """A SeppClient stand-in for driving the worker without a network."""

    def __init__(
        self, batches: list[list[Job] | None], extend_error: Exception | None = None
    ) -> None:
        self._batches = list(batches)
        self.acked: list[str] = []
        self.nacked: list[tuple[str, RetryDirective, str]] = []
        self.extends = 0
        self._extend_error = extend_error

    async def reserve(self, opts: object) -> list[Job] | None:
        if self._batches:
            return self._batches.pop(0)
        await asyncio.sleep(0.005)  # idle: yield without busy-spinning
        return None

    async def ack(self, ctx: object) -> None:
        self.acked.append(ctx.id)  # type: ignore[attr-defined]

    async def nack(
        self, ctx: object, retry: RetryDirective = RetryDirective.DEFAULT, reason: str = ""
    ) -> bool:
        self.nacked.append((ctx.id, retry, reason))  # type: ignore[attr-defined]
        return retry.kind == "dead_letter"

    async def _extend_inner(
        self, job_id: str, attempt: int, extension: timedelta, worker_id: str | None
    ) -> datetime:
        self.extends += 1
        if self._extend_error is not None:
            raise self._extend_error
        return datetime.now(timezone.utc) + extension


def make_job(client: object, job_type: str, job_id: str = VALID_UUID) -> Job:
    j = pb.Job(
        id=job_id,
        job_type=job_type,
        priority=1,
        enqueued_at=1_700_000_000_000,
        attempt=1,
        max_attempts=3,
        lease_expires_at=_convert.now_millis() + 60_000,
    )
    return _convert.job_from_pb(client, j, "worker-1")  # type: ignore[arg-type]


async def drive(worker: Worker, ready: asyncio.Event, timeout: float = 2.0) -> None:
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout)
    finally:
        worker.shutdown_handle().shutdown()
        await asyncio.wait_for(task, timeout)


# -- dispatch outcomes ------------------------------------------------------


async def test_processes_and_acks() -> None:
    client = FakeClient([[None]])  # placeholder, replaced below
    job = make_job(client, "t")
    client._batches = [[job]]
    ready = asyncio.Event()
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]

    @worker.handler("t")
    async def handle(payload, ctx):
        ready.set()

    await drive(worker, ready)
    assert client.acked == [VALID_UUID]
    assert client.nacked == []


async def test_handler_error_nacks_retry() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "t")]]
    ready = asyncio.Event()
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]

    @worker.handler("t")
    async def handle(payload, ctx):
        ready.set()
        raise HandlerError.retry("nope")

    await drive(worker, ready)
    assert client.acked == []
    assert len(client.nacked) == 1
    job_id, directive, reason = client.nacked[0]
    assert directive.kind == "default" and reason == "nope"


async def test_handler_permanent_dead_letters() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "t")]]
    ready = asyncio.Event()
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]

    @worker.handler("t")
    async def handle(payload, ctx):
        ready.set()
        raise HandlerError.permanent("bad input")

    await drive(worker, ready)
    assert client.nacked[0][1].kind == "dead_letter"


async def test_handler_exception_nacks() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "t")]]
    ready = asyncio.Event()
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]

    @worker.handler("t")
    async def handle(payload, ctx):
        ready.set()
        raise ValueError("kaboom")

    await drive(worker, ready)
    assert client.acked == []
    assert len(client.nacked) == 1
    assert client.nacked[0][1].kind == "default"
    assert "handler raised" in client.nacked[0][2]


async def test_no_handler_nacks() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "unhandled")]]
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]
    worker.handle("other", lambda payload, ctx: asyncio.sleep(0))

    # No handler for "unhandled": drive until a nack is recorded.
    task = asyncio.create_task(worker.run())
    for _ in range(200):
        if client.nacked:
            break
        await asyncio.sleep(0.005)
    worker.shutdown_handle().shutdown()
    await asyncio.wait_for(task, 2.0)
    assert client.acked == []
    assert client.nacked and client.nacked[0][0] == VALID_UUID


async def test_catch_all_handles_unregistered_type() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "unhandled")]]
    ready = asyncio.Event()
    seen: list[str] = []
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]
    worker.handle("other", lambda payload, ctx: asyncio.sleep(0))

    async def catch_all(payload, ctx):
        seen.append(ctx.job_type)
        ready.set()

    worker.catch_all(catch_all)

    await drive(worker, ready)
    assert seen == ["unhandled"]
    assert client.acked == [VALID_UUID]
    assert client.nacked == []


# -- registration -----------------------------------------------------------


async def test_duplicate_handler_raises() -> None:
    client = FakeClient([])
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]
    worker.handle("t", lambda payload, ctx: asyncio.sleep(0))
    with pytest.raises(DuplicateHandlerError):
        worker.handle("t", lambda payload, ctx: asyncio.sleep(0))


async def test_replace_handler() -> None:
    client = FakeClient([])
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]
    worker.handle("t", lambda payload, ctx: asyncio.sleep(0))
    worker.replace_handler("t", lambda payload, ctx: asyncio.sleep(0))  # no raise


# -- graceful shutdown ------------------------------------------------------


async def test_graceful_shutdown_drains_in_flight() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "t")]]
    started = asyncio.Event()
    release = asyncio.Event()
    worker = Worker(client, ["q"], timedelta(seconds=30))  # type: ignore[arg-type]

    @worker.handler("t")
    async def handle(payload, ctx):
        started.set()
        await release.wait()  # stay in-flight until released

    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 2.0)
    worker.shutdown_handle().shutdown()  # shut down while the job is in flight
    await asyncio.sleep(0.02)
    assert not task.done()  # run must wait for the in-flight job to drain
    release.set()
    await asyncio.wait_for(task, 2.0)
    assert client.acked == [VALID_UUID]  # the job was acked during drain


# -- auto-extend ------------------------------------------------------------


async def test_auto_extend_renews_lease() -> None:
    client = FakeClient([])
    client._batches = [[make_job(client, "t")]]
    ready = asyncio.Event()
    worker = Worker(
        client,  # type: ignore[arg-type]
        ["q"],
        timedelta(milliseconds=60),
        auto_extend=True,
        auto_extend_interval=timedelta(milliseconds=10),
    )

    @worker.handler("t")
    async def handle(payload, ctx):
        await asyncio.sleep(0.05)  # longer than the heartbeat interval
        ready.set()

    await drive(worker, ready)
    assert client.extends >= 1
    assert client.acked == [VALID_UUID]


async def test_auto_extend_abort_on_lease_lost() -> None:
    client = FakeClient([], extend_error=errors.AttemptMismatchError())
    client._batches = [[make_job(client, "t")]]
    aborted = asyncio.Event()
    worker = Worker(
        client,  # type: ignore[arg-type]
        ["q"],
        timedelta(milliseconds=30),
        auto_extend=True,
        auto_extend_interval=timedelta(milliseconds=10),
    )

    @worker.handler("t")
    async def handle(payload, ctx):
        try:
            await asyncio.sleep(1.0)  # would outlast the lease; heartbeat aborts us
        except asyncio.CancelledError:
            aborted.set()
            raise

    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(aborted.wait(), 2.0)
    worker.shutdown_handle().shutdown()
    await asyncio.wait_for(task, 2.0)
    # Lease was lost: the job is neither acked nor nacked (another worker owns it).
    assert client.acked == []
    assert client.nacked == []


async def test_auto_extend_lease_lost_authoritative_when_handler_swallows_cancel() -> None:
    # A handler that swallows the cancellation must STILL not get its job acked
    # once the lease is lost — the heartbeat's verdict is authoritative.
    client = FakeClient([], extend_error=errors.AttemptMismatchError())
    client._batches = [[make_job(client, "t")]]
    cancelled_seen = asyncio.Event()
    worker = Worker(
        client,  # type: ignore[arg-type]
        ["q"],
        timedelta(milliseconds=30),
        auto_extend=True,
        auto_extend_interval=timedelta(milliseconds=10),
    )

    @worker.handler("t")
    async def handle(payload, ctx):
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            cancelled_seen.set()
            return  # swallow the cancellation and return normally (anti-pattern)

    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(cancelled_seen.wait(), 2.0)
    worker.shutdown_handle().shutdown()
    await asyncio.wait_for(task, 2.0)
    assert client.acked == []
    assert client.nacked == []


# -- helpers ----------------------------------------------------------------


def test_heartbeat_interval_is_third_of_lease() -> None:
    assert _heartbeat_interval(timedelta(seconds=3)) == timedelta(seconds=1)
    assert _heartbeat_interval(timedelta(seconds=9)) == timedelta(seconds=3)


def test_heartbeat_interval_floor() -> None:
    assert _heartbeat_interval(timedelta(0)) == timedelta(milliseconds=1)


def test_default_worker_id_format() -> None:
    wid = _default_worker_id()
    parts = wid.rsplit("-", 2)
    assert len(parts) == 3
    assert parts[1].isdigit()  # pid
    assert len(parts[2]) == 8  # random hex suffix
