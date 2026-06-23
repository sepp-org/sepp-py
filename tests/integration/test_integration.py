"""Integration tests against a real sepp server (testcontainers).

Requires Docker. Skipped gracefully if Docker is unavailable or the container
fails to start (mirrors the sepp-rs ``tests/integration.rs`` suite).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from sepp import (
    DuplicateHandlerError,
    EnqueueAck,
    EnqueueRequest,
    HandlerError,
    JobRejectedError,
    Payload,
    ReserveOptions,
    RetryDirective,
    SeppClient,
    Worker,
)
from sepp import errors as errs


@pytest.fixture
async def client(sepp_server: str) -> SeppClient:
    c = await SeppClient.connect(sepp_server)
    try:
        yield c
    finally:
        await c.close()


# -- connect ----------------------------------------------------------------


async def test_connect_and_get_server_info(client: SeppClient) -> None:
    info = await client.get_server_info()
    assert len(info.version) > 0
    assert len(info.supported_protocol_versions) > 0
    assert info.max_enqueue_batch > 0
    assert info.max_reserve_batch > 0


# -- enqueue single ---------------------------------------------------------


async def test_enqueue_single(client: SeppClient) -> None:
    ack = await client.enqueue(EnqueueRequest("q-enq-1", "test"))
    assert ack.job_id
    assert ack.deduplicated is False


async def test_enqueue_single_with_payload(client: SeppClient) -> None:
    ack = await client.enqueue(
        EnqueueRequest("q-enq-2", "test", payload=Payload(b"hello", "text/plain"))
    )
    assert ack.job_id


async def test_enqueue_rejection_invalid_queue(client: SeppClient) -> None:
    info = await client.get_server_info()
    if not info.strict_queues:
        pytest.skip("server not in strict mode")
    with pytest.raises(JobRejectedError) as ei:
        await client.enqueue(EnqueueRequest("nonexistent-queue-xyz", "test"))
    assert isinstance(ei.value.rejection, errs.UnknownQueue)


# -- enqueue batch ----------------------------------------------------------


async def test_enqueue_batch(client: SeppClient) -> None:
    results = await client.enqueue_batch(
        [
            EnqueueRequest("q-batch-1", "test"),
            EnqueueRequest("q-batch-1", "test"),
            EnqueueRequest("q-batch-1", "test"),
        ]
    )
    assert len(results) == 3
    for r in results:
        assert isinstance(r, EnqueueAck)  # type: ignore[arg-type]
        assert r.job_id  # type: ignore[union-attr]


# -- enqueue atomic ---------------------------------------------------------


async def test_enqueue_atomic(client: SeppClient) -> None:
    acks = await client.enqueue_atomic(
        [
            EnqueueRequest("q-atomic-1", "test"),
            EnqueueRequest("q-atomic-1", "test"),
        ]
    )
    assert len(acks) == 2
    assert acks[0].job_id != acks[1].job_id


# -- idempotency ------------------------------------------------------------


async def test_idempotency_same_key_returns_same_id(client: SeppClient) -> None:
    ack1 = await client.enqueue(EnqueueRequest("q-idem-1", "test", idempotency_key="key-42"))
    ack2 = await client.enqueue(EnqueueRequest("q-idem-1", "test", idempotency_key="key-42"))
    assert ack2.job_id == ack1.job_id
    assert ack2.deduplicated is True


# -- reserve empty ----------------------------------------------------------


async def test_reserve_empty_queue_returns_none(client: SeppClient) -> None:
    jobs = await client.reserve(
        ReserveOptions(["empty-q-xyz"], timedelta(seconds=1), wait_timeout=timedelta(seconds=2))
    )
    assert jobs is None


# -- enqueue -> reserve -> ack cycle ----------------------------------------


async def test_enqueue_reserve_ack(client: SeppClient) -> None:
    await client.enqueue(
        EnqueueRequest("q-cycle-1", "greet", payload=Payload(b"{}", "application/json"))
    )
    jobs = await client.reserve(
        ReserveOptions(["q-cycle-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=5))
    )
    assert jobs is not None and len(jobs) == 1
    job = jobs[0]
    assert job.ctx.job_type == "greet"
    assert job.ctx.attempt == 1
    assert job.payload is not None and job.payload.data == b"{}"
    await client.ack(job.ctx)


# -- nack with retry --------------------------------------------------------


async def test_nack_retry_then_ack(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-nack-1", "retry-me"))
    jobs = await client.reserve(
        ReserveOptions(["q-nack-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=5))
    )
    assert jobs is not None
    ctx = jobs[0].ctx
    assert ctx.attempt == 1

    dead = await client.nack(ctx, RetryDirective.DEFAULT, "transient fail")
    assert dead is False

    jobs2 = await client.reserve(
        ReserveOptions(["q-nack-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=5))
    )
    assert jobs2 is not None
    ctx2 = jobs2[0].ctx
    assert ctx2.id == ctx.id
    assert ctx2.attempt == 2
    await client.ack(ctx2)


# -- nack dead letter -------------------------------------------------------


async def test_nack_dead_letter(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-dl-1", "dl-me", max_attempts=1))
    jobs = await client.reserve(
        ReserveOptions(["q-dl-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=5))
    )
    assert jobs is not None
    ctx = jobs[0].ctx
    dead = await client.nack(ctx, RetryDirective.DEAD_LETTER, "permanent")
    assert dead is True

    jobs2 = await client.reserve(
        ReserveOptions(["q-dl-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=2))
    )
    assert jobs2 is None


# -- extend lease -----------------------------------------------------------


async def test_extend_lease(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-ext-1", "long-job"))
    jobs = await client.reserve(
        ReserveOptions(["q-ext-1"], timedelta(seconds=2), wait_timeout=timedelta(seconds=5))
    )
    assert jobs is not None
    ctx = jobs[0].ctx

    extended = await client.extend(ctx, timedelta(seconds=30))
    assert extended > datetime.now(timezone.utc) + timedelta(seconds=25)
    await client.ack(ctx)


# -- trace context propagation ----------------------------------------------


async def test_trace_context_roundtrip(client: SeppClient) -> None:
    from sepp import TraceContext

    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    await client.enqueue(EnqueueRequest("q-tc-1", "traced", trace_context=tc))
    jobs = await client.reserve(
        ReserveOptions(["q-tc-1"], timedelta(seconds=10), wait_timeout=timedelta(seconds=5))
    )
    assert jobs is not None
    ctx = jobs[0].ctx
    assert ctx.trace_context is not None
    assert ctx.trace_context.traceparent == tc.traceparent
    await client.ack(ctx)


# -- worker: process and ack ------------------------------------------------


async def test_worker_processes_and_acks(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-worker-1", "hello"))
    await client.enqueue(EnqueueRequest("q-worker-1", "hello"))

    processed: list[str] = []

    worker = Worker(client, ["q-worker-1"], timedelta(seconds=30), max_in_flight=4)
    shutdown = worker.shutdown_handle()

    @worker.handler("hello")
    async def handle(payload: object, ctx: object) -> None:
        processed.append(ctx.id)  # type: ignore[union-attr]
        if len(processed) == 2:
            shutdown.shutdown()

    await worker.run()
    assert len(processed) == 2


# -- worker: handler error nacks --------------------------------------------


async def test_worker_handler_error_nacks(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-werr-1", "failing", max_attempts=1))

    worker = Worker(client, ["q-werr-1"], timedelta(seconds=30))
    shutdown = worker.shutdown_handle()

    @worker.handler("failing")
    async def handle(payload: object, ctx: object) -> None:
        raise HandlerError.retry("expected")
        shutdown.shutdown()

    # Let it nack and exit; start a second worker for the reserve
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker.run(), timeout=3)


# -- worker: exception nacks ------------------------------------------------


async def test_worker_exception_nacks(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-wexc-1", "panicky", max_attempts=1))

    worker = Worker(client, ["q-wexc-1"], timedelta(seconds=30))

    @worker.handler("panicky")
    async def handle(payload: object, ctx: object) -> None:
        raise RuntimeError("boom")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker.run(), timeout=3)


# -- worker: catch-all handler ----------------------------------------------


async def test_worker_catch_all(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-catch-1", "unhandled_type"))

    seen: list[str] = []
    worker = Worker(client, ["q-catch-1"], timedelta(seconds=30))
    shutdown = worker.shutdown_handle()

    @worker.catch_all
    async def catch(payload: object, ctx: object) -> None:
        seen.append(ctx.job_type)  # type: ignore[union-attr]
        shutdown.shutdown()

    await worker.run()
    assert seen == ["unhandled_type"]


# -- worker: auto-extend ----------------------------------------------------


async def test_worker_auto_extend(client: SeppClient) -> None:
    await client.enqueue(EnqueueRequest("q-autoext-1", "slow"))

    worker = Worker(
        client,
        ["q-autoext-1"],
        timedelta(milliseconds=500),
        auto_extend=True,
        auto_extend_interval=timedelta(milliseconds=100),
    )
    shutdown = worker.shutdown_handle()

    @worker.handler("slow")
    async def handle(payload: object, ctx: object) -> None:
        await asyncio.sleep(0.6)
        shutdown.shutdown()

    await worker.run()


# -- worker: duplicate handler ----------------------------------------------


async def test_worker_duplicate_handler_raises(client: SeppClient) -> None:
    worker = Worker(client, ["q-dup-1"], timedelta(seconds=30))

    async def h1(payload: object, ctx: object) -> None:
        pass

    async def h2(payload: object, ctx: object) -> None:
        pass

    worker.handle("dup", h1)  # type: ignore[arg-type]
    with pytest.raises(DuplicateHandlerError):
        worker.handle("dup", h2)  # type: ignore[arg-type]


# -- worker: no handler nacks -----------------------------------------------


async def test_worker_parallel_jobs(client: SeppClient) -> None:
    for _ in range(5):
        await client.enqueue(EnqueueRequest("q-par-1", "incr"))

    count = 0
    done = asyncio.Event()

    worker = Worker(client, ["q-par-1"], timedelta(seconds=30), max_in_flight=3)

    @worker.handler("incr")
    async def handle(payload: object, ctx: object) -> None:
        nonlocal count
        count += 1
        if count == 5:
            done.set()

    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(done.wait(), timeout=10)
    finally:
        worker.shutdown_handle().shutdown()
        await asyncio.wait_for(task, 5)

    assert count == 5
