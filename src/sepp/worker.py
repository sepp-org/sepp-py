"""A high-level worker that runs the reserve -> process -> ack/nack loop.

:class:`Worker` wraps a :class:`~sepp.client.SeppClient` and drives job
consumption for you: reserve jobs, dispatch each to the handler registered for
its ``job_type``, and ack on success or nack on failure. It adds bounded
concurrency, optional lease auto-extension, graceful shutdown via a
:class:`ShutdownHandle`, and (with the ``otel`` extra) metrics and trace
linkage.

A handler is an ``async def handler(payload, ctx) -> None``. Returning normally
acks the job. Raising a :class:`HandlerError` nacks it with the chosen
:class:`~sepp.client.RetryDirective`; raising any other exception nacks it for
retry::

    worker = Worker(client, ["emails"], timedelta(seconds=30), max_in_flight=32)

    @worker.handler("send_welcome")
    async def send_welcome(payload, ctx):
        ...  # returning acks; raising HandlerError.retry(...) nacks

    await worker.run()
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from sepp import _convert, _otel, errors
from sepp.client import RetryDirective, SeppClient
from sepp.types import Job, JobCtx, Payload, ReserveOptions

__all__ = ["Worker", "HandlerError", "ShutdownHandle", "DuplicateHandlerError"]

logger = _otel.create_sepp_logger("sepp")

Handler = Callable[[Payload | None, JobCtx], Awaitable[None]]

# Sentinel returned when a handler is aborted because its lease was lost — the
# job must not be acked or nacked (another worker now owns it).
_LEASE_LOST = object()

# An empty reserve that returns well before its requested wait timeout (e.g. a
# server draining at shutdown answers empty immediately) is backed off briefly
# so the poll loop does not hammer the server.
_EARLY_EMPTY_THRESHOLD = timedelta(milliseconds=100)
_EARLY_EMPTY_BACKOFF = timedelta(milliseconds=250)


class HandlerError(Exception):
    """Raised from a handler to nack its job, choosing how it should be retried.

    Use the :meth:`retry`, :meth:`retry_after`, and :meth:`permanent`
    constructors rather than instantiating directly.
    """

    def __init__(self, reason: str, directive: RetryDirective) -> None:
        self.reason = reason
        self.directive = directive
        super().__init__(reason)

    @classmethod
    def retry(cls, reason: str) -> HandlerError:
        """Nack the job for retry under the queue's default policy."""
        return cls(reason, RetryDirective.DEFAULT)

    @classmethod
    def retry_after(cls, reason: str, delay: timedelta) -> HandlerError:
        """Nack the job for retry after at least ``delay``."""
        return cls(reason, RetryDirective.after(delay))

    @classmethod
    def permanent(cls, reason: str) -> HandlerError:
        """Nack the job as a permanent failure, dead-lettering it immediately."""
        return cls(reason, RetryDirective.dead_letter())


class DuplicateHandlerError(errors.SeppError):
    """:meth:`Worker.handle` was called twice for the same job type."""

    def __init__(self, job_type: str) -> None:
        super().__init__(f"handler for job_type {job_type!r} is already registered")


class ShutdownHandle:
    """A handle for triggering a :class:`Worker`'s graceful shutdown.

    Obtain one from :meth:`Worker.shutdown_handle`. Calling :meth:`shutdown`
    stops new reservations; :meth:`Worker.run` then waits for in-flight jobs to
    finish before returning. The handle is shared, so you can hand it to a
    signal handler.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def shutdown(self) -> None:
        """Signal the worker to stop reserving new jobs and begin draining."""
        self._event.set()

    @property
    def is_shutdown(self) -> bool:
        """Whether shutdown has been signalled."""
        return self._event.is_set()

    async def _wait(self) -> None:
        await self._event.wait()


@dataclass(frozen=True)
class _AutoExtend:
    # None = derive the interval from the granted lease each cycle (default); set
    # = the caller's explicit interval. The default must track the GRANTED lease,
    # not the requested one: if the server clamps the lease below the request, a
    # requested-lease/3 interval fires only after the granted lease has expired,
    # so the job is redelivered and runs twice.
    explicit_interval: timedelta | None
    extend_by: timedelta


class Worker:
    """A job-processing loop built on a :class:`~sepp.client.SeppClient`.

    Configure it with the constructor, register one handler per job type with
    :meth:`handle` (or the :meth:`handler` decorator), and start it with
    :meth:`run`. ``run`` returns only after a :class:`ShutdownHandle` is
    triggered and in-flight jobs have drained.

    Each reserved job runs as its own task, bounded by ``max_in_flight``. A job
    whose ``job_type`` has no registered handler is nacked for retry.

    With ``auto_extend`` on, a heartbeat keeps each in-flight job's lease alive
    while its handler runs. Use with caution: a handler that hangs indefinitely
    keeps its lease extended forever, so its job is never redelivered. Bound
    hang-prone work yourself, e.g. with :func:`asyncio.timeout`.
    """

    def __init__(
        self,
        client: SeppClient,
        queues: Iterable[str],
        lease_duration: timedelta,
        *,
        wait_timeout: timedelta | None = None,
        max_jobs: int | None = None,
        max_in_flight: int = 16,
        reserve_error_backoff: timedelta = timedelta(seconds=1),
        auto_extend: bool = False,
        auto_extend_interval: timedelta | None = None,
        worker_id: str | None = None,
    ) -> None:
        resolved_worker_id = _default_worker_id() if worker_id is None else worker_id
        opts_kwargs: dict[str, object] = {"worker_id": resolved_worker_id, "max_jobs": max_jobs}

        if wait_timeout is not None:
            if wait_timeout <= timedelta(0):
                raise ValueError("wait_timeout must be at least 1ms")
            opts_kwargs["wait_timeout"] = wait_timeout
        self._opts = ReserveOptions(list(queues), lease_duration, **opts_kwargs)  # type: ignore[arg-type]
        self._client = client
        self._handlers: dict[str, Handler] = {}
        self._catch_all_handler: Handler | None = None
        self._max_in_flight = max(1, max_in_flight)
        self._reserve_error_backoff = reserve_error_backoff

        if auto_extend or auto_extend_interval is not None:
            explicit = (
                max(auto_extend_interval, timedelta(milliseconds=1))
                if auto_extend_interval is not None
                else None
            )
            self._auto_extend: _AutoExtend | None = _AutoExtend(
                explicit_interval=explicit,
                extend_by=lease_duration,
            )
        else:
            self._auto_extend = None

        self._shutdown = ShutdownHandle()
        self._metrics = _otel.WorkerMetrics()
        self._in_flight = 0

    def handle(self, job_type: str, handler: Handler) -> Worker:
        """Register the handler for ``job_type``. Raises
        :class:`DuplicateHandlerError` if one is already registered."""
        if job_type in self._handlers:
            raise DuplicateHandlerError(job_type)
        self._handlers[job_type] = handler
        return self

    def replace_handler(self, job_type: str, handler: Handler) -> Worker:
        """Register a handler, overwriting any existing one for the same
        ``job_type`` instead of erroring."""
        self._handlers[job_type] = handler
        return self

    def remove_handler(self, job_type: str) -> Worker:
        """Unregister the handler for ``job_type``, if any."""
        self._handlers.pop(job_type, None)
        return self

    def catch_all(self, handler: Handler) -> Handler:
        """Register a catch-all handler for job types without a specific
        handler, overwriting any previously registered one. Returns the
        handler, so it doubles as a decorator::

            @worker.catch_all
            async def fallback(payload, ctx): ...
        """
        self._catch_all_handler = handler
        return handler

    def handler(self, job_type: str) -> Callable[[Handler], Handler]:
        """Decorator form of :meth:`handle`::

        @worker.handler("send_welcome")
        async def send_welcome(payload, ctx): ...
        """

        def decorator(fn: Handler) -> Handler:
            self.handle(job_type, fn)
            return fn

        return decorator

    def shutdown_handle(self) -> ShutdownHandle:
        """Return the :class:`ShutdownHandle` for stopping the worker."""
        return self._shutdown

    async def run(self) -> None:
        """Run the reserve -> process -> ack/nack loop until shutdown.

        Does not return until the :class:`ShutdownHandle` is triggered *and* all
        in-flight jobs have finished draining. Reserve errors are logged and
        retried after ``reserve_error_backoff``; they do not stop the loop.

        The drain wait is unbounded: a handler that never returns blocks the
        return of ``run()`` indefinitely (and with ``auto_extend`` its lease is
        kept alive the whole time) — see the class note on bounding hang-prone
        handlers.

        Cancelling ``run()`` is safe but not graceful: the pending reserve and
        any in-flight job tasks are cancelled, abandoning their jobs to lease
        expiry. Prefer the :class:`ShutdownHandle` for a graceful stop.
        """
        sem = asyncio.Semaphore(self._max_in_flight)
        tasks: set[asyncio.Task[None]] = set()
        shutdown = self._shutdown
        logger.info(
            "worker started: worker_id=%s max_in_flight=%d handlers=%d auto_extend=%s",
            self._opts.worker_id,
            self._max_in_flight,
            len(self._handlers),
            self._auto_extend is not None,
        )

        try:
            while not shutdown.is_shutdown:
                # Acquire one permit (the reservation budget), racing shutdown.
                _, shutdown_won = await self._or_shutdown(sem.acquire())
                if shutdown_won:
                    break

                opts = self._reserve_opts_with_capacity()
                reserve_started = time.monotonic()
                try:
                    result, shutdown_won = await self._or_shutdown(self._client.reserve(opts))
                except errors.SeppError as err:
                    self._metrics.record_reserve_failed()
                    logger.warning(
                        "reserve error: %s; backing off for %s", err, self._reserve_error_backoff
                    )
                    sem.release()
                    _, shutdown_won = await self._or_shutdown(
                        asyncio.sleep(self._reserve_error_backoff.total_seconds())
                    )
                    if shutdown_won:
                        break
                    continue

                if shutdown_won:
                    sem.release()
                    break

                jobs = cast("list[Job] | None", result)
                if jobs is None:
                    self._metrics.record_reserve_ok(empty=True)
                    sem.release()
                    # See _EARLY_EMPTY_THRESHOLD: an empty answer well short of
                    # the wait window gets a brief, shutdown-aware backoff.
                    elapsed = timedelta(seconds=time.monotonic() - reserve_started)
                    if elapsed < min(_EARLY_EMPTY_THRESHOLD, opts.wait_timeout / 2):
                        _, shutdown_won = await self._or_shutdown(
                            asyncio.sleep(_EARLY_EMPTY_BACKOFF.total_seconds())
                        )
                        if shutdown_won:
                            break
                    continue

                self._metrics.record_reserve_ok(empty=False)
                # The first job inherits the permit already held; the rest acquire
                # their own (which also throttles how fast we drain the batch).
                self._spawn(tasks, sem, jobs[0])
                for job in jobs[1:]:
                    await sem.acquire()
                    self._spawn(tasks, sem, job)
        except asyncio.CancelledError:
            # Cancelled from outside (the reserve was already reaped by
            # _or_shutdown): reap the in-flight job tasks too, abandoning their
            # jobs to lease expiry, rather than leaking the tasks.
            for t in list(tasks):
                t.cancel()
            if tasks:
                await asyncio.gather(*list(tasks), return_exceptions=True)
            raise

        logger.info("worker shutting down; waiting for in-flight jobs to finish")
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("worker stopped")

    def _reserve_opts_with_capacity(self) -> ReserveOptions:
        # The worker already bounds in-flight jobs, so never ask for more than
        # the free capacity.
        capacity = max(1, self._max_in_flight - self._in_flight)
        max_jobs = capacity
        if self._opts.max_jobs is not None:
            max_jobs = min(self._opts.max_jobs, capacity)
        return dataclasses.replace(self._opts, max_jobs=max_jobs)

    def _spawn(self, tasks: set[asyncio.Task[None]], sem: asyncio.Semaphore, job: Job) -> None:
        self._in_flight += 1
        self._metrics.record_in_flight_delta(1)
        task: asyncio.Task[None] = asyncio.create_task(self._process_job(job))
        tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            tasks.discard(t)
            sem.release()
            self._in_flight -= 1
            self._metrics.record_in_flight_delta(-1)
            if not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    logger.error("job task crashed unexpectedly: %r", exc)

        task.add_done_callback(_done)

    async def _or_shutdown(self, aw: Awaitable[object]) -> tuple[object, bool]:
        """Await ``aw`` while racing the shutdown signal.

        Returns ``(result, False)`` if ``aw`` finished first (propagating its
        exception), or ``(None, True)`` if shutdown won (``aw`` is cancelled).

        If both complete in the same step, the awaited result wins. This is
        deliberate: a reserve that returned jobs just as shutdown fired gets
        processed rather than abandoned to lease expiry."""
        task = asyncio.ensure_future(aw)
        sd = asyncio.ensure_future(self._shutdown._wait())
        # Reaping a child below must not use `suppress(CancelledError): await
        # child`: if run() itself is cancelled while parked on that await, its
        # own CancelledError surfaces there and would be suppressed too,
        # leaving the worker running after a cancel. gather(...,
        # return_exceptions=True) absorbs only the child's cancellation and
        # still propagates the caller's.
        try:
            done, _ = await asyncio.wait({task, sd}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            # The caller itself was cancelled. asyncio.wait leaves its children
            # running, so reap both before propagating.
            task.cancel()
            sd.cancel()
            await asyncio.gather(task, sd, return_exceptions=True)
            raise

        if sd in done and task not in done:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return None, True

        if not sd.done():
            sd.cancel()
            await asyncio.gather(sd, return_exceptions=True)
        return task.result(), False

    async def _process_job(self, job: Job) -> None:
        with _otel.consumer_span("sepp.process", job.ctx.trace_context):
            await self._run_job(job)

    async def _run_job(self, job: Job) -> None:
        ctx = job.ctx
        handler = self._handlers.get(ctx.job_type) or self._catch_all_handler
        if handler is None:
            logger.warning("no handler registered for job_type `%s`", ctx.job_type)
            # Best-effort nack: if it fails, the job is redelivered at lease expiry.
            with contextlib.suppress(errors.SeppError):
                await self._client.nack(
                    ctx, RetryDirective.DEFAULT, "no handler registered for job_type"
                )
            return

        outcome: object
        if self._auto_extend is None:
            outcome = await self._run_plain(handler, job)
        else:
            outcome = await self._run_with_auto_extend(handler, job)

        if outcome is _LEASE_LOST:
            return
        await self._dispose(ctx, cast("Exception | None", outcome))

    async def _run_plain(self, handler: Handler, job: Job) -> Exception | None:
        try:
            await handler(job.payload, job.ctx)
            return None
        except HandlerError as err:
            return err
        except Exception as err:  # noqa: BLE001 - any handler failure nacks the job
            return err

    async def _run_with_auto_extend(self, handler: Handler, job: Job) -> object:
        assert self._auto_extend is not None
        ctx = job.ctx
        assert ctx._lease is not None
        # asyncio cancellation is cooperative — a handler could swallow the
        # CancelledError we inject on lease loss. So the heartbeat records its
        # decision in this flag, and it (not how the handler resolved) is
        # authoritative.
        lease_lost = [False]
        handler_task = asyncio.ensure_future(handler(job.payload, ctx))
        hb_task = asyncio.ensure_future(
            self._heartbeat(ctx._lease, self._auto_extend, handler_task, lease_lost)
        )
        outcome: object
        try:
            await handler_task
            outcome = None
        except asyncio.CancelledError:
            if not lease_lost[0]:
                # A genuine cancellation from elsewhere (not our heartbeat) must
                # not be masked — propagate it, as the no-auto-extend path does.
                raise
            outcome = _LEASE_LOST
        except HandlerError as err:
            outcome = err
        except Exception as err:  # noqa: BLE001 - any handler failure nacks the job
            outcome = err
        finally:
            hb_task.cancel()
            # gather, not suppress(CancelledError): see _or_shutdown.
            await asyncio.gather(hb_task, return_exceptions=True)

        # The heartbeat's verdict wins even if the handler completed or swallowed
        # the cancellation: a job whose lease was reassigned must not be acked or
        # nacked, since another worker now owns it.
        if lease_lost[0]:
            outcome = _LEASE_LOST
        if outcome is _LEASE_LOST:
            logger.error("lease lost; handler aborted")
        return outcome

    async def _dispose(self, ctx: JobCtx, error: Exception | None) -> None:
        try:
            if error is None:
                logger.debug("job completed; acking")
                await self._client.ack(ctx)
                self._metrics.record_processed()
            elif isinstance(error, HandlerError):
                logger.warning("handler returned error; nacking: %s", error)
                dead_lettered = await self._client.nack(ctx, error.directive, error.reason)
                self._metrics.record_nacked(dead_lettered)
            else:
                logger.error("handler raised %s; nacking", type(error).__name__)
                dead_lettered = await self._client.nack(
                    ctx, RetryDirective.DEFAULT, f"handler raised: {error}"
                )
                self._metrics.record_nacked(dead_lettered)
        except errors.JobNotFoundError as err:
            logger.error(
                "ack/nack returned JobNotFound: either the lease was lost and the job will be"
                " redelivered, or an earlier attempt of this call succeeded and only the"
                " response was lost: %s",
                err,
            )
        except errors.SeppError as err:
            logger.error(
                "failed to ack/nack job; it will be redelivered after lease expiry: %s", err
            )

    async def _heartbeat(
        self,
        lease: object,
        cfg: _AutoExtend,
        handler_task: asyncio.Task[None],
        lease_lost: list[bool],
    ) -> None:
        # `lease` is a sepp.client.Lease; typed loosely to avoid a cycle. The
        # `lease_lost` box is set before aborting so the caller's decision is
        # authoritative regardless of whether the handler honors cancellation.
        while True:
            # Derive from the granted lease; see _AutoExtend.explicit_interval.
            if cfg.explicit_interval is not None:
                interval = cfg.explicit_interval
            else:
                remaining_ms = max(0, lease.known_expiry_ms - _convert.now_millis())  # type: ignore[attr-defined]
                interval = _heartbeat_interval(timedelta(milliseconds=remaining_ms))
            await asyncio.sleep(interval.total_seconds())
            try:
                expiry = await lease.extend(cfg.extend_by)  # type: ignore[attr-defined]
                logger.debug("lease extended to %s", expiry)
            except (errors.AttemptMismatchError, errors.JobNotFoundError) as err:
                logger.error(
                    "lease reassigned by server (%s); aborting handler to avoid double processing",
                    err,
                )
                lease_lost[0] = True
                handler_task.cancel()
                return
            except errors.SeppError as err:
                if _convert.now_millis() >= lease.known_expiry_ms:  # type: ignore[attr-defined]
                    logger.error(
                        "lease lost (%s); aborting handler to avoid double processing", err
                    )
                    lease_lost[0] = True
                    handler_task.cancel()
                    return
                logger.warning("lease extend failed (%s); lease still valid, will retry", err)


def _heartbeat_interval(lease: timedelta) -> timedelta:
    return max(lease / 3, timedelta(milliseconds=1))


def _default_worker_id() -> str:
    try:
        host = socket.gethostname() or "unknown"
    except OSError:
        host = "unknown"
    return f"{host}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
