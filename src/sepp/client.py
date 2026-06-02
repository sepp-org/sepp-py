"""The gRPC client: connecting, enqueuing, reserving, and lease management.

:class:`SeppClient` is an async handle to a sepp server, built on
``grpc.aio``. Create one with :meth:`SeppClient.connect` for the common case
(it also accepts API-key auth, TLS, and a :class:`RetryPolicy`), or wrap an
existing channel with :meth:`SeppClient.from_channel`.

For consuming jobs you can call :meth:`reserve`, :meth:`ack`, :meth:`nack`, and
:meth:`extend` directly, or hand the client to a
:class:`~sepp.worker.Worker` and let it drive that loop.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar, TypeVar

import grpc
import grpc.aio

from sepp import _convert, _otel, errors
from sepp._pb import queue_pb2 as pb
from sepp._pb import queue_pb2_grpc as pb_grpc
from sepp.types import (
    DeadLetterRecord,
    EnqueueAck,
    EnqueueRequest,
    Job,
    JobCtx,
    ReserveOptions,
    ServerInfo,
)

__all__ = ["SeppClient", "RetryPolicy", "RetryDirective", "Lease"]

logger = logging.getLogger("sepp")

_T = TypeVar("_T")

# Extra deadline slack added on top of a reserve's long-poll wait, so the RPC
# deadline never fires before the server's own wait window elapses.
_RESERVE_DEADLINE_SLACK = timedelta(seconds=10)
_DEFAULT_CONNECT_TIMEOUT = timedelta(seconds=5)

try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        _VERSION = version("sepp")
    except PackageNotFoundError:
        _VERSION = "0.1.0"
except ImportError:  # pragma: no cover
    _VERSION = "0.1.0"

_USER_AGENT = f"sepp-py/{_VERSION}"

_CHANNEL_OPTIONS = [
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.primary_user_agent", _USER_AGENT),
]


@dataclass
class RetryPolicy:
    """Backoff policy for retrying *transient* RPC failures (those mapped to
    :class:`~sepp.errors.TransportError` / :class:`~sepp.errors.OverloadedError`).

    Applies to enqueue, ack, nack, extend, and get-server-info — but not to
    :meth:`SeppClient.reserve`, which is a long poll. The default policy
    performs **no** retries (``max_attempts == 1``); opt in by passing a
    configured policy to :meth:`SeppClient.connect`.
    """

    max_attempts: int = 1
    initial_backoff: timedelta = timedelta(milliseconds=100)
    max_backoff: timedelta = timedelta(seconds=10)
    multiplier: float = 2.0
    jitter: bool = True

    def __post_init__(self) -> None:
        # Values below the floor are clamped rather than rejected.
        self.max_attempts = max(1, self.max_attempts)
        self.multiplier = max(1.0, self.multiplier)


@dataclass(frozen=True)
class RetryDirective:
    """How the server should handle a :meth:`SeppClient.nack`ed job's next
    delivery.

    This is *job-level* retry (the handler failed), distinct from the
    connection-level :class:`RetryPolicy` (the RPC failed). Construct one with
    :meth:`after`, or use the :attr:`DEFAULT` / :attr:`DEAD_LETTER` constants.
    """

    kind: str
    delay: timedelta | None = None

    # Assigned after the class body; declared here so type checkers see them.
    DEFAULT: ClassVar[RetryDirective]
    DEAD_LETTER: ClassVar[RetryDirective]

    @classmethod
    def default(cls) -> RetryDirective:
        """Apply the queue's configured retry policy (backoff, max attempts)."""
        return cls("default")

    @classmethod
    def after(cls, delay: timedelta) -> RetryDirective:
        """Retry, but not before ``delay`` has elapsed."""
        return cls("after", delay)

    @classmethod
    def dead_letter(cls) -> RetryDirective:
        """Do not retry; send the job straight to the dead-letter queue."""
        return cls("dead_letter")


RetryDirective.DEFAULT = RetryDirective.default()
RetryDirective.DEAD_LETTER = RetryDirective.dead_letter()


class Lease:
    """Internal handle to a reserved job's lease, used to renew it.

    Attached to a :class:`~sepp.types.JobCtx` by the conversion layer; not
    constructed directly.
    """

    def __init__(
        self,
        client: SeppClient,
        job_id: str,
        attempt: int,
        lease_expires_at: datetime,
        worker_id: str | None,
    ) -> None:
        self._client = client
        self._job_id = job_id
        self._attempt = attempt
        self._worker_id = worker_id
        self._known_expiry_ms = _convert.datetime_to_millis(lease_expires_at)

    @property
    def worker_id(self) -> str | None:
        return self._worker_id

    @property
    def known_expiry_ms(self) -> int:
        return self._known_expiry_ms

    async def extend(self, by: timedelta) -> datetime:
        new_expiry = await self._client._extend_inner(
            self._job_id, self._attempt, by, self._worker_id
        )
        self._known_expiry_ms = _convert.datetime_to_millis(new_expiry)
        return new_expiry


class SeppClient:
    """An async handle to a sepp server.

    Every RPC method is a coroutine. The client owns a single ``grpc.aio``
    channel; close it with :meth:`close` or use the client as an async context
    manager.
    """

    def __init__(
        self,
        channel: grpc.aio.Channel,
        retry_policy: RetryPolicy | None = None,
        auth_metadata: Iterable[tuple[str, str]] | None = None,
    ) -> None:
        self._channel = channel
        self._stub = pb_grpc.QueueServiceStub(channel)
        self._retry_policy = retry_policy or RetryPolicy()
        self._auth_metadata: list[tuple[str, str]] = list(auth_metadata or [])

    @classmethod
    async def connect(
        cls,
        addr: str,
        *,
        api_key: str | None = None,
        retry_policy: RetryPolicy | None = None,
        tls: bool = False,
        tls_ca_cert: bytes | None = None,
        tls_domain: str | None = None,
        credentials: grpc.ChannelCredentials | None = None,
        connect_timeout: timedelta = _DEFAULT_CONNECT_TIMEOUT,
    ) -> SeppClient:
        """Connect to a sepp server and wait until the channel is ready.

        ``addr`` is a target such as ``"127.0.0.1:50051"``; an ``http://`` or
        ``https://`` scheme is accepted and stripped (``https://`` implies TLS).
        Pass ``api_key`` for ``Authorization: Bearer`` auth, and enable TLS with
        ``tls=True`` (system roots), ``tls_ca_cert`` (a PEM bundle for a private
        CA), ``tls_domain`` (override the verified name), or a full
        ``credentials`` object.

        Raises :class:`~sepp.errors.ConnectError` if the connection is not ready
        within ``connect_timeout``, or :class:`~sepp.errors.InvalidApiKeyError`
        if ``api_key`` cannot form an HTTP header value.
        """
        import asyncio

        target, scheme_is_tls = _normalize_target(addr)
        use_tls = tls or scheme_is_tls or tls_ca_cert is not None or tls_domain is not None
        auth_metadata = _auth_metadata_for(api_key)

        if auth_metadata and not (use_tls or credentials is not None):
            logger.warning(
                "API key configured without TLS; it will be sent over the connection in plaintext"
            )

        options = list(_CHANNEL_OPTIONS)
        if tls_domain is not None:
            options.append(("grpc.ssl_target_name_override", tls_domain))

        if credentials is not None:
            channel = grpc.aio.secure_channel(target, credentials, options=options)
        elif use_tls:
            creds = grpc.ssl_channel_credentials(root_certificates=tls_ca_cert)
            channel = grpc.aio.secure_channel(target, creds, options=options)
        else:
            channel = grpc.aio.insecure_channel(target, options=options)

        try:
            await asyncio.wait_for(channel.channel_ready(), timeout=connect_timeout.total_seconds())
        except (asyncio.TimeoutError, grpc.aio.AioRpcError) as err:
            await channel.close()
            reason = err.details() if isinstance(err, grpc.aio.AioRpcError) else "timed out"
            logger.error("failed to connect to Sepp server at %s: %s", addr, reason)
            raise errors.ConnectError(addr, reason or "connection failed") from err

        logger.info(
            "connected to Sepp server at %s (tls=%s, auth=%s)",
            addr,
            use_tls or credentials is not None,
            bool(auth_metadata),
        )
        return cls(channel, retry_policy=retry_policy, auth_metadata=auth_metadata)

    @classmethod
    def from_channel(
        cls, channel: grpc.aio.Channel, *, retry_policy: RetryPolicy | None = None
    ) -> SeppClient:
        """Wrap an already-established ``grpc.aio`` channel, with no auth and the
        given (or default) :class:`RetryPolicy`."""
        return cls(channel, retry_policy=retry_policy)

    async def close(self) -> None:
        """Close the underlying channel."""
        await self._channel.close()

    async def __aenter__(self) -> SeppClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def enqueue_batch(
        self, jobs: Iterable[EnqueueRequest]
    ) -> list[EnqueueAck | errors.JobRejection]:
        """Enqueue a batch of jobs on a best-effort basis.

        Returns one entry per submitted job, in order: an :class:`EnqueueAck`
        for an accepted job or a :class:`~sepp.errors.JobRejection` for a
        per-job refusal. Whole-call failures (empty batch, transport error,
        protocol violation) raise a :class:`~sepp.errors.ClientError`. Transient
        failures are retried per the client's :class:`RetryPolicy`.

        For all-or-nothing semantics, use :meth:`enqueue_atomic`.
        """
        pb_jobs = [_convert.enqueue_request_to_pb(j) for j in jobs]
        if not pb_jobs:
            raise errors.EmptyBatchError()

        with _otel.client_span("sepp.enqueue"):
            self._inject_trace_into_jobs(pb_jobs)
            req = pb.EnqueueBatchRequest(jobs=pb_jobs)
            try:
                resp = await self._with_retry(
                    "enqueue_batch",
                    lambda: self._stub.EnqueueBatch(req, metadata=self._call_metadata()),
                )
            except grpc.aio.AioRpcError as err:
                raise errors.client_error_from_rpc(err) from err

        if len(resp.results) != len(pb_jobs):
            raise errors.BatchResultCountMismatchError(len(pb_jobs), len(resp.results))

        out: list[EnqueueAck | errors.JobRejection] = []
        for jr in resp.results:
            outcome = jr.WhichOneof("outcome")
            if outcome == "success":
                out.append(_convert.enqueue_ack_from_pb(jr.success))
            elif outcome == "rejection":
                out.append(_convert.job_rejection_from_pb(jr.rejection))
            else:
                raise errors.MalformedResponseError("missing outcome in job result")
        return out

    async def enqueue(self, job: EnqueueRequest) -> EnqueueAck:
        """Enqueue a single job.

        A convenience wrapper over :meth:`enqueue_batch` that raises
        :class:`~sepp.errors.JobRejectedError` if the server rejects the job.
        """
        results = await self.enqueue_batch([job])
        result = results[0]
        if isinstance(result, errors.JobRejection):
            raise errors.JobRejectedError(result)
        return result

    async def enqueue_atomic(self, jobs: Iterable[EnqueueRequest]) -> list[EnqueueAck]:
        """Enqueue a batch of jobs atomically: either all are accepted or none.

        On success returns one :class:`EnqueueAck` per job, in order. If any job
        fails validation, nothing is enqueued and every failure is raised
        together as :class:`~sepp.errors.BatchValidationError`.
        """
        pb_jobs = [_convert.enqueue_request_to_pb(j) for j in jobs]
        if not pb_jobs:
            raise errors.EmptyBatchError()

        with _otel.client_span("sepp.enqueue_atomic"):
            self._inject_trace_into_jobs(pb_jobs)
            req = pb.EnqueueBatchRequest(jobs=pb_jobs)
            try:
                resp = await self._with_retry(
                    "enqueue_atomic",
                    lambda: self._stub.EnqueueAtomic(req, metadata=self._call_metadata()),
                )
            except grpc.aio.AioRpcError as err:
                raise errors.client_error_from_rpc(err) from err

        outcome = resp.WhichOneof("outcome")
        if outcome == "success":
            if len(resp.success.responses) != len(pb_jobs):
                raise errors.BatchResultCountMismatchError(
                    len(pb_jobs), len(resp.success.responses)
                )
            return [_convert.enqueue_ack_from_pb(r) for r in resp.success.responses]
        if outcome == "rejection":
            errs = [_convert.job_validation_error_from_pb(e) for e in resp.rejection.errors]
            raise errors.BatchValidationError(errs)
        raise errors.MalformedResponseError("missing outcome in EnqueueAtomicResponse")

    async def reserve(self, opts: ReserveOptions) -> list[Job] | None:
        """Long-poll for jobs to process.

        Blocks up to the options' ``wait_timeout`` for at least one job. Returns
        a non-empty list of leased :class:`~sepp.types.Job`, or ``None`` if the
        wait elapsed with nothing available (poll again). Each returned job must
        be :meth:`ack`ed, :meth:`nack`ed, or :meth:`extend`ed before its lease
        expires.

        Unlike the other RPCs, reserve is **not** retried by the
        :class:`RetryPolicy`. A malformed job in the response is logged and
        skipped rather than failing the whole batch.
        """
        req = _convert.reserve_options_to_pb(opts)
        timeout = (opts.wait_timeout + _RESERVE_DEADLINE_SLACK).total_seconds()

        with _otel.client_span("sepp.reserve"):
            try:
                resp = await self._stub.Reserve(
                    req, timeout=timeout, metadata=self._call_metadata()
                )
            except grpc.aio.AioRpcError as err:
                raise errors.reserve_error_from_rpc(err) from err

        jobs: list[Job] = []
        for j in resp.jobs:
            try:
                jobs.append(_convert.job_from_pb(self, j, opts.worker_id))
            except errors.JobConversionError as exc:
                logger.warning("skipping malformed job in reserve response: %s", exc)
        return jobs or None

    async def ack(self, ctx: JobCtx) -> None:
        """Acknowledge that a job completed successfully, removing it from the
        queue.

        The ``attempt`` carried by ``ctx`` guards against acking a job whose
        lease was already reassigned — that surfaces as
        :class:`~sepp.errors.AttemptMismatchError` or
        :class:`~sepp.errors.JobNotFoundError`.
        """
        req = pb.AckRequest(job_id=ctx.id, attempt=ctx.attempt)
        worker_id = ctx._lease.worker_id if ctx._lease else None
        if worker_id is not None:
            req.worker_id = worker_id

        with _otel.client_span("sepp.ack"):
            try:
                await self._with_retry(
                    "ack", lambda: self._stub.Ack(req, metadata=self._call_metadata())
                )
            except grpc.aio.AioRpcError as err:
                raise errors.lease_error_from_rpc(err) from err

    async def nack(
        self,
        ctx: JobCtx,
        retry: RetryDirective = RetryDirective.DEFAULT,
        reason: str = "",
    ) -> bool:
        """Negatively acknowledge a job, signalling that processing failed.

        ``retry`` selects what the server does next (see :class:`RetryDirective`)
        and ``reason`` is recorded for debugging and metrics. Returns ``True`` if
        this nack moved the job to the dead-letter queue (because
        ``DEAD_LETTER`` was requested or ``max_attempts`` was reached), ``False``
        if it will be retried.
        """
        nack_retry = pb.NackRetry()
        if retry.kind == "after":
            assert retry.delay is not None
            nack_retry.delay.FromTimedelta(retry.delay)
        elif retry.kind == "dead_letter":
            nack_retry.dead_letter.SetInParent()
        else:
            nack_retry.default.SetInParent()

        req = pb.NackRequest(job_id=ctx.id, attempt=ctx.attempt, reason=reason)
        req.retry.CopyFrom(nack_retry)
        worker_id = ctx._lease.worker_id if ctx._lease else None
        if worker_id is not None:
            req.worker_id = worker_id

        with _otel.client_span("sepp.nack"):
            try:
                resp = await self._with_retry(
                    "nack", lambda: self._stub.Nack(req, metadata=self._call_metadata())
                )
            except grpc.aio.AioRpcError as err:
                raise errors.lease_error_from_rpc(err) from err
        return bool(resp.dead_lettered)

    async def extend(self, ctx: JobCtx, extension: timedelta) -> datetime:
        """Extend a job's lease by ``extension``, measured from now, returning
        the new expiry. Equivalent to :meth:`JobCtx.extend
        <sepp.types.JobCtx.extend>`."""
        worker_id = ctx._lease.worker_id if ctx._lease else None
        return await self._extend_inner(ctx.id, ctx.attempt, extension, worker_id)

    async def _extend_inner(
        self, job_id: str, attempt: int, extension: timedelta, worker_id: str | None
    ) -> datetime:
        req = pb.ExtendRequest(job_id=job_id, attempt=attempt)
        req.lease_duration.FromTimedelta(extension)
        if worker_id is not None:
            req.worker_id = worker_id

        with _otel.client_span("sepp.extend"):
            try:
                resp = await self._with_retry(
                    "extend", lambda: self._stub.Extend(req, metadata=self._call_metadata())
                )
            except grpc.aio.AioRpcError as err:
                raise errors.lease_error_from_rpc(err) from err

        expiry = _convert.timestamp_to_datetime(resp.lease_expires_at)
        if expiry is None:
            raise errors.MalformedResponseError("extend returned an invalid lease_expires_at")
        return expiry

    async def get_server_info(self) -> ServerInfo:
        """Fetch the server's :class:`~sepp.types.ServerInfo`: version,
        capabilities, and limits."""
        with _otel.client_span("sepp.get_server_info"):
            try:
                resp = await self._with_retry(
                    "get_server_info",
                    lambda: self._stub.GetServerInfo(
                        pb.GetServerInfoRequest(), metadata=self._call_metadata()
                    ),
                )
            except grpc.aio.AioRpcError as err:
                raise errors.client_error_from_rpc(err) from err
        try:
            return _convert.server_info_from_pb(resp)
        except errors.ServerInfoError as exc:
            raise errors.MalformedServerInfoError(exc) from exc

    async def drain_dead_letters(
        self, queue: str | None = None, max_records: int = 1
    ) -> list[DeadLetterRecord]:
        """Drain dead-lettered jobs for inspection and manual replay.

        Returns up to ``max_records`` :class:`~sepp.types.DeadLetterRecord`
        (oldest-first, optionally filtered to one ``queue``) and **removes them
        from the server**. This is destructive: the records are gone once
        returned, so a dropped response loses exactly that batch — for that
        reason it is **not** retried by the :class:`RetryPolicy`. Inspect each
        record, then replay any you want with
        :meth:`DeadLetterRecord.to_enqueue_request
        <sepp.types.DeadLetterRecord.to_enqueue_request>`.

        An empty list means nothing matched, which is indistinguishable from
        dead-letter retention being disabled — check
        :attr:`ServerInfo.dead_letter_retention_enabled
        <sepp.types.ServerInfo.dead_letter_retention_enabled>`.
        """
        req = pb.DrainDeadLettersRequest(max=max(max_records, 1))
        if queue is not None:
            req.queue = queue

        with _otel.client_span("sepp.drain_dead_letters"):
            try:
                resp = await self._stub.DrainDeadLetters(req, metadata=self._call_metadata())
            except grpc.aio.AioRpcError as err:
                raise errors.client_error_from_rpc(err) from err

        records: list[DeadLetterRecord] = []
        for r in resp.records:
            try:
                records.append(_convert.dead_letter_record_from_pb(r))
            except errors.JobConversionError as exc:
                logger.warning("skipping malformed dead-letter record in drain response: %s", exc)
        return records

    def _call_metadata(self) -> list[tuple[str, str]]:
        metadata = list(self._auth_metadata)
        _otel.inject_context_metadata(metadata)
        return metadata

    def _inject_trace_into_jobs(self, pb_jobs: list[pb.EnqueueRequest]) -> None:
        headers = _otel.current_trace_headers()
        if headers is None:
            return
        traceparent, tracestate = headers
        for job in pb_jobs:
            if job.HasField("trace_context"):
                continue
            job.trace_context.traceparent = traceparent
            if tracestate:
                job.trace_context.tracestate = tracestate

    async def _with_retry(self, operation: str, factory: Callable[[], Awaitable[_T]]) -> _T:
        import asyncio

        policy = self._retry_policy
        attempt = 1
        backoff = policy.initial_backoff
        while True:
            try:
                return await factory()
            except grpc.aio.AioRpcError as err:
                retryable = err.code() in errors.RETRYABLE_CODES
                if attempt >= policy.max_attempts or not retryable:
                    raise
                delay = backoff.total_seconds()
                if policy.jitter:
                    # Equal-jitter: a value in [0.5, 1.0) of the computed delay.
                    delay *= 0.5 + 0.5 * random.random()
                logger.warning(
                    "retrying %s after transient error (attempt %d): %s %s",
                    operation,
                    attempt,
                    err.code(),
                    err.details(),
                )
                await asyncio.sleep(delay)
                backoff = timedelta(
                    seconds=min(
                        backoff.total_seconds() * policy.multiplier,
                        policy.max_backoff.total_seconds(),
                    )
                )
                attempt += 1


def _normalize_target(addr: str) -> tuple[str, bool]:
    """Strip an optional scheme from ``addr``. Returns ``(target, tls)`` where
    ``tls`` is ``True`` only for an explicit ``https://`` scheme."""
    if addr.startswith("http://"):
        return addr[len("http://") :], False
    if addr.startswith("https://"):
        return addr[len("https://") :], True
    return addr, False


def _auth_metadata_for(api_key: str | None) -> list[tuple[str, str]]:
    if api_key is None:
        return []
    # gRPC metadata values must be valid HTTP header values: ASCII, no CR/LF.
    if "\n" in api_key or "\r" in api_key:
        raise errors.InvalidApiKeyError()
    try:
        api_key.encode("ascii")
    except UnicodeEncodeError as exc:
        raise errors.InvalidApiKeyError() from exc
    return [("authorization", f"Bearer {api_key}")]
