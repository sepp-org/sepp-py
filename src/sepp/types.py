"""Domain types for the sepp client.

These mirror the public value types of the Rust ``sepp-rs`` client, adapted to
Python idioms: required fields are positional, optional fields are keyword
arguments, and results are dataclasses. The wire (protobuf) representation is
kept entirely in :mod:`sepp._convert`; nothing here imports the generated stubs.
"""

from __future__ import annotations

import contextlib
import enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sepp.client import Lease

# How long the server holds the reserve connection open if no job is ready.
_DEFAULT_WAIT_TIMEOUT = timedelta(seconds=30)

__all__ = [
    "Primitive",
    "Payload",
    "Priority",
    "PriorityOutOfRangeError",
    "TraceContext",
    "TraceContextError",
    "EnqueueRequest",
    "EnqueueAck",
    "JobCtx",
    "Job",
    "ReserveOptions",
    "ServerInfo",
    "DeadLetterCause",
    "DeadLetterRecord",
]

# A JSON-primitive value stored in a job's ``custom`` metadata map. Unlike the
# Rust ``Primitive`` enum, Python's native scalar types carry the distinction
# directly, so callers pass ``str``/``int``/``float``/``bool`` as-is. Note that
# ``bool`` is a subclass of ``int``; the conversion layer checks ``bool`` first.
Primitive = str | int | float | bool


@dataclass(frozen=True)
class Payload:
    """The opaque body of a job, plus an encoding hint.

    The queue never interprets ``data``; it only carries the bytes. ``encoding``
    (for example ``"application/json"`` or ``"text/plain"``) is a hint the
    producer sets and the worker reads to decide how to deserialize the bytes. A
    queue may restrict which encodings it accepts.
    """

    data: bytes
    encoding: str


class PriorityOutOfRangeError(ValueError):
    """Raised by :class:`Priority` when the value is outside ``0..=9``."""

    def __init__(self, value: int) -> None:
        self.value = value
        super().__init__(f"priority must be 0-9, got {value}")


@dataclass(frozen=True, order=True)
class Priority:
    """A job priority in the range ``0..=9``, where higher values dequeue first.

    Construct one with ``Priority(7)`` or use the :attr:`P0`–:attr:`P9`
    constants. Values outside the range raise :class:`PriorityOutOfRangeError`.
    """

    value: int

    # Assigned after the class body; declared here so type checkers see them.
    MIN: ClassVar[Priority]
    MAX: ClassVar[Priority]
    P0: ClassVar[Priority]
    P1: ClassVar[Priority]
    P2: ClassVar[Priority]
    P3: ClassVar[Priority]
    P4: ClassVar[Priority]
    P5: ClassVar[Priority]
    P6: ClassVar[Priority]
    P7: ClassVar[Priority]
    P8: ClassVar[Priority]
    P9: ClassVar[Priority]

    def __post_init__(self) -> None:
        if not isinstance(self.value, int) or isinstance(self.value, bool):
            raise PriorityOutOfRangeError(self.value)
        if self.value < 0 or self.value > 9:
            raise PriorityOutOfRangeError(self.value)

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)


# Priority constants, mirroring Rust's Priority::P0 .. Priority::P9 / MIN / MAX.
Priority.P0 = Priority(0)
Priority.P1 = Priority(1)
Priority.P2 = Priority(2)
Priority.P3 = Priority(3)
Priority.P4 = Priority(4)
Priority.P5 = Priority(5)
Priority.P6 = Priority(6)
Priority.P7 = Priority(7)
Priority.P8 = Priority(8)
Priority.P9 = Priority(9)
Priority.MIN = Priority.P0
Priority.MAX = Priority.P9


class TraceContextError(ValueError):
    """Raised by :class:`TraceContext` when the ``traceparent`` is malformed."""


def _validate_traceparent(traceparent: str) -> None:
    # W3C: version-trace_id-span_id-flags -> "00-<32hex>-<16hex>-<2hex>"
    parts = traceparent.split("-")
    if len(parts) != 4:
        raise TraceContextError("invalid traceparent: expected 4 hyphen-separated fields")
    version, trace_id, span_id, flags = parts
    if len(version) != 2 or not _is_hex(version):
        raise TraceContextError("invalid traceparent: version must be 2 hex chars")
    if len(trace_id) != 32 or not _is_hex(trace_id):
        raise TraceContextError("invalid traceparent: trace_id must be 32 hex chars")
    if set(trace_id) == {"0"}:
        raise TraceContextError("invalid traceparent: trace_id must not be all zeros")
    if len(span_id) != 16 or not _is_hex(span_id):
        raise TraceContextError("invalid traceparent: span_id must be 16 hex chars")
    if set(span_id) == {"0"}:
        raise TraceContextError("invalid traceparent: span_id must not be all zeros")
    if len(flags) != 2 or not _is_hex(flags):
        raise TraceContextError("invalid traceparent: flags must be 2 hex chars")


def _is_hex(s: str) -> bool:
    return all(c in "0123456789abcdefABCDEF" for c in s)


@dataclass(frozen=True)
class TraceContext:
    """A `W3C Trace Context <https://www.w3.org/TR/trace-context/>`_ on a job.

    Links a producer's trace to the worker that processes the job. The
    ``traceparent`` is validated on construction. With the ``otel`` extra
    installed, the client and :class:`~sepp.worker.Worker` wire this up
    automatically; construct one by hand only to bridge to or from another trace
    propagation system.
    """

    traceparent: str
    tracestate: str | None = None

    def __post_init__(self) -> None:
        _validate_traceparent(self.traceparent)

    @classmethod
    def from_current_otel(cls) -> TraceContext | None:
        """Capture the current OpenTelemetry context, or ``None`` if there is no
        valid active span (or the ``otel`` extra is not installed)."""
        from sepp import _otel

        return _otel.trace_context_from_current()

    def attach_to_otel(self) -> contextlib.AbstractContextManager[None]:
        """Install this trace context as the current OpenTelemetry context for
        the duration of the returned context manager.

        Requires the ``otel`` extra.
        """
        from sepp import _otel

        return _otel.attach_trace_context(self)

    def otel_span_context(self) -> Any:
        """Decode this trace context into an OpenTelemetry ``SpanContext``, or
        ``None`` if it does not represent a valid span (or the ``otel`` extra is
        not installed).

        Use this to add a span *link* from a worker's process span back to the
        producer by hand; :class:`~sepp.worker.Worker` does this automatically.
        """
        from sepp import _otel

        return _otel.span_context_from_trace_context(self)


@dataclass
class EnqueueRequest:
    """A job to enqueue.

    ``queue`` and ``job_type`` are required and must be non-empty; everything
    else is optional and falls back to the queue's server-side defaults when
    unset. ``priority`` accepts a :class:`Priority` or a bare ``int`` in
    ``0..=9``.
    """

    queue: str
    job_type: str
    payload: Payload | None = None
    idempotency_key: str | None = None
    priority: Priority | int | None = None
    max_attempts: int | None = None
    custom: Mapping[str, Primitive] = field(default_factory=dict)
    trace_context: TraceContext | None = None
    scheduled_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.queue:
            raise ValueError("queue name must not be empty")
        if not self.job_type:
            raise ValueError("job type must not be empty")
        if isinstance(self.priority, int) and not isinstance(self.priority, Priority):
            # Coerce a bare int (including out-of-range, which raises here).
            self.priority = Priority(self.priority)


@dataclass(frozen=True)
class EnqueueAck:
    """Confirmation that a job was accepted by the server."""

    job_id: str
    """The server-assigned job id (a UUID). When ``deduplicated`` is true, this
    is the id of the pre-existing job."""
    deduplicated: bool
    """``True`` if an idempotency key matched an existing job, so this enqueue
    was a no-op."""


@dataclass
class JobCtx:
    """Everything about a reserved job except its payload: identity, delivery
    metadata, and the handle needed to manage its lease.

    A handler receives this alongside the payload. :meth:`extend` renews the
    lease directly from it.
    """

    id: str
    queue: str
    job_type: str
    priority: Priority
    attempt: int
    max_attempts: int
    enqueued_at: datetime
    custom: dict[str, Primitive]
    trace_context: TraceContext | None
    lease_expires_at: datetime
    # Internal lease handle, attached by the conversion layer.
    _lease: Lease | None = field(default=None, repr=False, compare=False)

    async def extend(self, extension: timedelta) -> datetime:
        """Extend this job's lease by ``extension``, measured from now, and
        return the new expiry.

        Use this from inside a handler that needs more time than the original
        lease allowed. A :class:`~sepp.worker.Worker` configured with
        ``auto_extend=True`` does this for you.
        """
        if self._lease is None:
            raise RuntimeError("JobCtx has no lease handle; it was not produced by reserve()")
        return await self._lease.extend(extension)

    def __str__(self) -> str:
        return (
            f"JobCtx(id={self.id}, job_type={self.job_type}, "
            f"attempt={self.attempt}/{self.max_attempts}, priority={self.priority.value})"
        )


@dataclass
class Job:
    """A reserved job: its optional :class:`Payload` and its :class:`JobCtx`."""

    payload: Payload | None
    ctx: JobCtx


@dataclass
class ReserveOptions:
    """Parameters for a :meth:`~sepp.client.SeppClient.reserve` call.

    ``queues`` (polled in order, index 0 first) and ``lease_duration`` are
    required. ``wait_timeout`` defaults to 30s. If you use a
    :class:`~sepp.worker.Worker`, it builds and manages these for you.
    """

    queues: Sequence[str]
    lease_duration: timedelta
    wait_timeout: timedelta = _DEFAULT_WAIT_TIMEOUT
    worker_id: str | None = None
    max_jobs: int | None = None

    def __post_init__(self) -> None:
        self.queues = list(self.queues)
        if not self.queues:
            raise ValueError("at least one queue must be specified")
        for i, q in enumerate(self.queues):
            if not q:
                raise ValueError(f"queue name at index {i} must not be empty")
        if self.lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be at least 1ms")
        if self.worker_id is not None and not self.worker_id:
            raise ValueError("worker_id must not be empty when set")


@dataclass(frozen=True)
class ServerInfo:
    """The server's version, capabilities, and limits.

    The various ``max_*`` fields mirror the limits behind the job-rejection
    types. Fetching this once at startup lets a producer validate jobs locally
    and avoid round-trips that would only be rejected. The limits are the
    server's defaults; an individual queue may be configured more or less
    strictly.
    """

    version: str
    supported_protocol_versions: list[str]
    server_time: datetime
    restricts_encodings: bool
    allowed_encodings: list[str]
    max_payload_size: int
    max_custom_entries: int
    max_custom_total_bytes: int
    max_custom_key_bytes: int
    max_queue_name_bytes: int
    max_job_type_bytes: int
    max_idempotency_key_bytes: int
    max_schedule_horizon: timedelta
    max_enqueue_batch: int
    max_reserve_batch: int
    max_reserve_queues: int
    max_wait_timeout: timedelta
    max_lease_duration: timedelta
    strict_queues: bool
    dead_letter_retention_enabled: bool
    """If ``True``, the server retains dead-lettered jobs and
    :meth:`~sepp.client.SeppClient.drain_dead_letters` can return them; if
    ``False``, dead jobs are deleted and drain always returns empty."""


class DeadLetterCause(enum.Enum):
    """Why a job was moved to the server's dead-letter store. Mirrors the
    terminal paths a job can take."""

    UNSPECIFIED = "unspecified"
    """The cause was unset — an unknown or future variant."""
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    """The job exhausted its max attempts across nacks and redeliveries."""
    REJECTED = "rejected"
    """A worker nacked with ``RetryDirective.DEAD_LETTER``, skipping its
    remaining attempts."""
    LEASE_EXPIRED = "lease_expired"
    """The lease expired while the job was on its final attempt."""
    ADMIN = "admin"
    """An operator dead-lettered the job through the admin API."""


@dataclass(frozen=True)
class DeadLetterRecord:
    """A dead-lettered job retained by the server, returned by
    :meth:`~sepp.client.SeppClient.drain_dead_letters`.

    A snapshot for inspection and manual replay: read :attr:`cause`,
    :attr:`last_reason`, and :attr:`final_attempt` to see what went wrong, then
    call :meth:`to_enqueue_request` to re-submit it.
    """

    queue: str
    job_id: str
    job_type: str
    payload: Payload | None
    priority: Priority
    custom: dict[str, Primitive]
    trace_context: TraceContext | None
    enqueued_at: datetime
    cause: DeadLetterCause
    failed_at: datetime
    final_attempt: int
    last_reason: str | None

    def to_enqueue_request(self) -> EnqueueRequest:
        """Build an :class:`EnqueueRequest` that replays this job into its
        original queue, preserving its payload, priority, custom metadata, and
        trace context. The replay is a fresh job — the server assigns a new id
        and resets the attempt counter."""
        return EnqueueRequest(
            queue=self.queue,
            job_type=self.job_type,
            payload=self.payload,
            priority=self.priority,
            custom=dict(self.custom),
            trace_context=self.trace_context,
        )
