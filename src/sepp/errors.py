"""Exceptions and rejection types for the sepp client.

The hierarchy mirrors the Rust client's error enums, mapped onto Python
exceptions:

* :class:`ClientError` and its subclasses cover transport/protocol failures,
  shared by most RPCs.
* :class:`JobRejection` (and its variants) is a *value*, not an exception: it
  describes a deterministic, per-job refusal returned by the enqueue RPCs. The
  single-job :meth:`~sepp.client.SeppClient.enqueue` wrapper raises it wrapped
  in :class:`JobRejectedError`.
* :class:`LeaseError` / :class:`ReserveError` carry the domain-specific
  failures of the lease and reserve RPCs; other failures surface as a
  :class:`ClientError`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import grpc

__all__ = [
    "SeppError",
    "ClientError",
    "ConnectError",
    "InvalidApiKeyError",
    "TransportError",
    "UnauthenticatedError",
    "OverloadedError",
    "InvalidRequestError",
    "ServerInternalError",
    "UnexpectedStatusError",
    "EmptyBatchError",
    "BatchResultCountMismatchError",
    "MalformedResponseError",
    "MalformedJobError",
    "MalformedServerInfoError",
    "JobConversionError",
    "ServerInfoError",
    "JobRejection",
    "UnknownQueue",
    "PayloadTooLarge",
    "EncodingNotAllowed",
    "JobTypeNotAllowed",
    "CustomEntriesTooMany",
    "CustomMapTooLarge",
    "CustomKeyTooLong",
    "QueueNameTooLong",
    "JobTypeNameTooLong",
    "IdempotencyKeyTooLong",
    "ScheduledTooFar",
    "InvalidRequest",
    "QueueFull",
    "QueueClosing",
    "UnknownRejection",
    "JobValidationError",
    "JobRejectedError",
    "BatchValidationError",
    "LeaseError",
    "JobNotFoundError",
    "AttemptMismatchError",
    "ReserveError",
    "UnknownQueuesError",
]


class SeppError(Exception):
    """Base class for every error raised by this library."""


class ClientError(SeppError):
    """A transport- or protocol-level failure, shared by most RPCs.

    gRPC status codes are mapped onto the subclasses below: ``UNAVAILABLE`` /
    ``DEADLINE_EXCEEDED`` / ``ABORTED`` / ``CANCELLED`` become
    :class:`TransportError`, ``RESOURCE_EXHAUSTED`` becomes
    :class:`OverloadedError`, and so on. The transport-ish ones are what the
    retry policy retries.
    """


class ConnectError(ClientError):
    """Establishing the connection failed."""

    def __init__(self, addr: str, reason: str) -> None:
        self.addr = addr
        self.reason = reason
        super().__init__(f"could not connect to Sepp server at {addr}: {reason}")


class InvalidApiKeyError(ClientError):
    """The configured API key could not be encoded as an HTTP header value."""

    def __init__(self) -> None:
        super().__init__("the API key is not a valid HTTP header value")


class TransportError(ClientError):
    """A transient transport-level failure (connection dropped, deadline
    exceeded, request aborted/cancelled). Generally safe to retry."""


class UnauthenticatedError(ClientError):
    """The server rejected the credentials (missing/invalid API key, or
    permission denied)."""


class OverloadedError(ClientError):
    """The server is shedding load (``RESOURCE_EXHAUSTED``); back off and
    retry."""


class InvalidRequestError(ClientError):
    """The server rejected the request as malformed (``INVALID_ARGUMENT``)."""


class ServerInternalError(ClientError):
    """The server hit an internal error (``INTERNAL`` / ``DATA_LOSS`` /
    ``UNKNOWN``)."""


class UnexpectedStatusError(ClientError):
    """The server returned a status code this client does not map to a more
    specific error."""

    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        self.code = code
        self.status_message = message
        super().__init__(f"server returned unexpected status {code!r}: {message}")


class EmptyBatchError(ClientError):
    """An enqueue was attempted with no jobs."""

    def __init__(self) -> None:
        super().__init__("empty batch")


class BatchResultCountMismatchError(ClientError):
    """The server returned a different number of results than jobs sent — a
    protocol violation."""

    def __init__(self, expected: int, got: int) -> None:
        self.expected = expected
        self.got = got
        super().__init__(f"server returned {got} results for a batch of {expected} jobs")


class MalformedResponseError(ClientError):
    """A response was missing a field the protocol requires."""


class MalformedJobError(ClientError):
    """A job in a response could not be decoded; see :class:`JobConversionError`."""

    def __init__(self, cause: JobConversionError) -> None:
        self.cause = cause
        super().__init__(f"server returned a malformed job: {cause}")


class MalformedServerInfoError(ClientError):
    """A server-info response could not be decoded; see :class:`ServerInfoError`."""

    def __init__(self, cause: ServerInfoError) -> None:
        self.cause = cause
        super().__init__(f"server returned malformed server info: {cause}")


class JobConversionError(SeppError):
    """A job received from the server could not be decoded into a
    :class:`~sepp.types.Job`.

    During :meth:`~sepp.client.SeppClient.reserve` this is logged and the
    offending job is skipped rather than failing the whole batch, so you
    normally only encounter it indirectly.
    """


class ServerInfoError(SeppError):
    """A ``get_server_info`` response could not be decoded into a
    :class:`~sepp.types.ServerInfo`."""


class JobRejection:
    """Why the server refused a single job.

    Every variant except :class:`QueueFull` and :class:`QueueClosing` is
    *deterministic*: re-sending the same job against the same server
    configuration produces the same rejection.
    Transient problems surface as a :class:`ClientError` instead. Most limits
    behind these variants are advertised up front by
    :class:`~sepp.types.ServerInfo`, so a producer can validate locally before
    sending.

    This is a value returned by :meth:`~sepp.client.SeppClient.enqueue_batch`,
    not an exception. Pattern-match on the concrete subclass (e.g.
    ``isinstance(rej, PayloadTooLarge)``).
    """

    @property
    def message(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class UnknownQueue(JobRejection):
    """The server is in strict mode and the target queue is not declared."""

    queue: str

    @property
    def message(self) -> str:
        return f"queue {self.queue!r} is not declared on the server (strict mode)"


@dataclass(frozen=True)
class PayloadTooLarge(JobRejection):
    """The payload exceeds the queue's ``max_payload_size``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return f"payload size {self.actual} bytes exceeds the queue limit of {self.limit}"


@dataclass(frozen=True)
class EncodingNotAllowed(JobRejection):
    """The queue restricts encodings and the payload's encoding is not on the
    allow-list."""

    encoding: str
    allowed: list[str]

    @property
    def message(self) -> str:
        return f"payload encoding {self.encoding!r} is not allowed; accepted: {self.allowed}"


@dataclass(frozen=True)
class JobTypeNotAllowed(JobRejection):
    """The queue restricts job types and this one is not on the allow-list."""

    job_type: str
    allowed: list[str]

    @property
    def message(self) -> str:
        return f"job_type {self.job_type!r} is not accepted by this queue; accepted: {self.allowed}"


@dataclass(frozen=True)
class CustomEntriesTooMany(JobRejection):
    """The custom map has more entries than ``max_custom_entries``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return f"custom map has {self.actual} entries, exceeding the queue limit of {self.limit}"


@dataclass(frozen=True)
class CustomMapTooLarge(JobRejection):
    """The custom map's total size exceeds ``max_custom_total_bytes``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return (
            f"custom map's total size {self.actual} bytes exceeds the queue limit of {self.limit}"
        )


@dataclass(frozen=True)
class CustomKeyTooLong(JobRejection):
    """A custom key exceeds ``max_custom_key_bytes``."""

    key: str
    limit: int
    actual: int

    @property
    def message(self) -> str:
        return (
            f"custom key {self.key!r} is {self.actual} bytes, exceeding the limit of {self.limit}"
        )


@dataclass(frozen=True)
class QueueNameTooLong(JobRejection):
    """The queue name exceeds ``max_queue_name_bytes``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return f"queue name is {self.actual} bytes, exceeding the limit of {self.limit}"


@dataclass(frozen=True)
class JobTypeNameTooLong(JobRejection):
    """The job type exceeds ``max_job_type_bytes``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return f"job_type is {self.actual} bytes, exceeding the limit of {self.limit}"


@dataclass(frozen=True)
class IdempotencyKeyTooLong(JobRejection):
    """The idempotency key exceeds ``max_idempotency_key_bytes``."""

    limit: int
    actual: int

    @property
    def message(self) -> str:
        return f"idempotency_key is {self.actual} bytes, exceeding the limit of {self.limit}"


@dataclass(frozen=True)
class ScheduledTooFar(JobRejection):
    """``scheduled_at`` is further out than the server's schedule horizon.

    :attr:`actual` is the requested run time (an absolute instant) and
    :attr:`horizon` is the maximum scheduling distance the server allows."""

    horizon: timedelta
    actual: datetime

    @property
    def message(self) -> str:
        return (
            f"scheduled_at {self.actual.isoformat()} is beyond "
            f"the schedule horizon ({self.horizon})"
        )


@dataclass(frozen=True)
class InvalidRequest(JobRejection):
    """The request failed the server's structural validation."""

    detail: str

    @property
    def message(self) -> str:
        return f"structural validation failed: {self.detail}"


@dataclass(frozen=True)
class QueueFull(JobRejection):
    """The target queue is at its ``max_queue_depth``. Non-deterministic: it
    clears once the queue drains."""

    queue: str
    limit: int

    @property
    def message(self) -> str:
        return f"queue {self.queue!r} is full (max depth {self.limit})"


@dataclass(frozen=True)
class QueueClosing(JobRejection):
    """The target queue is being deleted and is not accepting new jobs.
    Non-deterministic: it clears once the delete completes or is abandoned."""

    queue: str

    @property
    def message(self) -> str:
        return f"queue {self.queue!r} is being deleted and is not accepting new jobs"


@dataclass(frozen=True)
class UnknownRejection(JobRejection):
    """The server sent a rejection reason this client version does not
    recognize."""

    @property
    def message(self) -> str:
        return "server returned an unrecognized rejection variant"


@dataclass(frozen=True)
class JobValidationError:
    """A single job's rejection within an atomic batch, paired with its
    zero-based position in the request."""

    index: int
    rejection: JobRejection


class JobRejectedError(SeppError):
    """Raised by the single-job :meth:`~sepp.client.SeppClient.enqueue` when the
    server accepts the request but rejects the job. Carries the
    :class:`JobRejection`."""

    def __init__(self, rejection: JobRejection) -> None:
        self.rejection = rejection
        super().__init__(f"server rejected the job: {rejection}")


class BatchValidationError(SeppError):
    """Raised by :meth:`~sepp.client.SeppClient.enqueue_atomic` when one or more
    jobs fail validation, so the whole batch was rejected and nothing was
    enqueued. Carries every failure in :attr:`errors`."""

    def __init__(self, errors: list[JobValidationError]) -> None:
        self.errors = errors
        super().__init__(f"atomic batch rejected: {len(errors)} job(s) failed validation")


class LeaseError(SeppError):
    """Base for the domain failures of the lease operations ``ack`` / ``nack`` /
    ``extend``.

    :class:`JobNotFoundError` and :class:`AttemptMismatchError` both mean the
    worker no longer holds the lease — typically because it was allowed to
    expire and the job was redelivered. Transport failures surface as a
    :class:`ClientError` instead.
    """


class JobNotFoundError(LeaseError):
    """No in-flight job has this id: it was already acked, the lease expired, or
    it never existed."""

    def __init__(self, message: str = "") -> None:
        super().__init__(
            message or "no in-flight job with this id (already acked, expired, or never existed)"
        )


class AttemptMismatchError(LeaseError):
    """The attempt number no longer matches the server's: the lease was
    reassigned to another delivery."""

    def __init__(self, message: str = "") -> None:
        super().__init__(message or "attempt mismatch: the lease was reassigned")


class ReserveError(SeppError):
    """Base for the domain failures of :meth:`~sepp.client.SeppClient.reserve`.
    Transport failures surface as a :class:`ClientError` instead."""


class UnknownQueuesError(ReserveError):
    """The server is in strict mode and one or more requested queues are not
    declared; the message lists them."""

    def __init__(self, message: str) -> None:
        super().__init__(f"requested queues are not declared on the server: {message}")


_TRANSPORT_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.CANCELLED,
    }
)

# Codes worth retrying. CANCELLED is treated as a transport error but is *not*
# retried (it usually means the caller went away), matching the Rust client.
RETRYABLE_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)


def client_error_from_rpc(err: grpc.aio.AioRpcError) -> ClientError:
    """Map a gRPC error onto the appropriate :class:`ClientError` subclass."""
    code = err.code()
    msg = err.details() or ""
    if code in _TRANSPORT_CODES:
        return TransportError(msg)
    if code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
        return UnauthenticatedError(msg)
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        return OverloadedError(msg)
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return InvalidRequestError(msg)
    if code in (grpc.StatusCode.INTERNAL, grpc.StatusCode.DATA_LOSS, grpc.StatusCode.UNKNOWN):
        return ServerInternalError(msg)
    return UnexpectedStatusError(code, msg)


def lease_error_from_rpc(err: grpc.aio.AioRpcError) -> SeppError:
    """Map a gRPC error onto a lease error, or a :class:`ClientError`."""
    code = err.code()
    if code == grpc.StatusCode.NOT_FOUND:
        return JobNotFoundError()
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        return AttemptMismatchError()
    return client_error_from_rpc(err)


def reserve_error_from_rpc(err: grpc.aio.AioRpcError) -> SeppError:
    """Map a gRPC error onto a reserve error, or a :class:`ClientError`."""
    if err.code() == grpc.StatusCode.FAILED_PRECONDITION:
        return UnknownQueuesError(err.details() or "")
    return client_error_from_rpc(err)
