"""Conversions between the generated protobuf messages and the domain types.

Kept separate so :mod:`sepp.types` stays free of protobuf imports.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from google.protobuf.timestamp_pb2 import Timestamp as _Timestamp

from sepp import errors
from sepp.types import (
    DeadLetterCause,
    DeadLetterRecord,
    EnqueueAck,
    EnqueueRequest,
    Job,
    JobCtx,
    Payload,
    Primitive,
    Priority,
    PriorityOutOfRangeError,
    ReserveOptions,
    ServerInfo,
    TraceContext,
)
from sepp.v1 import queue_pb2 as pb

if TYPE_CHECKING:
    from sepp.client import SeppClient

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


_ONE_MS = timedelta(milliseconds=1)


def datetime_to_millis(dt: datetime) -> int:
    """Milliseconds since the Unix epoch (UTC). Naive datetimes are assumed UTC.
    Pre-epoch instants yield a negative value (callers decide how to treat it)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Integer arithmetic: routing through float seconds can be off by 1ms for
    # far-future / sub-ms instants.
    return (dt - _EPOCH) // _ONE_MS


def millis_to_datetime(ms: int) -> datetime | None:
    """A UTC datetime for ``ms`` since the epoch, or ``None`` if ``ms`` is
    negative or not representable."""
    if ms < 0:
        return None
    try:
        return _EPOCH + timedelta(milliseconds=ms)
    except (OverflowError, OSError):
        return None


def timedelta_to_millis(td: timedelta) -> int:
    return int(td.total_seconds() * 1000)


def now_millis() -> int:
    return datetime_to_millis(datetime.now(timezone.utc))


def timestamp_to_datetime(ts: _Timestamp) -> datetime | None:
    """A UTC datetime for a ``google.protobuf.Timestamp``, or ``None`` if it is
    pre-epoch or not representable."""
    if ts.seconds < 0 or ts.nanos < 0:
        return None
    try:
        return ts.ToDatetime(tzinfo=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def primitive_to_pb(value: Primitive) -> pb.PrimitiveValue:
    pv = pb.PrimitiveValue()
    # bool is a subclass of int, so it must be checked first.
    if isinstance(value, bool):
        pv.bool_value = value
    elif isinstance(value, int):
        pv.int_value = value
    elif isinstance(value, float):
        pv.double_value = value
    elif isinstance(value, str):
        pv.string_value = value
    else:
        raise TypeError(f"custom value must be str/int/float/bool, got {type(value).__name__}")
    return pv


def primitive_from_pb(pv: pb.PrimitiveValue) -> Primitive | None:
    which = pv.WhichOneof("value")
    if which is None:
        return None

    value: Primitive = getattr(pv, which)
    return value


def payload_to_pb(p: Payload) -> pb.Payload:
    return pb.Payload(data=p.data, encoding=p.encoding)


def payload_from_pb(p: pb.Payload) -> Payload:
    return Payload(data=p.data, encoding=p.encoding)


def trace_context_to_pb(tc: TraceContext) -> pb.TraceContext:
    m = pb.TraceContext(traceparent=tc.traceparent)

    if tc.tracestate is not None:
        m.tracestate = tc.tracestate

    return m


def trace_context_from_pb(m: pb.TraceContext) -> TraceContext | None:
    # An invalid trace context must not block job delivery: drop it and lose
    # trace continuity rather than failing the whole reservation.
    tracestate = m.tracestate if m.HasField("tracestate") else None

    try:
        return TraceContext(m.traceparent, tracestate)
    except ValueError:
        return None


def enqueue_request_to_pb(req: EnqueueRequest) -> pb.EnqueueRequest:
    m = pb.EnqueueRequest(queue=req.queue, job_type=req.job_type)

    if req.payload is not None:
        m.payload.CopyFrom(payload_to_pb(req.payload))
    if req.idempotency_key is not None:
        m.idempotency_key = req.idempotency_key
    if req.priority is not None:
        m.priority = int(req.priority)
    if req.max_attempts is not None:
        m.max_attempts = req.max_attempts
    if req.trace_context is not None:
        m.trace_context.CopyFrom(trace_context_to_pb(req.trace_context))
    for key, value in req.custom.items():
        m.custom[key].CopyFrom(primitive_to_pb(value))
    if req.scheduled_at is not None:
        # Pre-epoch schedules are dropped rather than sent as a negative instant.
        if datetime_to_millis(req.scheduled_at) >= 0:
            m.scheduled_at.FromDatetime(req.scheduled_at)

    return m


def enqueue_ack_from_pb(r: pb.EnqueueResponse) -> EnqueueAck:
    return EnqueueAck(job_id=r.job_id, deduplicated=r.deduplicated)


def job_rejection_from_pb(r: pb.JobRejection) -> errors.JobRejection:
    which = r.WhichOneof("reason")

    if which is None:
        return errors.UnknownRejection()
    x = getattr(r, which)
    if which == "unknown_queue":
        return errors.UnknownQueue(queue=x.queue)
    if which == "payload_too_large":
        return errors.PayloadTooLarge(limit=x.limit, actual=x.actual)
    if which == "encoding_not_allowed":
        return errors.EncodingNotAllowed(encoding=x.encoding, allowed=list(x.allowed))
    if which == "job_type_not_allowed":
        return errors.JobTypeNotAllowed(job_type=x.job_type, allowed=list(x.allowed))
    if which == "custom_entries_too_many":
        return errors.CustomEntriesTooMany(limit=x.limit, actual=x.actual)
    if which == "custom_map_too_large":
        return errors.CustomMapTooLarge(limit=x.limit, actual=x.actual)
    if which == "custom_key_too_long":
        return errors.CustomKeyTooLong(key=x.key, limit=x.limit, actual=x.actual)
    if which == "queue_name_too_long":
        return errors.QueueNameTooLong(limit=x.limit, actual=x.actual)
    if which == "job_type_name_too_long":
        return errors.JobTypeNameTooLong(limit=x.limit, actual=x.actual)
    if which == "idempotency_key_too_long":
        return errors.IdempotencyKeyTooLong(limit=x.limit, actual=x.actual)
    if which == "scheduled_too_far":
        return errors.ScheduledTooFar(
            horizon=x.horizon.ToTimedelta(),
            actual=x.actual.ToDatetime(tzinfo=timezone.utc),
        )
    if which == "invalid_request":
        return errors.InvalidRequest(detail=x.message)
    if which == "queue_full":
        return errors.QueueFull(queue=x.queue, limit=x.limit)
    if which == "queue_closing":
        return errors.QueueClosing(queue=x.queue)

    return errors.UnknownRejection()


def job_validation_error_from_pb(e: pb.JobValidationError) -> errors.JobValidationError:
    rejection = (
        job_rejection_from_pb(e.rejection) if e.HasField("rejection") else errors.UnknownRejection()
    )

    return errors.JobValidationError(index=e.index, rejection=rejection)


def reserve_options_to_pb(opts: ReserveOptions) -> pb.ReserveRequest:
    m = pb.ReserveRequest(queues=list(opts.queues))
    m.wait_timeout.FromTimedelta(opts.wait_timeout)
    m.lease_duration.FromTimedelta(opts.lease_duration)

    if opts.worker_id is not None:
        m.worker_id = opts.worker_id
    if opts.max_jobs is not None:
        m.max_jobs = opts.max_jobs

    return m


def server_info_from_pb(r: pb.GetServerInfoResponse) -> ServerInfo:
    if not r.server_version:
        raise errors.ServerInfoError("server info is missing required field `server_version`")

    server_time = timestamp_to_datetime(r.server_time)
    if server_time is None:
        raise errors.ServerInfoError(
            f"server_time is not a representable time ({r.server_time.ToJsonString()})"
        )

    return ServerInfo(
        version=r.server_version,
        supported_protocol_versions=list(r.supported_protocol_versions),
        server_time=server_time,
        restricts_encodings=r.restricts_encodings,
        allowed_encodings=list(r.allowed_encodings),
        max_payload_bytes=r.max_payload_bytes,
        max_custom_entries=r.max_custom_entries,
        max_custom_total_bytes=r.max_custom_total_bytes,
        max_custom_key_bytes=r.max_custom_key_bytes,
        max_queue_name_bytes=r.max_queue_name_bytes,
        max_job_type_bytes=r.max_job_type_bytes,
        max_idempotency_key_bytes=r.max_idempotency_key_bytes,
        max_schedule_horizon=r.max_schedule_horizon.ToTimedelta(),
        max_enqueue_batch=r.max_enqueue_batch,
        max_reserve_batch=r.max_reserve_batch,
        max_reserve_queues=r.max_reserve_queues,
        max_wait_timeout=r.max_wait_timeout.ToTimedelta(),
        max_lease_duration=r.max_lease_duration.ToTimedelta(),
        strict_queues=r.strict_queues,
        dead_letter_retention_enabled=r.dead_letter_retention_enabled,
    )


def job_from_pb(client: SeppClient, j: pb.Job, worker_id: str | None) -> Job:
    from sepp.client import Lease

    if not j.id:
        raise errors.JobConversionError("job is missing required field `id`")
    if not j.job_type:
        raise errors.JobConversionError("job is missing required field `job_type`")

    try:
        priority = Priority(j.priority)
    except PriorityOutOfRangeError as exc:
        raise errors.JobConversionError(
            f"job priority {j.priority} is out of range (expected 0-9)"
        ) from exc

    enqueued_at = timestamp_to_datetime(j.enqueued_at)
    if enqueued_at is None:
        raise errors.JobConversionError(
            "job timestamp `enqueued_at` is not a representable time "
            f"({j.enqueued_at.ToJsonString()})"
        )
    lease_expires_at = timestamp_to_datetime(j.lease_expires_at)
    if lease_expires_at is None:
        raise errors.JobConversionError(
            "job timestamp `lease_expires_at` is not a representable time "
            f"({j.lease_expires_at.ToJsonString()})"
        )

    custom: dict[str, Primitive] = {}
    for key, pv in j.custom.items():
        value = primitive_from_pb(pv)
        if value is None:
            raise errors.JobConversionError(f"custom value for key `{key}` has no value set")
        custom[key] = value

    trace_context = trace_context_from_pb(j.trace_context) if j.HasField("trace_context") else None
    payload = payload_from_pb(j.payload) if j.HasField("payload") else None

    lease = Lease(client, j.id, j.attempt, lease_expires_at, worker_id)
    ctx = JobCtx(
        id=j.id,
        queue=j.queue,
        job_type=j.job_type,
        priority=priority,
        attempt=j.attempt,
        max_attempts=j.max_attempts,
        enqueued_at=enqueued_at,
        custom=custom,
        trace_context=trace_context,
        lease_expires_at=lease_expires_at,
        scheduled_at=_scheduled_at_from_pb(j),
        _lease=lease,
    )

    return Job(payload=payload, ctx=ctx)


def _scheduled_at_from_pb(j: pb.Job) -> datetime | None:
    # Unset means "was available immediately".
    if not j.HasField("scheduled_at"):
        return None
    return timestamp_to_datetime(j.scheduled_at)


_CAUSE_FROM_PB = {
    pb.DeadLetterCause.DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED: DeadLetterCause.ATTEMPTS_EXHAUSTED,
    pb.DeadLetterCause.DEAD_LETTER_CAUSE_REJECTED: DeadLetterCause.REJECTED,
    pb.DeadLetterCause.DEAD_LETTER_CAUSE_LEASE_EXPIRED: DeadLetterCause.LEASE_EXPIRED,
    pb.DeadLetterCause.DEAD_LETTER_CAUSE_ADMIN: DeadLetterCause.ADMIN,
}


def dead_letter_record_from_pb(r: pb.DeadLetterRecord) -> DeadLetterRecord:
    if not r.HasField("job"):
        raise errors.JobConversionError("dead-letter record is missing required field `job`")

    j = r.job
    if not j.id:
        raise errors.JobConversionError("job is missing required field `id`")
    if not j.job_type:
        raise errors.JobConversionError("job is missing required field `job_type`")

    try:
        priority = Priority(j.priority)
    except PriorityOutOfRangeError as exc:
        raise errors.JobConversionError(
            f"job priority {j.priority} is out of range (expected 0-9)"
        ) from exc

    enqueued_at = timestamp_to_datetime(j.enqueued_at)
    if enqueued_at is None:
        raise errors.JobConversionError(
            "job timestamp `enqueued_at` is not a representable time "
            f"({j.enqueued_at.ToJsonString()})"
        )
    failed_at = timestamp_to_datetime(r.failed_at)
    if failed_at is None:
        raise errors.JobConversionError(
            "dead-letter timestamp `failed_at` is not a representable time "
            f"({r.failed_at.ToJsonString()})"
        )

    custom: dict[str, Primitive] = {}
    for key, pv in j.custom.items():
        value = primitive_from_pb(pv)
        if value is None:
            raise errors.JobConversionError(f"custom value for key `{key}` has no value set")
        custom[key] = value

    trace_context = trace_context_from_pb(j.trace_context) if j.HasField("trace_context") else None
    payload = payload_from_pb(j.payload) if j.HasField("payload") else None

    return DeadLetterRecord(
        queue=j.queue,
        job_id=j.id,
        job_type=j.job_type,
        payload=payload,
        priority=priority,
        max_attempts=j.max_attempts,
        custom=custom,
        trace_context=trace_context,
        enqueued_at=enqueued_at,
        scheduled_at=_scheduled_at_from_pb(j),
        cause=_CAUSE_FROM_PB.get(r.cause, DeadLetterCause.UNSPECIFIED),
        failed_at=failed_at,
        final_attempt=r.final_attempt,
        last_reason=r.last_reason if r.HasField("last_reason") else None,
    )
