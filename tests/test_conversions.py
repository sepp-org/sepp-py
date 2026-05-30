"""Tests for the protobuf <-> domain conversion layer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from tests.conftest import (
    VALID_TP,
    VALID_UUID,
    FakeStub,  # noqa: F401  (kept for symmetry)
    make_client,
)

from sepp import _convert, errors
from sepp._pb import queue_pb2 as pb
from sepp.types import DeadLetterCause, EnqueueRequest, Payload, Priority, TraceContext

# -- time helpers -----------------------------------------------------------


def test_millis_to_datetime_zero() -> None:
    assert _convert.millis_to_datetime(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_millis_to_datetime_negative_is_none() -> None:
    assert _convert.millis_to_datetime(-1) is None


def test_datetime_to_millis_roundtrip() -> None:
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=42)
    assert _convert.datetime_to_millis(dt) == 42


def test_datetime_to_millis_epoch() -> None:
    assert _convert.datetime_to_millis(datetime(1970, 1, 1, tzinfo=timezone.utc)) == 0


def test_timedelta_to_millis() -> None:
    assert _convert.timedelta_to_millis(timedelta(seconds=5)) == 5000


def test_datetime_to_millis_exact_integer_math() -> None:
    # A far-future, ms-aligned instant where float (timestamp()*1000) truncates
    # off-by-one; exact integer arithmetic must give the true value.
    dt = datetime(2038, 5, 20, 21, 36, 29, 437000, tzinfo=timezone.utc)
    assert _convert.datetime_to_millis(dt) == 2158004189437


def test_datetime_to_millis_truncates_submillisecond() -> None:
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=1500)
    assert _convert.datetime_to_millis(dt) == 1  # 1.5ms truncates to 1, like Rust


# -- primitives -------------------------------------------------------------


def test_primitive_to_pb_string() -> None:
    pv = _convert.primitive_to_pb("x")
    assert pv.WhichOneof("value") == "string_value" and pv.string_value == "x"


def test_primitive_to_pb_int() -> None:
    pv = _convert.primitive_to_pb(7)
    assert pv.WhichOneof("value") == "int_value" and pv.int_value == 7


def test_primitive_to_pb_double() -> None:
    pv = _convert.primitive_to_pb(2.5)
    assert pv.WhichOneof("value") == "double_value" and pv.double_value == 2.5


def test_primitive_to_pb_bool_before_int() -> None:
    pv = _convert.primitive_to_pb(True)
    assert pv.WhichOneof("value") == "bool_value" and pv.bool_value is True


def test_primitive_from_pb_roundtrip() -> None:
    for value in ("x", 7, 2.5, True):
        pv = _convert.primitive_to_pb(value)
        assert _convert.primitive_from_pb(pv) == value


def test_primitive_from_pb_empty_is_none() -> None:
    assert _convert.primitive_from_pb(pb.PrimitiveValue()) is None


def test_primitive_to_pb_rejects_other() -> None:
    with pytest.raises(TypeError):
        _convert.primitive_to_pb(object())  # type: ignore[arg-type]


# -- payload / trace context ------------------------------------------------


def test_payload_roundtrip() -> None:
    p = Payload(b"\x01\x02", "json")
    m = _convert.payload_to_pb(p)
    assert m.data == b"\x01\x02" and m.encoding == "json"
    assert _convert.payload_from_pb(m) == p


def test_trace_context_to_pb() -> None:
    m = _convert.trace_context_to_pb(TraceContext(VALID_TP, "v=1"))
    assert m.traceparent == VALID_TP and m.tracestate == "v=1"


def test_trace_context_from_pb_valid() -> None:
    m = pb.TraceContext(traceparent=VALID_TP)
    m.tracestate = "v=1"
    tc = _convert.trace_context_from_pb(m)
    assert tc is not None and tc.traceparent == VALID_TP and tc.tracestate == "v=1"


def test_trace_context_from_pb_invalid_is_none() -> None:
    assert _convert.trace_context_from_pb(pb.TraceContext(traceparent="garbage")) is None


# -- enqueue request --------------------------------------------------------


def test_enqueue_request_to_pb_minimal() -> None:
    m = _convert.enqueue_request_to_pb(EnqueueRequest("q", "t"))
    assert m.queue == "q" and m.job_type == "t"
    assert not m.HasField("payload")
    assert not m.HasField("priority")
    assert not m.HasField("scheduled_at")
    assert len(m.custom) == 0


def test_enqueue_request_to_pb_all_fields() -> None:
    req = EnqueueRequest(
        "q",
        "t",
        payload=Payload(b"\x01", "raw"),
        idempotency_key="idem",
        priority=Priority(7),
        max_attempts=5,
        custom={"k": 1},
        trace_context=TraceContext(VALID_TP),
        scheduled_at=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=1234),
    )
    m = _convert.enqueue_request_to_pb(req)
    assert m.payload.encoding == "raw" and m.payload.data == b"\x01"
    assert m.idempotency_key == "idem"
    assert m.priority == 7
    assert m.max_attempts == 5
    assert m.custom["k"].int_value == 1
    assert m.HasField("trace_context")
    assert m.scheduled_at == 1234


def test_enqueue_request_scheduled_pre_epoch_dropped() -> None:
    req = EnqueueRequest(
        "q", "t", scheduled_at=datetime(1970, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    )
    m = _convert.enqueue_request_to_pb(req)
    assert not m.HasField("scheduled_at")


def test_enqueue_ack_from_pb() -> None:
    ack = _convert.enqueue_ack_from_pb(pb.EnqueueResponse(job_id="abc", deduplicated=True))
    assert ack.job_id == "abc" and ack.deduplicated is True


# -- job rejection ----------------------------------------------------------


def _rej(**kwargs: object) -> pb.JobRejection:
    return pb.JobRejection(**kwargs)  # type: ignore[arg-type]


def test_job_rejection_unknown_queue() -> None:
    r = _convert.job_rejection_from_pb(_rej(unknown_queue=pb.UnknownQueue(queue="q")))
    assert isinstance(r, errors.UnknownQueue) and r.queue == "q"


def test_job_rejection_payload_too_large() -> None:
    r = _convert.job_rejection_from_pb(
        _rej(payload_too_large=pb.PayloadTooLarge(limit=10, actual=20))
    )
    assert isinstance(r, errors.PayloadTooLarge) and r.limit == 10 and r.actual == 20


def test_job_rejection_encoding_not_allowed() -> None:
    r = _convert.job_rejection_from_pb(
        _rej(encoding_not_allowed=pb.EncodingNotAllowed(encoding="gzip", allowed=["json"]))
    )
    assert isinstance(r, errors.EncodingNotAllowed)
    assert r.encoding == "gzip" and r.allowed == ["json"]


def test_job_rejection_custom_key_too_long() -> None:
    r = _convert.job_rejection_from_pb(
        _rej(custom_key_too_long=pb.CustomKeyTooLong(key="k", limit=1, actual=2))
    )
    assert isinstance(r, errors.CustomKeyTooLong)
    assert r.key == "k" and r.limit == 1 and r.actual == 2


def test_job_rejection_scheduled_too_far() -> None:
    r = _convert.job_rejection_from_pb(
        _rej(scheduled_too_far=pb.ScheduledTooFar(horizon_ms=60_000, actual_ms=120_000))
    )
    assert isinstance(r, errors.ScheduledTooFar)
    assert r.horizon_ms == 60_000 and r.actual_ms == 120_000


def test_job_rejection_invalid_request() -> None:
    r = _convert.job_rejection_from_pb(_rej(invalid_request=pb.InvalidRequest(message="oops")))
    assert isinstance(r, errors.InvalidRequest) and r.detail == "oops"


def test_job_rejection_none_is_unknown() -> None:
    r = _convert.job_rejection_from_pb(pb.JobRejection())
    assert isinstance(r, errors.UnknownRejection)


def test_job_rejection_messages_render() -> None:
    # Every variant should produce a non-empty human-readable message.
    for r in (
        errors.UnknownQueue("q"),
        errors.PayloadTooLarge(1, 2),
        errors.EncodingNotAllowed("g", ["j"]),
        errors.JobTypeNotAllowed("x", ["y"]),
        errors.CustomEntriesTooMany(1, 2),
        errors.CustomMapTooLarge(1, 2),
        errors.CustomKeyTooLong("k", 1, 2),
        errors.QueueNameTooLong(1, 2),
        errors.JobTypeNameTooLong(1, 2),
        errors.IdempotencyKeyTooLong(1, 2),
        errors.ScheduledTooFar(1, 2),
        errors.InvalidRequest("m"),
        errors.UnknownRejection(),
    ):
        assert str(r)


def test_job_validation_error_from_pb() -> None:
    e = _convert.job_validation_error_from_pb(
        pb.JobValidationError(
            index=3, rejection=pb.JobRejection(unknown_queue=pb.UnknownQueue(queue="q"))
        )
    )
    assert e.index == 3 and isinstance(e.rejection, errors.UnknownQueue)


def test_job_validation_error_missing_rejection_is_unknown() -> None:
    e = _convert.job_validation_error_from_pb(pb.JobValidationError(index=1))
    assert isinstance(e.rejection, errors.UnknownRejection)


# -- reserve options --------------------------------------------------------


def test_reserve_options_to_pb() -> None:
    from sepp.types import ReserveOptions

    opts = ReserveOptions(
        ["q1", "q2"],
        timedelta(seconds=5),
        wait_timeout=timedelta(seconds=2),
        worker_id="w",
        max_jobs=7,
    )
    m = _convert.reserve_options_to_pb(opts)
    assert list(m.queues) == ["q1", "q2"]
    assert m.wait_timeout_ms == 2000
    assert m.lease_duration_ms == 5000
    assert m.worker_id == "w"
    assert m.max_jobs == 7


# -- server info ------------------------------------------------------------


def _valid_server_info() -> pb.GetServerInfoResponse:
    return pb.GetServerInfoResponse(
        server_version="1.2.3",
        supported_protocol_versions=["v1"],
        server_time_ms=1_700_000_000_000,
        restricts_encodings=False,
        allowed_encodings=["json"],
        max_payload_bytes=1024,
        max_custom_entries=10,
        max_custom_total_bytes=2048,
        max_custom_key_bytes=64,
        max_queue_name_bytes=512,
        max_job_type_bytes=256,
        max_idempotency_key_bytes=128,
        max_schedule_horizon_ms=86_400_000,
        max_enqueue_batch=100,
        max_reserve_batch=50,
        max_reserve_queues=8,
        max_wait_timeout_ms=30_000,
        max_lease_duration_ms=60_000,
        strict_queues=True,
    )


def test_server_info_happy_path() -> None:
    info = _convert.server_info_from_pb(_valid_server_info())
    assert info.version == "1.2.3"
    assert info.max_payload_size == 1024
    assert info.max_lease_duration_ms == 60_000
    assert info.strict_queues is True
    assert info.dead_letter_retention_enabled is False
    assert info.server_time == datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        milliseconds=1_700_000_000_000
    )


def test_server_info_missing_version() -> None:
    msg = _valid_server_info()
    msg.server_version = ""
    with pytest.raises(errors.ServerInfoError, match="server_version"):
        _convert.server_info_from_pb(msg)


def test_server_info_invalid_time() -> None:
    msg = _valid_server_info()
    msg.server_time_ms = -1
    with pytest.raises(errors.ServerInfoError, match="server_time_ms"):
        _convert.server_info_from_pb(msg)


# -- job_from_pb ------------------------------------------------------------


def _valid_job() -> pb.Job:
    return pb.Job(
        id=VALID_UUID,
        queue="emails",
        job_type="send_email",
        priority=3,
        enqueued_at=1_700_000_000_000,
        attempt=1,
        max_attempts=5,
        lease_expires_at=1_700_000_060_000,
    )


def test_job_from_pb_happy_path() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.payload.data = b"\x01\x02"
    j.payload.encoding = "json"
    j.custom["k"].string_value = "v"
    job = _convert.job_from_pb(client, j, None)
    assert job.ctx.id == VALID_UUID
    assert job.ctx.queue == "emails"
    assert job.ctx.job_type == "send_email"
    assert job.ctx.priority == Priority(3)
    assert job.ctx.attempt == 1 and job.ctx.max_attempts == 5
    assert job.payload is not None and job.payload.encoding == "json"
    assert job.ctx.custom["k"] == "v"
    assert job.ctx._lease is not None


def test_job_from_pb_missing_id() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.id = ""
    with pytest.raises(errors.JobConversionError, match="id"):
        _convert.job_from_pb(client, j, None)


def test_job_from_pb_missing_job_type() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.job_type = ""
    with pytest.raises(errors.JobConversionError, match="job_type"):
        _convert.job_from_pb(client, j, None)


@pytest.mark.parametrize("priority", [10, 300])
def test_job_from_pb_priority_out_of_range(priority: int) -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.priority = priority
    with pytest.raises(errors.JobConversionError, match="priority"):
        _convert.job_from_pb(client, j, None)


def test_job_from_pb_invalid_enqueued_at() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.enqueued_at = -1
    with pytest.raises(errors.JobConversionError, match="enqueued_at"):
        _convert.job_from_pb(client, j, None)


def test_job_from_pb_invalid_lease_expires_at() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.lease_expires_at = -5
    with pytest.raises(errors.JobConversionError, match="lease_expires_at"):
        _convert.job_from_pb(client, j, None)


def test_job_from_pb_empty_custom_value() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.custom["k"].CopyFrom(pb.PrimitiveValue())
    with pytest.raises(errors.JobConversionError, match="no value set"):
        _convert.job_from_pb(client, j, None)


def test_job_from_pb_drops_invalid_trace_context() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.trace_context.traceparent = "garbage"
    job = _convert.job_from_pb(client, j, None)
    assert job.ctx.trace_context is None


def test_job_from_pb_preserves_valid_trace_context() -> None:
    client = make_client(FakeStub())
    j = _valid_job()
    j.trace_context.traceparent = VALID_TP
    j.trace_context.tracestate = "v=1"
    job = _convert.job_from_pb(client, j, None)
    assert job.ctx.trace_context is not None
    assert job.ctx.trace_context.traceparent == VALID_TP
    assert job.ctx.trace_context.tracestate == "v=1"


# -- dead_letter_record_from_pb ---------------------------------------------


def _valid_dead_letter() -> pb.DeadLetterRecord:
    return pb.DeadLetterRecord(
        job=_valid_job(),
        cause=pb.DeadLetterCause.DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED,
        failed_at=1_700_000_100_000,
        final_attempt=5,
        last_reason="boom",
    )


def test_dead_letter_record_from_pb() -> None:
    r = _convert.dead_letter_record_from_pb(_valid_dead_letter())
    assert r.job_id == VALID_UUID
    assert r.queue == "emails"
    assert r.job_type == "send_email"
    assert r.cause == DeadLetterCause.ATTEMPTS_EXHAUSTED
    assert r.final_attempt == 5
    assert r.last_reason == "boom"
    assert r.priority == Priority(3)
    assert r.failed_at == datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        milliseconds=1_700_000_100_000
    )


def test_dead_letter_record_missing_job() -> None:
    msg = _valid_dead_letter()
    msg.ClearField("job")
    with pytest.raises(errors.JobConversionError, match="job"):
        _convert.dead_letter_record_from_pb(msg)


def test_dead_letter_record_lease_expired_has_no_reason() -> None:
    msg = _valid_dead_letter()
    msg.cause = pb.DeadLetterCause.DEAD_LETTER_CAUSE_LEASE_EXPIRED
    msg.ClearField("last_reason")
    r = _convert.dead_letter_record_from_pb(msg)
    assert r.cause == DeadLetterCause.LEASE_EXPIRED
    assert r.last_reason is None


def test_dead_letter_record_unknown_cause_is_unspecified() -> None:
    msg = _valid_dead_letter()
    msg.cause = 9999
    r = _convert.dead_letter_record_from_pb(msg)
    assert r.cause == DeadLetterCause.UNSPECIFIED


def test_dead_letter_record_replays_into_its_queue() -> None:
    msg = _valid_dead_letter()
    msg.job.payload.data = b"\x01\x02\x03"
    msg.job.payload.encoding = "json"
    req = _convert.dead_letter_record_from_pb(msg).to_enqueue_request()
    assert isinstance(req, EnqueueRequest)
    assert req.queue == "emails"
    assert req.job_type == "send_email"
    assert req.priority == Priority(3)
    assert req.payload is not None and req.payload.data == b"\x01\x02\x03"
