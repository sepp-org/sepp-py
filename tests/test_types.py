"""Tests for the domain value types: Priority, TraceContext, EnqueueRequest,
ReserveOptions, JobCtx."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from tests.conftest import VALID_TP

from sepp import (
    EnqueueRequest,
    JobCtx,
    Payload,
    Priority,
    PriorityOutOfRangeError,
    ReserveOptions,
    TraceContext,
    TraceContextError,
)

# -- Priority ---------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 5, 9])
def test_priority_valid(value: int) -> None:
    assert int(Priority(value)) == value


@pytest.mark.parametrize("value", [10, 255, -1])
def test_priority_out_of_range(value: int) -> None:
    with pytest.raises(PriorityOutOfRangeError):
        Priority(value)


def test_priority_constants() -> None:
    assert Priority.MIN == Priority.P0 == Priority(0)
    assert Priority.MAX == Priority.P9 == Priority(9)
    assert Priority.P7 == Priority(7)


def test_priority_equality_and_ordering() -> None:
    assert Priority(3) == Priority(3)
    assert Priority(3) < Priority(4)
    assert {Priority(1), Priority(1)} == {Priority(1)}


def test_priority_rejects_bool() -> None:
    with pytest.raises(PriorityOutOfRangeError):
        Priority(True)  # type: ignore[arg-type]


# -- TraceContext -----------------------------------------------------------


def test_traceparent_valid() -> None:
    tc = TraceContext(VALID_TP)
    assert tc.traceparent == VALID_TP
    assert tc.tracestate is None


def test_traceparent_with_tracestate() -> None:
    tc = TraceContext(VALID_TP, "vendor=abc")
    assert tc.tracestate == "vendor=abc"


@pytest.mark.parametrize(
    "tp",
    [
        "00-deadbeef-0123",  # wrong field count
        "0-0123456789abcdef0123456789abcdef-0123456789abcdef-01",  # bad version length
        "0g-0123456789abcdef0123456789abcdef-0123456789abcdef-01",  # non-hex version
        "00-deadbeef-0123456789abcdef-01",  # bad trace_id length
        "00-0123456789abcdeg0123456789abcdef-0123456789abcdef-01",  # non-hex trace_id
        "00-00000000000000000000000000000000-0123456789abcdef-01",  # all-zero trace_id
        "00-0123456789abcdef0123456789abcdef-deadbeef-01",  # bad span_id length
        "00-0123456789abcdef0123456789abcdef-0123456789abcdeg-01",  # non-hex span_id
        "00-0123456789abcdef0123456789abcdef-0000000000000000-01",  # all-zero span_id
        "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0",  # bad flags length
        "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0g",  # non-hex flags
    ],
)
def test_traceparent_invalid(tp: str) -> None:
    with pytest.raises(TraceContextError):
        TraceContext(tp)


def test_otel_span_context_roundtrip() -> None:
    # With the otel extra installed, a valid traceparent decodes to a valid
    # SpanContext (used to hand-build a worker->producer span link).
    from sepp import _otel

    if not _otel.OTEL_AVAILABLE:
        pytest.skip("otel extra not installed")
    sc = TraceContext(VALID_TP).otel_span_context()
    assert sc is not None
    assert format(sc.span_id, "016x") == "0123456789abcdef"


# -- EnqueueRequest ---------------------------------------------------------


def test_enqueue_request_empty_queue() -> None:
    with pytest.raises(ValueError, match="queue"):
        EnqueueRequest("", "type")


def test_enqueue_request_empty_job_type() -> None:
    with pytest.raises(ValueError, match="job type"):
        EnqueueRequest("q", "")


def test_enqueue_request_defaults() -> None:
    req = EnqueueRequest("q", "t")
    assert req.payload is None
    assert req.priority is None
    assert req.custom == {}


def test_enqueue_request_coerces_int_priority() -> None:
    req = EnqueueRequest("q", "t", priority=7)
    assert req.priority == Priority(7)


def test_enqueue_request_accepts_priority_object() -> None:
    req = EnqueueRequest("q", "t", priority=Priority.P3)
    assert req.priority == Priority(3)


def test_enqueue_request_int_priority_out_of_range() -> None:
    with pytest.raises(PriorityOutOfRangeError):
        EnqueueRequest("q", "t", priority=10)


def test_enqueue_request_distinct_custom_maps() -> None:
    a = EnqueueRequest("q", "t")
    b = EnqueueRequest("q", "t")
    a.custom["k"] = 1
    assert b.custom == {}


# -- ReserveOptions ---------------------------------------------------------


def test_reserve_options_empty_queues() -> None:
    with pytest.raises(ValueError, match="at least one queue"):
        ReserveOptions([], timedelta(seconds=1))


def test_reserve_options_empty_queue_name() -> None:
    with pytest.raises(ValueError, match="index 1"):
        ReserveOptions(["ok", ""], timedelta(seconds=1))


def test_reserve_options_zero_lease() -> None:
    with pytest.raises(ValueError, match="lease_duration"):
        ReserveOptions(["q"], timedelta(0))


def test_reserve_options_empty_worker_id() -> None:
    with pytest.raises(ValueError, match="worker_id"):
        ReserveOptions(["q"], timedelta(seconds=1), worker_id="")


def test_reserve_options_default_wait_timeout() -> None:
    opts = ReserveOptions(["q"], timedelta(seconds=1))
    assert opts.wait_timeout == timedelta(seconds=30)


def test_reserve_options_wait_timeout_override() -> None:
    opts = ReserveOptions(["q"], timedelta(seconds=1), wait_timeout=timedelta(milliseconds=500))
    assert opts.wait_timeout == timedelta(milliseconds=500)


# -- Payload / JobCtx -------------------------------------------------------


def test_payload_fields() -> None:
    p = Payload(b"abc", "text/plain")
    assert p.data == b"abc"
    assert p.encoding == "text/plain"


def test_jobctx_str() -> None:
    ctx = JobCtx(
        id="abc",
        queue="emails",
        job_type="send_email",
        priority=Priority(3),
        attempt=2,
        max_attempts=5,
        enqueued_at=datetime.now(timezone.utc),
        custom={},
        trace_context=None,
        lease_expires_at=datetime.now(timezone.utc),
    )
    s = str(ctx)
    assert "abc" in s and "send_email" in s and "2/5" in s


async def test_jobctx_extend_without_lease_raises() -> None:
    ctx = JobCtx(
        id="abc",
        queue="q",
        job_type="t",
        priority=Priority(0),
        attempt=1,
        max_attempts=1,
        enqueued_at=datetime.now(timezone.utc),
        custom={},
        trace_context=None,
        lease_expires_at=datetime.now(timezone.utc),
    )
    with pytest.raises(RuntimeError):
        await ctx.extend(timedelta(seconds=1))


# -- ReserveOptions additional validation -----------------------------------


def test_reserve_options_max_jobs_less_than_one() -> None:
    with pytest.raises(ValueError, match="max_jobs"):
        ReserveOptions(["q"], timedelta(seconds=1), max_jobs=0)


def test_reserve_options_max_jobs_negative() -> None:
    with pytest.raises(ValueError, match="max_jobs"):
        ReserveOptions(["q"], timedelta(seconds=1), max_jobs=-1)


def test_reserve_options_negative_lease() -> None:
    with pytest.raises(ValueError, match="lease_duration"):
        ReserveOptions(["q"], timedelta(seconds=-5))


# -- EnqueueRequest additional validation -----------------------------------


def test_enqueue_request_bool_priority_raises() -> None:
    with pytest.raises(PriorityOutOfRangeError):
        EnqueueRequest("q", "t", priority=True)  # type: ignore[arg-type]
