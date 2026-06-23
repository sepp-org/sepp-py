"""Tests for the OpenTelemetry integration module (_otel.py)."""

from __future__ import annotations

from unittest import mock

import pytest

from sepp import _otel
from sepp.types import TraceContext

# -- enabled ----------------------------------------------------------------


def test_enabled() -> None:
    assert isinstance(_otel.enabled(), bool)


# -- no-op paths (OTEL not installed) ---------------------------------------


@pytest.fixture
def otel_disabled() -> None:
    with mock.patch.object(_otel, "OTEL_AVAILABLE", False):
        yield


def test_inject_context_metadata_noop(otel_disabled: None) -> None:  # noqa: ARG001
    meta: list[tuple[str, str]] = []
    _otel.inject_context_metadata(meta)
    assert meta == []


def test_current_trace_headers_noop(otel_disabled: None) -> None:  # noqa: ARG001
    assert _otel.current_trace_headers() is None


def test_trace_context_from_current_noop(otel_disabled: None) -> None:  # noqa: ARG001
    assert _otel.trace_context_from_current() is None


def test_attach_trace_context_noop(otel_disabled: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    with _otel.attach_trace_context(tc):
        pass


def test_span_context_from_trace_context_noop(otel_disabled: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    assert _otel.span_context_from_trace_context(tc) is None


def test_client_span_noop(otel_disabled: None) -> None:  # noqa: ARG001
    with _otel.client_span("test.span"):
        pass


def test_consumer_span_noop(otel_disabled: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    with _otel.consumer_span("test.span", tc):
        pass


def test_consumer_span_none_producer_noop(otel_disabled: None) -> None:  # noqa: ARG001
    with _otel.consumer_span("test.span", None):
        pass


def test_worker_metrics_noop(otel_disabled: None) -> None:  # noqa: ARG001
    m = _otel.WorkerMetrics()
    m.record_processed()
    m.record_nacked(False)
    m.record_nacked(True)
    m.record_in_flight_delta(1)
    m.record_in_flight_delta(-1)
    m.record_reserve_ok(empty=True)
    m.record_reserve_ok(empty=False)
    m.record_reserve_failed()


def test_create_sepp_logger_noop(otel_disabled: None) -> None:  # noqa: ARG001
    logger = _otel.create_sepp_logger("test_logger")
    assert not isinstance(logger, _otel._SeppLogger)


# -- OTEL-enabled paths -----------------------------------------------------


@pytest.fixture
def otel_configured() -> None:
    if not _otel.OTEL_AVAILABLE:
        pytest.skip("otel extra not installed")
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    from opentelemetry import trace as _trace

    original = _trace.get_tracer_provider()
    _trace.set_tracer_provider(provider)
    try:
        yield
    finally:
        _trace.set_tracer_provider(original)


def test_inject_context_metadata_appends_headers(otel_configured: None) -> None:  # noqa: ARG001
    meta: list[tuple[str, str]] = []
    with _otel.client_span("test.span"):
        _otel.inject_context_metadata(meta)
    found = {k for k, _ in meta}
    assert "traceparent" in found


def test_inject_context_metadata_no_active_span(otel_configured: None) -> None:  # noqa: ARG001
    meta: list[tuple[str, str]] = []
    _otel.inject_context_metadata(meta)
    assert meta == []


def test_current_trace_headers_returns_tuple(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.client_span("test.span"):
        headers = _otel.current_trace_headers()
    assert headers is not None
    assert len(headers) == 2
    assert headers[0].startswith("00-")


def test_current_trace_headers_no_active_span(otel_configured: None) -> None:  # noqa: ARG001
    assert _otel.current_trace_headers() is None


def test_trace_context_from_current_yields_valid(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.client_span("test.span"):
        tc = _otel.trace_context_from_current()
    assert tc is not None
    assert tc.traceparent.startswith("00-")


def test_attach_trace_context(otel_configured: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    with _otel.attach_trace_context(tc):
        headers = _otel.current_trace_headers()
    assert headers is not None
    assert headers[0] == tc.traceparent


def test_span_context_from_trace_context_valid(otel_configured: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    sc = _otel.span_context_from_trace_context(tc)
    assert sc is not None


def test_client_span_opens_span(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.client_span("sepp.test"):
        headers = _otel.current_trace_headers()
    assert headers is not None


def test_consumer_span_opens_span(otel_configured: None) -> None:  # noqa: ARG001
    tc = TraceContext("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    with _otel.consumer_span("sepp.test", tc):
        headers = _otel.current_trace_headers()
    assert headers is not None


def test_consumer_span_none_producer(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.consumer_span("sepp.test", None):
        headers = _otel.current_trace_headers()
    assert headers is not None


def test_worker_metrics_all_instruments(otel_configured: None) -> None:  # noqa: ARG001
    m = _otel.WorkerMetrics()
    assert m._enabled is True
    m.record_processed()
    m.record_nacked(False)
    m.record_nacked(True)
    m.record_in_flight_delta(1)
    m.record_in_flight_delta(-3)
    m.record_reserve_ok(empty=True)
    m.record_reserve_ok(empty=False)
    m.record_reserve_failed()


def test_create_sepp_logger_is_sepp_logger(otel_configured: None) -> None:  # noqa: ARG001
    logger = _otel.create_sepp_logger("test_logger")
    assert isinstance(logger, _otel._SeppLogger)


def test_sepp_logger_delegates_to_standard_logger(otel_configured: None) -> None:  # noqa: ARG001
    logger = _otel.create_sepp_logger("test_sepp_logger")
    logger.debug("debug msg")
    logger.info("info msg")
    logger.warning("warning msg")
    logger.error("error msg")


def test_sepp_logger_formats_args(otel_configured: None) -> None:  # noqa: ARG001
    logger = _otel.create_sepp_logger("test_sepp_logger_fmt")
    logger.info("hello %s", "world")


def test_emit_span_event_no_active_span(otel_configured: None) -> None:  # noqa: ARG001
    _otel._emit_span_event(9, "test message", ())  # type: ignore[arg-type]


def test_emit_span_event_with_active_span(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.client_span("sepp.test"):
        _otel._emit_span_event(9, "test message", ())  # type: ignore[arg-type]


def test_emit_span_event_with_args(otel_configured: None) -> None:  # noqa: ARG001
    with _otel.client_span("sepp.test"):
        _otel._emit_span_event(13, "warning: %s", ("details",))  # type: ignore[arg-type]
