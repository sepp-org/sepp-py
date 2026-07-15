"""Optional OpenTelemetry integration, installed via the ``otel`` extra.

Opens client/consumer spans, propagates W3C trace context from the producer's
enqueue span to the worker's process span, and exposes worker metrics. When
OpenTelemetry is not installed, every entry point here degrades to a no-op.

The host application still owns the exporter/provider; this module only emits
spans and metrics into whatever the host has configured (or the global no-op
providers).
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sepp.types import TraceContext

try:
    from opentelemetry import context as _otel_context
    from opentelemetry import metrics as _metrics
    from opentelemetry import trace as _trace
    from opentelemetry.trace import Link, SpanKind
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    _PROPAGATOR = TraceContextTextMapPropagator()
    _TRACER = _trace.get_tracer("sepp")
    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the otel extra
    OTEL_AVAILABLE = False


def enabled() -> bool:
    """Whether the ``otel`` extra is installed."""
    return OTEL_AVAILABLE


def inject_context_metadata(metadata: list[tuple[str, str]]) -> None:
    """Append W3C trace headers for the current context onto a gRPC metadata
    list. No-op when OpenTelemetry is absent or there is no active span."""
    if not OTEL_AVAILABLE:
        return
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier)
    tp = carrier.get("traceparent")
    if tp is not None:
        metadata.append(("traceparent", tp))
    ts = carrier.get("tracestate")
    if ts:
        metadata.append(("tracestate", ts))


def current_trace_headers() -> tuple[str, str | None] | None:
    """Return ``(traceparent, tracestate)`` for the current context, or ``None``
    if there is no valid active span (or no OpenTelemetry)."""
    if not OTEL_AVAILABLE:
        return None
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier)
    tp = carrier.get("traceparent")
    if tp is None:
        return None
    return tp, carrier.get("tracestate")


def trace_context_from_current() -> TraceContext | None:
    """Capture the current context as a :class:`~sepp.types.TraceContext`."""
    headers = current_trace_headers()
    if headers is None:
        return None
    from sepp.types import TraceContext, TraceContextError

    traceparent, tracestate = headers
    try:
        return TraceContext(traceparent, tracestate)
    except TraceContextError:
        return None


@contextlib.contextmanager
def attach_trace_context(tc: TraceContext) -> Iterator[None]:
    """Install ``tc`` as the current OpenTelemetry context for the duration of
    the ``with`` block."""
    if not OTEL_AVAILABLE:
        yield
        return
    carrier = {"traceparent": tc.traceparent}
    if tc.tracestate is not None:
        carrier["tracestate"] = tc.tracestate
    ctx = _PROPAGATOR.extract(carrier)
    token = _otel_context.attach(ctx)
    try:
        yield
    finally:
        _otel_context.detach(token)


def span_context_from_trace_context(tc: TraceContext) -> Any:
    """Decode a :class:`~sepp.types.TraceContext` into an OpenTelemetry
    ``SpanContext``, or ``None`` if it is not a valid span (or no OTel)."""
    if not OTEL_AVAILABLE:
        return None
    carrier = {"traceparent": tc.traceparent}
    if tc.tracestate is not None:
        carrier["tracestate"] = tc.tracestate
    ctx = _PROPAGATOR.extract(carrier)
    span_context = _trace.get_current_span(ctx).get_span_context()
    return span_context if span_context.is_valid else None


def _link_from_trace_context(tc: TraceContext) -> Any:
    """Build a span :class:`Link` back to the producer, or ``None``."""
    span_context = span_context_from_trace_context(tc)
    return Link(span_context) if span_context is not None else None


@contextlib.contextmanager
def client_span(name: str) -> Iterator[None]:
    """Open a CLIENT-kind span around an outbound RPC. No-op without OTel."""
    if not OTEL_AVAILABLE:
        yield
        return
    with _TRACER.start_as_current_span(name, kind=SpanKind.CLIENT):
        yield


@contextlib.contextmanager
def consumer_span(name: str, producer: TraceContext | None) -> Iterator[None]:
    """Open a CONSUMER-kind span around job processing, linked back to the
    producer's enqueue span when a trace context is present. No-op without
    OTel."""
    if not OTEL_AVAILABLE:
        yield
        return
    links = []
    if producer is not None:
        link = _link_from_trace_context(producer)
        if link is not None:
            links.append(link)
    with _TRACER.start_as_current_span(name, kind=SpanKind.CONSUMER, links=links):
        yield


class WorkerMetrics:
    """The worker's OpenTelemetry instruments, or no-ops when OTel is absent."""

    def __init__(self) -> None:
        self._enabled = OTEL_AVAILABLE
        if not self._enabled:
            return
        meter = _metrics.get_meter("sepp")
        self._processed = meter.create_counter(
            "sepp.jobs.processed", description="Jobs successfully acked."
        )
        self._nacked = meter.create_counter(
            "sepp.jobs.nacked",
            description="Jobs nacked. Attribute `outcome` is `retry` or `dead_letter`.",
        )
        self._in_flight = meter.create_up_down_counter(
            "sepp.jobs.in_flight",
            description="Jobs currently being processed by handlers.",
        )
        self._reserves_completed = meter.create_counter(
            "sepp.reserves.completed",
            description="Reserve RPCs that returned. Attribute `jobs` is `some` or `empty`.",
        )
        self._reserves_failed = meter.create_counter(
            "sepp.reserves.failed", description="Reserve RPCs that failed."
        )

    def record_processed(self) -> None:
        if self._enabled:
            self._processed.add(1)

    def record_nacked(self, dead_lettered: bool) -> None:
        if self._enabled:
            outcome = "dead_letter" if dead_lettered else "retry"
            self._nacked.add(1, {"outcome": outcome})

    def record_in_flight_delta(self, delta: int) -> None:
        if self._enabled:
            self._in_flight.add(delta)

    def record_reserve_ok(self, empty: bool) -> None:
        if self._enabled:
            self._reserves_completed.add(1, {"jobs": "empty" if empty else "some"})

    def record_reserve_failed(self) -> None:
        if self._enabled:
            self._reserves_failed.add(1)


_SEVERITY_NUMBER: dict[int, int] = {
    logging.DEBUG: 5,
    logging.INFO: 9,
    logging.WARNING: 13,
    logging.ERROR: 17,
    logging.CRITICAL: 21,
}


def _emit_span_event(level: int, msg: str, args: tuple[object, ...]) -> None:
    """Record a log event on the active span, if any."""
    if not OTEL_AVAILABLE:
        return
    span = _trace.get_current_span()
    if not span.is_recording():
        return
    formatted = msg % args if args else msg
    span.add_event(
        "",
        attributes={
            "event.name": "log",
            "log.severity": _SEVERITY_NUMBER.get(level, 0),
            "log.message": formatted,
        },
    )


class _SeppLogger:
    """Wraps a standard :class:`logging.Logger` so every log call is also
    recorded as an OpenTelemetry span event on the active span."""

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def debug(self, msg: str, *args: object) -> None:
        self._logger.debug(msg, *args)
        _emit_span_event(logging.DEBUG, msg, args)

    def info(self, msg: str, *args: object) -> None:
        self._logger.info(msg, *args)
        _emit_span_event(logging.INFO, msg, args)

    def warning(self, msg: str, *args: object) -> None:
        self._logger.warning(msg, *args)
        _emit_span_event(logging.WARNING, msg, args)

    def error(self, msg: str, *args: object) -> None:
        self._logger.error(msg, *args)
        _emit_span_event(logging.ERROR, msg, args)


def create_sepp_logger(name: str) -> _SeppLogger | logging.Logger:
    """Returns a logger that emits span events alongside log calls when
    OpenTelemetry is installed, or a plain :class:`logging.Logger` otherwise."""
    logger = logging.getLogger(name)
    if OTEL_AVAILABLE:
        return _SeppLogger(logger)
    return logger
