"""End-to-end roundtrip *with OpenTelemetry tracing*.

Run this to see the producer's enqueue span and the worker's process span show
up as linked spans in a tracing backend.

Prerequisites — two things must be running:

  1. A sepp server. Point at it with ``SEPP_ADDR`` (default ``127.0.0.1:50051``).

  2. An OTLP collector. Point at it with ``OTEL_ENDPOINT`` (default
     ``http://localhost:4317``). The quickest one to stand up is Jaeger v2::

         docker run --rm --name jaeger \\
           -p 16686:16686 -p 4317:4317 -p 4318:4318 \\
           jaegertracing/jaeger:latest

Run it (needs the ``otel`` extra plus the OTLP exporter)::

    uv run --extra otel python examples/traced.py

Then open your tracing backend, pick the ``sepp-py-example`` service, and open
the most recent ``sepp.process`` trace. That span carries a *link* back to the
``sepp.enqueue`` span — the producer -> worker linkage.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from sepp import EnqueueRequest, JobCtx, Payload, SeppClient, Worker

QUEUE = "traced-example"
JOB_TYPE = "greeting"


def init_telemetry() -> TracerProvider:
    """Build an OTLP exporter and install it as the global tracer provider. The
    returned provider must be shut down before the process exits or buffered
    spans are lost."""
    endpoint = os.environ.get("OTEL_ENDPOINT", "http://localhost:4317")
    provider = TracerProvider(resource=Resource.create({"service.name": "sepp-py-example"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    return provider


async def roundtrip() -> None:
    addr = os.environ.get("SEPP_ADDR", "127.0.0.1:50051")
    client = await SeppClient.connect(addr)
    tracer = trace.get_tracer("sepp-py-example")

    # 1. Enqueue a job inside a span. The client's `sepp.enqueue` span stamps its
    #    trace context onto the job; here we wrap it in an app span too.
    with tracer.start_as_current_span("produce"):
        ack = await client.enqueue(
            EnqueueRequest(QUEUE, JOB_TYPE, payload=Payload(b"hello, sepp", "text/plain"))
        )
        print(f"enqueued job {ack.job_id}")

    # 2. Run a worker. The client's `sepp.process` span links back to the
    #    enqueue span recovered from the job's trace context.
    done: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    worker = Worker(client, [QUEUE], timedelta(seconds=30))

    @worker.handler(JOB_TYPE)
    async def greeting(payload: Payload | None, ctx: JobCtx) -> None:
        body = payload.data.decode() if payload else ""
        with tracer.start_as_current_span("handle"):
            print(f"handling job {ctx.id} — payload {body!r}")
        if not done.done():
            done.set_result(ctx.id)

    worker_task = asyncio.create_task(worker.run())
    try:
        job_id = await asyncio.wait_for(asyncio.shield(done), timeout=15)
        await asyncio.sleep(0.5)  # let the worker ack
        print(f"roundtrip OK — job {job_id} processed; check the trace in your backend")
    except asyncio.TimeoutError:
        print("timed out waiting for the job to be processed")
    finally:
        worker.shutdown_handle().shutdown()
        await worker_task
        await client.close()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    provider = init_telemetry()
    try:
        await roundtrip()
    finally:
        provider.shutdown()  # flush spans to the collector before exiting


if __name__ == "__main__":
    asyncio.run(main())
