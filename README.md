<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sepp-org/sepp-py/HEAD/media/sepp-python-paper-200.png">
    <img alt="sepp-py" src="https://raw.githubusercontent.com/sepp-org/sepp-py/HEAD/media/sepp-python-ink-200.png" width="128" height="128">
  </picture>

  <h1>sepp-py</h1>

  <p>
    <strong>The official Python client for <a href="https://github.com/sepp-org/sepp">sepp</a>,</strong>
    <br/>
    a small, language-agnostic durable job queue.
  </p>

  <p>
    <a href="https://github.com/sepp-org/sepp-py/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sepp-org/sepp-py/ci.yml?branch=master&labelColor=181512"></a>
    <a href="https://pypi.org/project/sepp-py/"><img alt="PyPI" src="https://img.shields.io/pypi/v/sepp-py?labelColor=181512"></a>
    <a href="https://pypi.org/project/sepp-py/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/sepp-py?labelColor=181512"></a>
    <a href="LICENSE"><img alt="license" src="https://img.shields.io/pypi/l/sepp-py?color=ec6a2e&labelColor=181512"></a>
  </p>

  <p>
    <a href="https://pypi.org/project/sepp-py/">PyPI</a>
    ·
    <a href="https://sepp-org.github.io/sepp/docs/">sepp docs</a>
    ·
    <a href="https://buf.build/sepp-org/sepp-proto/docs/main%3Asepp.v1">Protocol</a>
    ·
    <a href="https://github.com/sepp-org/sepp-py/issues">Issues</a>
  </p>
</div>

## Functionality

- Enqueue jobs one at a time, in best-effort batches, or atomically, with idempotency keys, priorities, scheduled delivery and custom metadata.
- A high-level `Worker` runs the whole reserve → process → ack loop for you with bounded concurrency, automatic lease extension and graceful shutdown, or drop down to the raw `reserve` / `ack` / `nack` / `extend` calls for full control.
- With the `otel` extra, the client emits spans and worker metrics and propagates W3C trace context from the producer's enqueue span to the worker's process span. The host application owns the exporter.

The client is built on
[`grpc.aio`](https://grpc.github.io/grpc/python/grpc_asyncio.html) and is
**async only**; run it under `asyncio`.

## Install

```sh
pip install sepp-py            # or: uv add sepp-py
pip install "sepp-py[otel]"    # with OpenTelemetry spans, metrics & trace propagation
```

Requires Python 3.10+.

## Quickstart

Enqueue a job, then run a worker that processes it (requires a running [sepp server](https://github.com/sepp-org/sepp)):

```python
import asyncio
from datetime import timedelta

from sepp import SeppClient, Worker, EnqueueRequest, Payload


async def main() -> None:
    client = await SeppClient.connect("127.0.0.1:50051")

    # Producer: enqueue a job onto the `emails` queue.
    ack = await client.enqueue(
        EnqueueRequest(
            "emails",
            "send_welcome",
            payload=Payload(b'{"user": 42}', "application/json"),
        )
    )
    print("enqueued job", ack.job_id)

    # Consumer: process `send_welcome` jobs. A handler that returns normally
    # acks the job; one that raises HandlerError nacks it.
    worker = Worker(client, ["emails"], timedelta(seconds=30))

    @worker.handler("send_welcome")
    async def send_welcome(payload, ctx):
        print("processing job", ctx.id)

    await worker.run()


asyncio.run(main())
```

Runnable versions live in [`examples/`](examples/), including
[`traced.py`](examples/traced.py) which wires up an OTLP exporter for
end-to-end distributed tracing.

## Concepts

### Queues and job types

Every job is enqueued onto a named queue and tagged
with a `job_type`. Workers reserve from one or more queues and dispatch each job
to the handler registered for its `job_type`.

### Payload

Every job can carry an opaque blob of bytes plus an encoding hint
(`Payload(data, encoding)`). The queue never interprets the bytes; producers and
workers agree on the encoding. For primitive key-value metadata, pass the
`custom` mapping on `EnqueueRequest` instead (values may be `str`, `int`,
`float`, or `bool`).

### Leases and redelivery

A reserved job is leased to the worker for a bounded
duration. The worker must `ack`, `nack`, or `extend` the lease before it
expires; otherwise the server redelivers the job (with `ctx.attempt`
incremented) until `max_attempts` is reached and it is dead-lettered. The
`Worker` can extend leases automatically — see `auto_extend=True`.

## Producing jobs

```python
from datetime import datetime, timedelta, timezone
from sepp import EnqueueRequest, Payload, Priority

req = EnqueueRequest(
    "emails",
    "send_welcome",
    payload=Payload(b"{}", "application/json"),
    priority=Priority.P7,                 # or a bare int 0..=9
    idempotency_key="welcome-user-42",    # server drops duplicates in its dedup window
    max_attempts=5,
    custom={"user_id": 42, "vip": True},
    scheduled_at=datetime.now(timezone.utc) + timedelta(minutes=5),
)

ack = await client.enqueue(req)           # raises JobRejectedError if the server rejects it

# Best-effort batch: one result per job, in order — an EnqueueAck or a JobRejection.
results = await client.enqueue_batch([req, req])

# Atomic batch: all-or-nothing — raises BatchValidationError if any job fails.
acks = await client.enqueue_atomic([req, req])
```

Fields left unset fall back to the queue's server-side defaults. Most limits are
advertised by `await client.get_server_info()`, so a producer can validate
locally before sending.

## Consuming jobs

### With a `Worker`

```python
from datetime import timedelta
from sepp import Worker, HandlerError

worker = Worker(
    client,
    ["emails"],
    lease_duration=timedelta(seconds=30),
    max_in_flight=32,
    auto_extend=True,            # keep long-running handlers' leases alive
)

@worker.handler("send_welcome")
async def send_welcome(payload, ctx):
    body = payload.data if payload else b""
    ...                          # return to ack

@worker.handler("send_receipt")
async def send_receipt(payload, ctx):
    raise HandlerError.retry("payment service unavailable")   # nack for retry

await worker.run()               # returns after graceful shutdown drains in-flight jobs
```

A handler's outcome decides the job's fate:

- returning normally → **ack**
- raising `HandlerError.retry(reason)` → nack, retry under the queue's policy
- raising `HandlerError.retry_after(reason, delay)` → nack, retry after a delay
- raising `HandlerError.permanent(reason)` → nack straight to the dead-letter queue
- raising any other exception → nack for retry (caught so it can't crash the worker)

A job whose `job_type` has no registered handler is nacked for retry. Register a
catch-all handler to process those instead:

```python
@worker.catch_all
async def fallback(payload, ctx):
    logging.warning("no handler for %s", ctx.job_type)
    ...                          # same outcomes as a normal handler
```

Trigger a graceful shutdown from a signal handler:

```python
import signal

handle = worker.shutdown_handle()
asyncio.get_running_loop().add_signal_handler(signal.SIGINT, handle.shutdown)
await worker.run()
```

### Manually

```python
from datetime import timedelta
from sepp import ReserveOptions, RetryDirective

opts = ReserveOptions(["emails"], lease_duration=timedelta(seconds=30), max_jobs=10)
jobs = await client.reserve(opts)        # None if the long poll elapsed empty
for job in jobs or []:
    try:
        ...                               # process job.payload / job.ctx
        await client.ack(job.ctx)
    except Exception:
        await client.nack(job.ctx, RetryDirective.after(timedelta(seconds=30)), "boom")
```

## Authentication & TLS

```python
client = await SeppClient.connect(
    "https://queue.example.com:50051",   # https:// implies TLS
    api_key="secret",                     # sent as Authorization: Bearer <key>
    tls_ca_cert=open("ca.pem", "rb").read(),
)
```

`connect` also accepts `tls=True` (system roots), `tls_domain=...` (override the
verified name), a full `credentials=...` object, a `retry_policy=...`, and the
timeout and message-size options below.

## Retries

Transient RPC failures (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED`,
`RESOURCE_EXHAUSTED`) can be retried with exponential backoff. The default
policy does **not** retry; opt in:

```python
from datetime import timedelta
from sepp import RetryPolicy, SeppClient

client = await SeppClient.connect(
    "127.0.0.1:50051",
    retry_policy=RetryPolicy(
        max_attempts=5,
        initial_backoff=timedelta(milliseconds=50),
        max_backoff=timedelta(seconds=5),
    ),
)
```

`reserve` is a long poll and is never retried by this policy.

A retried enqueue can duplicate jobs that carry no idempotency key, so when any
job in an enqueue lacks one, the ambiguous `DEADLINE_EXCEEDED` and `ABORTED`
failures (the request may already have committed server-side) are not retried
for that call.

## Timeouts and message size

Every unary RPC except `reserve` carries a deadline, 30 seconds by default;
tune it with `rpc_timeout=` on `connect` (very large enqueue batches may need
more). `reserve` instead derives its deadline from its options' `wait_timeout`.

Workers reserving many or large payloads should also raise
`max_receive_message_bytes=`, which lifts gRPC's 4 MiB cap on a single received
message: a reserve response can be up to the server's `max_reserve_batch` times
`max_payload_bytes`, and a response over the cap fails on the client after the
server has already leased the jobs, stranding them until their leases expire.

```python
client = await SeppClient.connect(
    "127.0.0.1:50051",
    rpc_timeout=timedelta(seconds=60),
    max_receive_message_bytes=32 * 1024 * 1024,
)
```

## OpenTelemetry

Install the `otel` extra to emit client/consumer spans, worker metrics, and
propagate W3C trace context from the producer's enqueue span to the worker's
process span. The host application owns the exporter/provider; this client only
emits into it. See [`examples/traced.py`](examples/traced.py).

## Development

```sh
uv sync --extra otel              # install locked deps, including the dev group
uv run pytest                     # run the test suite (integration tests need Docker)
uv run ruff check . && uv run mypy
buf generate                      # regenerate stubs under src/sepp/v1/ from the BSR
```

The generated stubs (`queue_pb2.py`, `queue_pb2_grpc.py`) are committed under
`src/sepp/v1/` and regenerated with `buf generate` from
[`buf.build/sepp-org/sepp-proto`](https://buf.build/sepp-org/sepp-proto)
(pinned to `v1.2.0` in `buf.gen.yaml`), so installing the package needs no
`protoc`.

## Docs

This README is the client's usage documentation. For running and configuring
the sepp server itself, see the [sepp docs site](https://sepp-org.github.io/sepp/docs/).

## License

sepp-py is licensed under the MIT License. See [LICENSE](LICENSE) for details.
