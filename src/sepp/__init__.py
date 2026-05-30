"""An async Python client for the `sepp <https://github.com/sepp-org/sepp>`_
durable job queue.

This client provides both halves of the job queue API:

* **Producers** enqueue jobs with a :class:`~sepp.client.SeppClient` — one at a
  time, in best-effort batches, or atomic batches.
* **Consumers** reserve jobs and report their outcome. The low-level
  :meth:`~sepp.client.SeppClient.reserve` / :meth:`~sepp.client.SeppClient.ack`
  / :meth:`~sepp.client.SeppClient.nack` calls give full manual control, while
  the high-level :class:`~sepp.worker.Worker` runs the whole
  reserve -> process -> ack loop with bounded concurrency, lease
  auto-extension, graceful shutdown, and metrics.

Quickstart::

    import asyncio
    from datetime import timedelta
    from sepp import SeppClient, Worker, EnqueueRequest, Payload

    async def main():
        client = await SeppClient.connect("127.0.0.1:50051")

        await client.enqueue(
            EnqueueRequest(
                "emails", "send_welcome",
                payload=Payload(b'{"user": 42}', "application/json"),
            )
        )

        worker = Worker(client, ["emails"], timedelta(seconds=30))

        @worker.handler("send_welcome")
        async def send_welcome(payload, ctx):
            print("processing", ctx.id)

        await worker.run()

    asyncio.run(main())
"""

from __future__ import annotations

from sepp.client import RetryDirective, RetryPolicy, SeppClient
from sepp.errors import (
    AttemptMismatchError,
    BatchResultCountMismatchError,
    BatchValidationError,
    ClientError,
    ConnectError,
    CustomEntriesTooMany,
    CustomKeyTooLong,
    CustomMapTooLarge,
    EmptyBatchError,
    EncodingNotAllowed,
    IdempotencyKeyTooLong,
    InvalidApiKeyError,
    InvalidRequest,
    InvalidRequestError,
    JobConversionError,
    JobNotFoundError,
    JobRejectedError,
    JobRejection,
    JobTypeNameTooLong,
    JobTypeNotAllowed,
    JobValidationError,
    LeaseError,
    MalformedJobError,
    MalformedResponseError,
    MalformedServerInfoError,
    OverloadedError,
    PayloadTooLarge,
    QueueNameTooLong,
    ReserveError,
    ScheduledTooFar,
    SeppError,
    ServerInfoError,
    ServerInternalError,
    TransportError,
    UnauthenticatedError,
    UnexpectedStatusError,
    UnknownQueue,
    UnknownQueuesError,
    UnknownRejection,
)
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
    TraceContextError,
)
from sepp.worker import DuplicateHandlerError, HandlerError, ShutdownHandle, Worker

try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("sepp")
    except PackageNotFoundError:  # pragma: no cover
        __version__ = "0.1.0"
except ImportError:  # pragma: no cover
    __version__ = "0.1.0"

__all__ = [
    "__version__",
    # client
    "SeppClient",
    "RetryPolicy",
    "RetryDirective",
    # worker
    "Worker",
    "HandlerError",
    "ShutdownHandle",
    "DuplicateHandlerError",
    # types
    "Payload",
    "Primitive",
    "Priority",
    "PriorityOutOfRangeError",
    "TraceContext",
    "TraceContextError",
    "EnqueueRequest",
    "EnqueueAck",
    "Job",
    "JobCtx",
    "ReserveOptions",
    "ServerInfo",
    "DeadLetterCause",
    "DeadLetterRecord",
    # errors — base + transport/protocol
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
    # errors — enqueue / lease / reserve
    "JobRejectedError",
    "BatchValidationError",
    "LeaseError",
    "JobNotFoundError",
    "AttemptMismatchError",
    "ReserveError",
    "UnknownQueuesError",
    # rejections (values)
    "JobRejection",
    "JobValidationError",
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
    "UnknownRejection",
]
