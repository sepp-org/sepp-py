"""Shared test helpers: a way to build gRPC errors and a channel-free client."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import grpc
import grpc.aio

from sepp.client import RetryPolicy, SeppClient

VALID_TP = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
VALID_UUID = "550e8400-e29b-41d4-a716-446655440000"


def rpc_error(code: grpc.StatusCode, details: str = "x") -> grpc.aio.AioRpcError:
    md = grpc.aio.Metadata()
    return grpc.aio.AioRpcError(code, md, md, details=details)


class FakeUnaryUnary:
    """Stands in for a stub method: each call returns a fresh awaitable that
    either returns the next queued response or raises the next queued error.

    A single response/error is reused for every call (useful for retry tests
    that need the same failure repeatedly); pass a list to script a sequence.
    """

    def __init__(
        self,
        response: object = None,
        error: BaseException | None = None,
        sequence: list[object] | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.sequence = sequence
        self.calls = 0
        self.last_request: object = None
        self.last_metadata: object = None

    def __call__(
        self, request: object, metadata: object = None, timeout: object = None
    ) -> Awaitable[object]:
        self.calls += 1
        self.last_request = request
        self.last_metadata = metadata

        async def _run() -> object:
            if self.sequence is not None:
                item = self.sequence[min(self.calls - 1, len(self.sequence) - 1)]
                if isinstance(item, BaseException):
                    raise item
                return item
            if self.error is not None:
                raise self.error
            return self.response

        return _run()


class FakeStub:
    """A QueueServiceStub stand-in; set attributes to FakeUnaryUnary."""

    def __init__(self) -> None:
        self.EnqueueBatch: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.EnqueueAtomic: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.Reserve: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.Ack: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.Nack: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.Extend: Callable[..., Awaitable[object]] = FakeUnaryUnary()
        self.GetServerInfo: Callable[..., Awaitable[object]] = FakeUnaryUnary()


def make_client(stub: FakeStub, retry_policy: RetryPolicy | None = None) -> SeppClient:
    """Build a SeppClient with a fake stub and no real channel."""
    client: SeppClient = object.__new__(SeppClient)
    client._channel = None  # type: ignore[assignment]
    client._stub = stub  # type: ignore[assignment]
    client._retry_policy = retry_policy or RetryPolicy()
    client._auth_metadata = []
    return client
