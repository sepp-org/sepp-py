"""Tests for SeppClient RPC behavior, using a fake stub (no network)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import grpc
import pytest
from tests.conftest import VALID_UUID, FakeStub, FakeUnaryUnary, make_client, rpc_error

from sepp import errors
from sepp._pb import queue_pb2 as pb
from sepp.client import (
    RetryDirective,
    RetryPolicy,
    SeppClient,
    _auth_metadata_for,
    _normalize_target,
)
from sepp.types import EnqueueRequest, ReserveOptions

FAST_RETRY = RetryPolicy(
    max_attempts=5,
    initial_backoff=timedelta(0),
    max_backoff=timedelta(0),
    jitter=False,
)


def _batch_response(*outcomes: pb.JobResult) -> pb.EnqueueBatchResponse:
    return pb.EnqueueBatchResponse(results=list(outcomes))


def _success(job_id: str = "j1", dedup: bool = False) -> pb.JobResult:
    return pb.JobResult(success=pb.EnqueueResponse(job_id=job_id, deduplicated=dedup))


def _rejection() -> pb.JobResult:
    return pb.JobResult(
        rejection=pb.JobRejection(payload_too_large=pb.PayloadTooLarge(limit=1, actual=2))
    )


def _valid_job_pb() -> pb.Job:
    return pb.Job(
        id=VALID_UUID,
        job_type="t",
        priority=1,
        enqueued_at=1_700_000_000_000,
        attempt=1,
        max_attempts=3,
        lease_expires_at=1_700_000_060_000,
    )


# -- enqueue ----------------------------------------------------------------


async def test_enqueue_success() -> None:
    stub = FakeStub()
    stub.EnqueueBatch = FakeUnaryUnary(_batch_response(_success("abc", True)))
    client = make_client(stub)
    ack = await client.enqueue(EnqueueRequest("q", "t"))
    assert ack.job_id == "abc" and ack.deduplicated is True


async def test_enqueue_rejection_raises() -> None:
    stub = FakeStub()
    stub.EnqueueBatch = FakeUnaryUnary(_batch_response(_rejection()))
    client = make_client(stub)
    with pytest.raises(errors.JobRejectedError) as ei:
        await client.enqueue(EnqueueRequest("q", "t"))
    assert isinstance(ei.value.rejection, errors.PayloadTooLarge)


async def test_enqueue_batch_mixed_results() -> None:
    stub = FakeStub()
    stub.EnqueueBatch = FakeUnaryUnary(_batch_response(_success(), _rejection()))
    client = make_client(stub)
    results = await client.enqueue_batch([EnqueueRequest("q", "t"), EnqueueRequest("q", "t")])
    assert len(results) == 2
    assert results[0].job_id == "j1"  # type: ignore[union-attr]
    assert isinstance(results[1], errors.PayloadTooLarge)


async def test_enqueue_empty_batch_raises() -> None:
    client = make_client(FakeStub())
    with pytest.raises(errors.EmptyBatchError):
        await client.enqueue_batch([])


async def test_enqueue_batch_count_mismatch() -> None:
    stub = FakeStub()
    stub.EnqueueBatch = FakeUnaryUnary(_batch_response(_success()))  # 1 result for 2 jobs
    client = make_client(stub)
    with pytest.raises(errors.BatchResultCountMismatchError):
        await client.enqueue_batch([EnqueueRequest("q", "t"), EnqueueRequest("q", "t")])


async def test_enqueue_transport_error() -> None:
    stub = FakeStub()
    stub.EnqueueBatch = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = make_client(stub)
    with pytest.raises(errors.TransportError):
        await client.enqueue(EnqueueRequest("q", "t"))


# -- enqueue_atomic ---------------------------------------------------------


async def test_enqueue_atomic_success() -> None:
    stub = FakeStub()
    resp = pb.EnqueueAtomicResponse(
        success=pb.EnqueueAtomicSuccess(
            responses=[pb.EnqueueResponse(job_id="a"), pb.EnqueueResponse(job_id="b")]
        )
    )
    stub.EnqueueAtomic = FakeUnaryUnary(resp)
    client = make_client(stub)
    acks = await client.enqueue_atomic([EnqueueRequest("q", "t"), EnqueueRequest("q", "t")])
    assert [a.job_id for a in acks] == ["a", "b"]


async def test_enqueue_atomic_validation_failure() -> None:
    stub = FakeStub()
    resp = pb.EnqueueAtomicResponse(
        rejection=pb.BatchValidationFailure(
            errors=[
                pb.JobValidationError(
                    index=1, rejection=pb.JobRejection(unknown_queue=pb.UnknownQueue(queue="q"))
                )
            ]
        )
    )
    stub.EnqueueAtomic = FakeUnaryUnary(resp)
    client = make_client(stub)
    with pytest.raises(errors.BatchValidationError) as ei:
        await client.enqueue_atomic([EnqueueRequest("q", "t")])
    assert ei.value.errors[0].index == 1
    assert isinstance(ei.value.errors[0].rejection, errors.UnknownQueue)


# -- reserve ----------------------------------------------------------------


async def test_reserve_returns_jobs() -> None:
    stub = FakeStub()
    stub.Reserve = FakeUnaryUnary(pb.ReserveResponse(jobs=[_valid_job_pb()]))
    client = make_client(stub)
    jobs = await client.reserve(ReserveOptions(["q"], timedelta(seconds=1)))
    assert jobs is not None and len(jobs) == 1 and jobs[0].ctx.id == VALID_UUID


async def test_reserve_empty_returns_none() -> None:
    stub = FakeStub()
    stub.Reserve = FakeUnaryUnary(pb.ReserveResponse())
    client = make_client(stub)
    assert await client.reserve(ReserveOptions(["q"], timedelta(seconds=1))) is None


async def test_reserve_skips_malformed_job() -> None:
    stub = FakeStub()
    bad = _valid_job_pb()
    bad.id = ""  # malformed -> skipped
    stub.Reserve = FakeUnaryUnary(pb.ReserveResponse(jobs=[bad]))
    client = make_client(stub)
    assert await client.reserve(ReserveOptions(["q"], timedelta(seconds=1))) is None


async def test_reserve_unknown_queues() -> None:
    stub = FakeStub()
    stub.Reserve = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.FAILED_PRECONDITION, "queues: a"))
    client = make_client(stub)
    with pytest.raises(errors.UnknownQueuesError):
        await client.reserve(ReserveOptions(["q"], timedelta(seconds=1)))


async def test_reserve_not_retried() -> None:
    stub = FakeStub()
    stub.Reserve = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = make_client(stub, retry_policy=FAST_RETRY)
    with pytest.raises(errors.TransportError):
        await client.reserve(ReserveOptions(["q"], timedelta(seconds=1)))
    assert stub.Reserve.calls == 1  # type: ignore[attr-defined]


# -- ack / nack / extend ----------------------------------------------------


def _ctx(client: SeppClient):  # type: ignore[no-untyped-def]
    from sepp import _convert

    return _convert.job_from_pb(client, _valid_job_pb(), "worker-1").ctx


async def test_ack_success() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(pb.AckResponse(job_id=VALID_UUID))
    client = make_client(stub)
    await client.ack(_ctx(client))
    req = stub.Ack.last_request  # type: ignore[attr-defined]
    assert req.job_id == VALID_UUID and req.attempt == 1 and req.worker_id == "worker-1"


async def test_ack_job_not_found() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.NOT_FOUND))
    client = make_client(stub)
    with pytest.raises(errors.JobNotFoundError):
        await client.ack(_ctx(client))


async def test_ack_attempt_mismatch() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.FAILED_PRECONDITION))
    client = make_client(stub)
    with pytest.raises(errors.AttemptMismatchError):
        await client.ack(_ctx(client))


async def test_nack_default_directive() -> None:
    stub = FakeStub()
    stub.Nack = FakeUnaryUnary(pb.NackResponse(job_id=VALID_UUID, dead_lettered=False))
    client = make_client(stub)
    dead = await client.nack(_ctx(client), RetryDirective.DEFAULT, "boom")
    assert dead is False
    req = stub.Nack.last_request  # type: ignore[attr-defined]
    assert req.reason == "boom"
    assert req.retry.WhichOneof("strategy") == "default"


async def test_nack_directives_map_to_strategy() -> None:
    for directive, expected in [
        (RetryDirective.DEFAULT, "default"),
        (RetryDirective.dead_letter(), "dead_letter"),
        (RetryDirective.after(timedelta(seconds=2)), "delay_ms"),
    ]:
        stub = FakeStub()
        stub.Nack = FakeUnaryUnary(pb.NackResponse(job_id=VALID_UUID, dead_lettered=True))
        client = make_client(stub)
        dead = await client.nack(_ctx(client), directive, "r")
        assert dead is True
        req = stub.Nack.last_request  # type: ignore[attr-defined]
        assert req.retry.WhichOneof("strategy") == expected
        if expected == "delay_ms":
            assert req.retry.delay_ms == 2000


async def test_extend_returns_new_expiry() -> None:
    stub = FakeStub()
    stub.Extend = FakeUnaryUnary(
        pb.ExtendResponse(job_id=VALID_UUID, lease_expires_at=1_700_000_120_000)
    )
    client = make_client(stub)
    expiry = await client.extend(_ctx(client), timedelta(seconds=60))
    assert expiry == datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        milliseconds=1_700_000_120_000
    )
    req = stub.Extend.last_request  # type: ignore[attr-defined]
    assert req.lease_duration_ms == 60_000


async def test_extend_invalid_expiry_raises() -> None:
    stub = FakeStub()
    stub.Extend = FakeUnaryUnary(pb.ExtendResponse(job_id=VALID_UUID, lease_expires_at=-1))
    client = make_client(stub)
    with pytest.raises(errors.MalformedResponseError):
        await client.extend(_ctx(client), timedelta(seconds=60))


# -- get_server_info --------------------------------------------------------


async def test_get_server_info() -> None:
    stub = FakeStub()
    stub.GetServerInfo = FakeUnaryUnary(
        pb.GetServerInfoResponse(
            server_version="1.0.0", supported_protocol_versions=["v1"], server_time_ms=1
        )
    )
    client = make_client(stub)
    info = await client.get_server_info()
    assert info.version == "1.0.0"


async def test_get_server_info_malformed() -> None:
    stub = FakeStub()
    stub.GetServerInfo = FakeUnaryUnary(
        pb.GetServerInfoResponse(server_version="", server_time_ms=1)
    )
    client = make_client(stub)
    with pytest.raises(errors.MalformedServerInfoError):
        await client.get_server_info()


# -- retry semantics --------------------------------------------------------


async def test_retry_succeeds_after_transient() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(
        sequence=[
            rpc_error(grpc.StatusCode.UNAVAILABLE),
            rpc_error(grpc.StatusCode.UNAVAILABLE),
            pb.AckResponse(job_id=VALID_UUID),
        ]
    )
    client = make_client(stub, retry_policy=FAST_RETRY)
    await client.ack(_ctx(client))
    assert stub.Ack.calls == 3  # type: ignore[attr-defined]


async def test_retry_gives_up_after_max() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = make_client(
        stub, retry_policy=RetryPolicy(max_attempts=3, initial_backoff=timedelta(0), jitter=False)
    )
    with pytest.raises(errors.TransportError):
        await client.ack(_ctx(client))
    assert stub.Ack.calls == 3  # type: ignore[attr-defined]


async def test_retry_skips_non_transient() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.INVALID_ARGUMENT))
    client = make_client(stub, retry_policy=FAST_RETRY)
    with pytest.raises(errors.InvalidRequestError):
        await client.ack(_ctx(client))
    assert stub.Ack.calls == 1  # type: ignore[attr-defined]


async def test_default_policy_does_not_retry() -> None:
    stub = FakeStub()
    stub.Ack = FakeUnaryUnary(error=rpc_error(grpc.StatusCode.UNAVAILABLE))
    client = make_client(stub)  # default RetryPolicy: max_attempts=1
    with pytest.raises(errors.TransportError):
        await client.ack(_ctx(client))
    assert stub.Ack.calls == 1  # type: ignore[attr-defined]


# -- helpers ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("addr", "expected"),
    [
        ("http://127.0.0.1:50051", ("127.0.0.1:50051", False)),
        ("https://host:443", ("host:443", True)),
        ("127.0.0.1:50051", ("127.0.0.1:50051", False)),
    ],
)
def test_normalize_target(addr: str, expected: tuple[str, bool]) -> None:
    assert _normalize_target(addr) == expected


def test_auth_metadata_valid() -> None:
    assert _auth_metadata_for("secret") == [("authorization", "Bearer secret")]


def test_auth_metadata_none() -> None:
    assert _auth_metadata_for(None) == []


@pytest.mark.parametrize("bad", ["bad\nkey", "bad\rkey", "naïve"])
def test_auth_metadata_invalid(bad: str) -> None:
    with pytest.raises(errors.InvalidApiKeyError):
        _auth_metadata_for(bad)
