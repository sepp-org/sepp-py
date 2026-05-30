"""Tests for the gRPC-status-code -> exception mapping and RetryPolicy clamping."""

from __future__ import annotations

from datetime import timedelta

import grpc
import pytest
from tests.conftest import rpc_error

from sepp import errors
from sepp.client import RetryDirective, RetryPolicy


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (grpc.StatusCode.UNAVAILABLE, errors.TransportError),
        (grpc.StatusCode.DEADLINE_EXCEEDED, errors.TransportError),
        (grpc.StatusCode.ABORTED, errors.TransportError),
        (grpc.StatusCode.CANCELLED, errors.TransportError),
        (grpc.StatusCode.UNAUTHENTICATED, errors.UnauthenticatedError),
        (grpc.StatusCode.PERMISSION_DENIED, errors.UnauthenticatedError),
        (grpc.StatusCode.RESOURCE_EXHAUSTED, errors.OverloadedError),
        (grpc.StatusCode.INVALID_ARGUMENT, errors.InvalidRequestError),
        (grpc.StatusCode.INTERNAL, errors.ServerInternalError),
        (grpc.StatusCode.DATA_LOSS, errors.ServerInternalError),
        (grpc.StatusCode.UNKNOWN, errors.ServerInternalError),
        (grpc.StatusCode.NOT_FOUND, errors.UnexpectedStatusError),
    ],
)
def test_client_error_mapping(code: grpc.StatusCode, expected: type) -> None:
    assert isinstance(errors.client_error_from_rpc(rpc_error(code)), expected)


def test_client_error_preserves_message() -> None:
    err = errors.client_error_from_rpc(rpc_error(grpc.StatusCode.INTERNAL, "boom"))
    assert "boom" in str(err)


def test_unexpected_status_carries_code() -> None:
    err = errors.client_error_from_rpc(rpc_error(grpc.StatusCode.NOT_FOUND))
    assert isinstance(err, errors.UnexpectedStatusError)
    assert err.code == grpc.StatusCode.NOT_FOUND


def test_lease_error_mapping() -> None:
    assert isinstance(
        errors.lease_error_from_rpc(rpc_error(grpc.StatusCode.NOT_FOUND)), errors.JobNotFoundError
    )
    assert isinstance(
        errors.lease_error_from_rpc(rpc_error(grpc.StatusCode.FAILED_PRECONDITION)),
        errors.AttemptMismatchError,
    )
    assert isinstance(
        errors.lease_error_from_rpc(rpc_error(grpc.StatusCode.UNAVAILABLE)), errors.TransportError
    )


def test_reserve_error_mapping() -> None:
    err = errors.reserve_error_from_rpc(
        rpc_error(grpc.StatusCode.FAILED_PRECONDITION, "queues: a, b")
    )
    assert isinstance(err, errors.UnknownQueuesError)
    assert "a, b" in str(err)
    assert isinstance(
        errors.reserve_error_from_rpc(rpc_error(grpc.StatusCode.UNAVAILABLE)), errors.TransportError
    )


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    ],
)
def test_retryable_codes(code: grpc.StatusCode) -> None:
    assert code in errors.RETRYABLE_CODES


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DATA_LOSS,
        grpc.StatusCode.UNKNOWN,
    ],
)
def test_non_retryable_codes(code: grpc.StatusCode) -> None:
    assert code not in errors.RETRYABLE_CODES


# -- RetryPolicy ------------------------------------------------------------


def test_retry_policy_default_no_retry() -> None:
    assert RetryPolicy().max_attempts == 1


def test_retry_policy_clamps_max_attempts() -> None:
    assert RetryPolicy(max_attempts=0).max_attempts == 1


def test_retry_policy_clamps_multiplier() -> None:
    assert RetryPolicy(multiplier=0.5).multiplier == 1.0


# -- RetryDirective ---------------------------------------------------------


def test_retry_directive_constants() -> None:
    assert RetryDirective.DEFAULT.kind == "default"
    assert RetryDirective.DEAD_LETTER.kind == "dead_letter"


def test_retry_directive_after() -> None:
    d = RetryDirective.after(timedelta(seconds=5))
    assert d.kind == "after" and d.delay == timedelta(seconds=5)
