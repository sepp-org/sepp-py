"""Session-scoped container lifecycle for integration tests.

Spins up a single ``ghcr.io/sepp-org/sepp:master`` container for the whole
session, waits for it to be ready, and tears it down at the end. Tests that
require the server use the ``sepp_server`` fixture (which returns the
``host:port`` address). When no server can be provided the fixture fails the
run loudly. Set ``SEPP_TEST_ADDR`` (``host:port``) to use an
already-running server instead of a container; the tests then share that
server and its state.
"""

from __future__ import annotations

import contextlib
import os
from typing import NoReturn

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

SEPP_GRPC_PORT = 50051
READY_MESSAGE = "queue server listening"
IMAGE = "ghcr.io/sepp-org/sepp:master"


def _container() -> DockerContainer:
    return (
        DockerContainer(IMAGE)
        .with_exposed_ports(SEPP_GRPC_PORT)
        .waiting_for(LogMessageWaitStrategy(READY_MESSAGE).with_startup_timeout(60))
    )


def _fail(reason: str) -> NoReturn:
    pytest.fail(
        f"cannot provide a sepp server for the integration tests: {reason} "
        "(set SEPP_TEST_ADDR to use an already-running server)"
    )


@pytest.fixture(scope="session")
def sepp_server() -> str:
    addr = os.environ.get("SEPP_TEST_ADDR")
    if addr:
        yield addr
        return

    container = _container()
    try:
        container.start()  # waits for READY_MESSAGE via the wait strategy
    except Exception as exc:
        with contextlib.suppress(Exception):
            container.stop()
        _fail(f"Docker not available or Sepp server did not become ready: {exc}")

    host = container.get_container_host_ip()
    port = container.get_exposed_port(SEPP_GRPC_PORT)
    yield f"{host}:{port}"
    container.stop()
