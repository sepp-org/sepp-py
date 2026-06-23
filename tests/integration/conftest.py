"""Session-scoped container lifecycle for integration tests.

Spins up a single ``ghcr.io/sepp-org/sepp:master`` container for the whole
session, waits for it to be ready, and tears it down at the end. Tests that
require the server use the ``sepp_server`` fixture (which returns the
``host:port`` address). If Docker is unavailable the fixture skips all tests.
"""

from __future__ import annotations

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

SEPP_GRPC_PORT = 50051
READY_MESSAGE = "queue server listening"
IMAGE = "ghcr.io/sepp-org/sepp:master"


def _container() -> DockerContainer:
    return (
        DockerContainer(IMAGE)
        .with_exposed_ports(SEPP_GRPC_PORT)
        .with_env("SEPP_ALLOW_QUEUE_CREATION", "true")
    )


@pytest.fixture(scope="session")
def sepp_server() -> str:
    try:
        container = _container()
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker not available or container failed to start: {exc}")

    try:
        wait_for_logs(container, READY_MESSAGE, timeout=60)
    except Exception as exc:
        container.stop()
        pytest.skip(f"Sepp server did not become ready: {exc}")

    host = container.get_container_host_ip()
    port = container.get_exposed_port(SEPP_GRPC_PORT)
    yield f"{host}:{port}"
    container.stop()
