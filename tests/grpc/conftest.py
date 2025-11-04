import pytest

from tests.grpc_testing_tools.grpc_testing_ports import (
    BASE_CHILD_PORT,
    BASE_MANAGER_PORT,
    BASE_ROOT_PORT,
    MAX_CHILDREN,
)
from tests.grpc_testing_tools.port_cleaner import kill_process_on_port


@pytest.fixture(scope="function")
def grpc_port_cleaner():
    """Fixture to clean gRPC ports before and after tests."""
    ports_to_clean = [BASE_MANAGER_PORT, BASE_ROOT_PORT]
    ports_to_clean += [BASE_CHILD_PORT + i for i in range(MAX_CHILDREN)]

    # Clean ports before test
    for port in ports_to_clean:
        kill_process_on_port(port)

    yield

    # Clean ports after test
    for port in ports_to_clean:
        kill_process_on_port(port)
