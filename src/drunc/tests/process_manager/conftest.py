from unittest.mock import MagicMock, patch

import pytest
from druncschema.process_manager_pb2 import (
    BootRequest,
    ProcessDescription,
    ProcessMetadata,
    ProcessRestriction,
)
from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import (
    ProcessManagerDriver,
)


@pytest.fixture
def app_data():
    """
    Provides a mock application dictionary with required keys.
    """
    return {
        "restriction": "localhost",
        "name": "TestApp",
        "type": "binary",
        "args": ["--arg1"],
        "env": {"CUSTOM_ENV": "value"},
        "log_path": "/app/logs",
        "tree_id": "tree123",
    }


@pytest.fixture
def dummy_bootrequest(app_data):
    return BootRequest(
        token=Token(),
        process_description=ProcessDescription(
            metadata=ProcessMetadata(
                user="test_user",
                session="session1",
                name=app_data["name"],
                hostname="",
                tree_id=app_data["tree_id"],
            ),
            executable_and_arguments=[{"exec": "binary", "args": ["--arg1"]}],
            env={
                **app_data["env"],
                "DUNE_DAQ_BASE_RELEASE": "release1",
                "SPACK_RELEASES_DIR": "spack_release",
            },
            process_execution_directory="/pwd",
            process_logs_path=app_data["log_path"],
        ),
        process_restriction=ProcessRestriction(
            allowed_hosts=[app_data["restriction"]],
        ),
    )


@pytest.fixture(scope="module")
def mock_logger():
    with patch("drunc.utils.shell_utils.get_logger") as mock_get_logger:
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance


@pytest.fixture(scope="function")
def mock_driver(mock_logger):
    """
    Create a ProcessManagerDriver instance with a mocked gRPC stub.
    This fixture creates a driver instance where the underlying gRPC channel
    and stub are mocked.
    Returns:
        ProcessManagerDriver: Driver instance with mocked dependencies
    """
    with (
        patch("drunc.process_manager.process_manager_driver.grpc.insecure_channel"),
        patch(
            "drunc.process_manager.process_manager_driver.ProcessManagerStub"
        ) as mock_stub_class,
    ):
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        # Initialise driver with mocked dependencies
        driver = ProcessManagerDriver(address="localhost:50051", token=Token())

        driver.log = mock_logger

        # Attach mock stub for easy access in tests
        driver._mock_stub = mock_stub

        return driver


@pytest.fixture
def boot_test_setup(mock_driver):
    """
    Fixture to prepare common mocks for testing the `boot` method of a process manager driver.
    """

    def _setup(*, is_ready=True, grpc_error=None):
        # Create a mock boot request with metadata and host restriction
        mock_request = MagicMock()
        mock_request.process_description.metadata.name = "test_app"
        mock_request.process_restriction.allowed_hosts = {"host1"}

        # Create a mock session DAL with no infrastructure applications
        fake_dal = MagicMock(infrastructure_applications=[])

        # Mock connectivity service
        csc_mock = MagicMock(is_ready=MagicMock(return_value=is_ready))
        mock_driver._connect_to_service = MagicMock(
            return_value=(csc_mock, "server", 1234)
        )

        # Internal methods of the driver
        mock_driver._consolidate_config = MagicMock()
        mock_driver._initialise_session = MagicMock(return_value=("db", fake_dal))

        mock_driver._convert_oks_to_boot_request = MagicMock(
            return_value=[mock_request]
        )
        mock_driver._discover_controller = MagicMock()

        # Configure the boot stub to either return a response or raise an error
        if grpc_error:
            mock_driver.stub.boot = MagicMock(side_effect=grpc_error)
        else:
            mock_driver.stub.boot = MagicMock(return_value="boot_response")

        return mock_request, csc_mock

    return _setup
