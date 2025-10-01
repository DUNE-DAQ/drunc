from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc_testing
import pytest
from druncschema.description_pb2 import CommandDescription
from druncschema.request_response_pb2 import Request
from druncschema.session_manager_pb2 import (
    DESCRIPTOR,
)
from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager import SessionManager
from drunc.session_manager.session_manager_driver import SessionManagerDriver

# -----------------------------------------------------
#    Session Manager serialisation tests fixtures
# -----------------------------------------------------


@pytest.fixture
def mock_config_environment(monkeypatch):
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")
    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        mock_configuration = MagicMock()
        mock_configuration.get_dals.return_value = [
            MagicMock(id="session_1"),
            MagicMock(id="session_2"),
        ]

        yield mock_configuration


# -----------------------------------------------------
#    Session Manager server tests fixtures
# -----------------------------------------------------


@pytest.fixture(scope="function")
def mock_request():
    return Request(token=Token(user_name="abc", token="13"))


@pytest.fixture(scope="function")
def mock_context():
    return MagicMock()


@pytest.fixture(scope="function")
def mock_logger():
    with patch("drunc.session_manager.session_manager.get_logger") as mock_get_logger:
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance


@pytest.fixture(scope="function")
def session_manager(mock_logger):
    dummy_conf_handler = MagicMock()
    return SessionManager(name="dummy_name", configuration=dummy_conf_handler)


@pytest.fixture
def commands():
    return [
        CommandDescription(
            name="describe",
            data_type=["None"],
            help="List the methods exposed by this endpoint.",
            return_type="description_pb2.Description",
        ),
        CommandDescription(
            name="list_all_sessions",
            data_type=["None"],
            help="List all active sessions.",
            return_type="session_manager_pb2.AllActiveSessions",
        ),
        CommandDescription(
            name="list_all_configs",
            data_type=["None"],
            help="List all available configurations.",
            return_type="session_manager_pb2.AllConfigKeys",
        ),
    ]


# -----------------------------------------------------
#    Session Manager endpoints tests fixtures
# -----------------------------------------------------


@pytest.fixture(scope="function")
def grpc_servicer(mock_logger):
    """
    Create and configure a SessionManager service interface for testing.
    """
    servicer = SessionManager(name="dummy_session", configuration=MagicMock())
    return servicer


@pytest.fixture(scope="function")
def grpc_test_server_factory(grpc_servicer):
    """
    Create a function for generating gRPC test servers with specific endpoint mocks.
    Args:
        grpc_servicer: The SessionManager servicer instance to register
    Returns:
        function: Factory function that accepts (endpoint_name, expected_response) parameters
    """

    def create_server(endpoint_name, expected_response):
        """
        Create a gRPC test server with a specific endpoint mocked.
        Args:
            endpoint_name (str): Name of the endpoint method to mock (e.g., 'describe')
            expected_response: The response object to return from the mocked method
        """
        # Mock the method for the specified endpoint
        mock_method = MagicMock(return_value=expected_response)
        setattr(grpc_servicer, endpoint_name, mock_method)

        # Register the servicer with the gRPC testing framework
        servicers = {DESCRIPTOR.services_by_name["SessionManager"]: grpc_servicer}
        test_server = grpc_testing.server_from_dictionary(
            servicers, grpc_testing.strict_real_time()
        )
        return (test_server, expected_response)

    return create_server


# -----------------------------------------------------
#    Session Manager driver tests fixtures
# -----------------------------------------------------


@pytest.fixture(scope="function")
def mock_driver():
    """
    This fixture creates a driver instance where the underlying gRPC channel
    and stub are mocked.

    Returns:
        SessionManagerDriver: Driver instance with mocked dependencies
    """
    with (
        patch("drunc.session_manager.session_manager_driver.grpc.insecure_channel"),
        patch(
            "drunc.session_manager.session_manager_driver.SessionManagerStub"
        ) as mock_stub_class,
    ):
        # Create mock stub instance that will be returned by SessionManagerStub()
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        # Initialize driver with mocked dependencies
        driver = SessionManagerDriver(address="localhost:50051", token=Token())

        # Attach mock stub for easy access in tests
        driver._mock_stub = mock_stub

        return driver
