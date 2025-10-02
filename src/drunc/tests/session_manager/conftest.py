from pathlib import Path
from unittest.mock import MagicMock, patch

import google.protobuf.any_pb2
import grpc_testing
import pytest
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.session_manager_pb2 import (
    DESCRIPTOR,
    ActiveSession,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)
from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager import SessionManager
from drunc.session_manager.session_manager_driver import SessionManagerDriver

# -----------------------------------------------------
#    Session Manager Serialisation Tests Fixtures
# -----------------------------------------------------


@pytest.fixture
def mock_config_environment(monkeypatch):
    """
    Fixture that sets up a mocked configuration environment for testing.
    """
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
#    Session Manager Server Tests Fixtures
# -----------------------------------------------------


@pytest.fixture(scope="function")
def mock_request():
    return Request(token=Token(user_name="abc", token="13"))


@pytest.fixture(scope="function")
def mock_context():
    return MagicMock()


@pytest.fixture(scope="function")
def mock_logger():
    """
    Mocks the logger used by the session manager.
    """
    with patch("drunc.session_manager.session_manager.get_logger") as mock_get_logger:
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance


@pytest.fixture(scope="function")
def session_manager(mock_logger):
    dummy_conf_handler = MagicMock()
    return SessionManager(name="dummy_name", configuration=dummy_conf_handler)


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def config_keys():
    """
    Fixture that provides a list of mock ConfigKey objects for testing.
    """
    return [
        ConfigKey(file=f"mock_file_{i}.data.xml", session_id=f"session_{j}")
        for i in range(1, 4)
        for j in range(1, 3)
    ]


# -----------------------------------------------------
#    Session Manager Endpoints Tests Fixtures
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
#    Session Manager Driver Rests Fixtures
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


# -----------------------------------------------------
#    Request Fixtures
# -----------------------------------------------------


@pytest.fixture(scope="session")
def generic_request():
    """
    Provide a generic Request for testing endpoints that accept any data.

    Returns:
        Request: Basic request containing token and arbitrary data payload
    """
    return Request(token=Token(), data=google.protobuf.any_pb2.Any(value=b"test_data"))


@pytest.fixture(scope="session")
def invalid_request_type():
    return "invalid_request_type"


@pytest.fixture(scope="session")
def request_type(request):
    """
    This fixture resolves the name to the actual fixture for expected_responses
    to be used in parameterised tests.
    """
    return request.getfixturevalue(request.param)


# -----------------------------------------------------
#    Response Fixtures
# -----------------------------------------------------


@pytest.fixture(scope="session")
def command_description_list():
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


@pytest.fixture(scope="session")
def describe_response(command_description_list):
    return Description(
        type="session_manager",
        name="dummy_session",
        commands=command_description_list,
        children=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        token=None,
    )


@pytest.fixture(scope="session")
def config_key():
    return ConfigKey(file="dummy_config_file", session_id="dummy_config_session_id")


@pytest.fixture(scope="session")
def active_session(config_key):
    return ActiveSession(name="dummy_session", user="dummy_user", config_key=config_key)


@pytest.fixture(scope="session")
def all_active_sessions_response(active_session):
    return AllActiveSessions(
        name="dummy_session",
        token=None,
        active_sessions=[active_session],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture(scope="session")
def all_config_keys_response(config_keys):
    return AllConfigKeys(
        name="dummy_session",
        token=None,
        config_keys=config_keys,
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )


@pytest.fixture
def expected_response(request):
    """
    This fixture resolves the name to the actual fixture for various expected 
    responses to be used in parameterised tests.
    """
    param = request.param
    if isinstance(param, str):
        return request.getfixturevalue(param)
    return param  # Direct value like MagicMock
