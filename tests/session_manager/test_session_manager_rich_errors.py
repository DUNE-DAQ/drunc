from concurrent import futures
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.session_manager_pb2_grpc import (
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)

from drunc.session_manager.session_manager import SessionManager
from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    RichErrorServerInterceptor,
)


class SessionManagerRichErrorTestSuite:
    """Test suite for rich error message propagation for Session Manager."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None

    def setup_server_and_client(self):
        """Initialise a real gRPC server and client for testing rich error handling.

        Args:
            method_name: Name of the method to mock (e.g., 'describe')
            rich_error_details: Dictionary containing rich error details to be used in the mocked method.
                Keys are 'domain', 'message', and 'details'.
        """
        # Mock the logger to prevent logging interference during tests
        with patch("drunc.session_manager.session_manager.get_logger") as mock_logger:
            mock_logger_instance = MagicMock()
            mock_logger.return_value = mock_logger_instance
            self.servicer = SessionManager(name="dummy_session", configuration=[])

        # Configure and start the gRPC server
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=[RichErrorServerInterceptor()],
        )
        add_SessionManagerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        self.mock_client_logger = MagicMock()
        client_interceptor = RichErrorClientInterceptor(logger=self.mock_client_logger)

        # Create client channel and stub
        raw_channel = grpc.insecure_channel(self.server_address)
        self.channel = grpc.intercept_channel(raw_channel, client_interceptor)
        self.stub = SessionManagerStub(self.channel)

    def teardown_server_and_client(self):
        """Clean up gRPC server and client resources."""
        if self.channel:
            self.channel.close()
        if self.server:
            self.server.stop(grace=0)
        self.stub = None
        self.servicer = None


@pytest.fixture(scope="function")
def session_manager_rich_error_test_suite():
    """
    Pytest fixture for SessionManagerTestSuite.
    """
    suite = SessionManagerRichErrorTestSuite()
    yield suite
    suite.teardown_server_and_client()


def test_list_all_configs_no_config_files_rich_error(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    """
    Test when DUNEDAQ_DB_PATH is not set in the environment.
    """

    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub
    mock_logger = session_manager_rich_error_test_suite.mock_client_logger

    # Remove the DUNEDAQ_DB_PATH from the environment to simulate it's not set
    monkeypatch.delenv("DUNEDAQ_DB_PATH", raising=False)

    with pytest.raises(grpc.RpcError) as excinfo:
        stub.list_all_configs(generic_request)

    err = excinfo.value

    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "DUNEDAQ_DB_PATH" in err.details()

    # The interceptor calls log.error twice (once for the method, once for the details)
    assert mock_logger.error.call_count == 2

    # Extract the GrpcErrorDetails object that the interceptor logged
    logged_error_details = mock_logger.error.call_args_list[1][0][0]

    assert logged_error_details.code == "FAILED_PRECONDITION"

    # Access the PreconditionFailure detail object directly from the list
    precond_detail = logged_error_details.details[0]
    assert precond_detail.violations[0].type == "MISSING OR INVALID"
    assert (
        "DUNEDAQ_DB_PATH env variable not set"
        in precond_detail.violations[0].description
    )


def test_no_config_files_rich_error(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub
    mock_logger = session_manager_rich_error_test_suite.mock_client_logger

    monkeypatch.setenv("DUNEDAQ_DB_PATH", "/fake_path")

    with pytest.raises(grpc.RpcError) as excinfo:
        stub.list_all_configs(generic_request)

    err = excinfo.value

    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "Config files" in err.details()

    # The interceptor calls log.error twice (once for the method, once for the details)
    assert mock_logger.error.call_count == 2

    # Extract the GrpcErrorDetails object that the interceptor logged
    logged_error_details = mock_logger.error.call_args_list[1][0][0]

    assert logged_error_details.code == "FAILED_PRECONDITION"

    # Access the PreconditionFailure detail object directly from the list
    precond_detail = logged_error_details.details[0]
    assert precond_detail.violations[0].type == "MISSING OR INVALID"
    assert (
        "No configuration files found in /fake_path"
        in precond_detail.violations[0].description
    )


def test_config_parse_failure(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub
    mock_logger = session_manager_rich_error_test_suite.mock_client_logger

    # Set env var so search_paths is non-empty
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")

    # Mock files returned by Path.rglob
    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        # Force Configuration to raise
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            side_effect=Exception("Config failed"),
        ):
            with pytest.raises(grpc.RpcError) as excinfo:
                stub.list_all_configs(generic_request)

    err = excinfo.value
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION

    # The interceptor calls log.error twice (once for the method, once for the details)
    assert mock_logger.error.call_count == 2

    # Extract the GrpcErrorDetails object that the interceptor logged
    logged_error_details = mock_logger.error.call_args_list[1][0][0]

    # Access the PreconditionFailure detail object directly from the list
    precond_detail = logged_error_details.details[0]
    assert precond_detail.violations[0].type == "MISSING OR INVALID"
    assert "Config files" in precond_detail.violations[0].subject
    assert (
        "Failed to parse configuration file" in precond_detail.violations[0].description
    )


def test_dals_missing_or_invalid(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub
    mock_logger = session_manager_rich_error_test_suite.mock_client_logger

    # Set env var so search_paths is non-empty
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")

    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        # Patch Configuration to return an object whose get_dals raises
        fake_config = MagicMock()
        fake_config.get_dals.side_effect = Exception("DALs broken")
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=fake_config,
        ):
            with pytest.raises(grpc.RpcError) as excinfo:
                stub.list_all_configs(generic_request)

    err = excinfo.value
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert mock_logger.error.call_count == 2

    # Extract the GrpcErrorDetails object that the interceptor logged
    logged_error_details = mock_logger.error.call_args_list[1][0][0]

    # Access the PreconditionFailure detail object directly from the list
    precond_detail = logged_error_details.details[0]
    assert precond_detail.violations[0].type == "MISSING OR INVALID"
    assert "Session DALs" in precond_detail.violations[0].subject
    assert "DALs missing or invalid" in precond_detail.violations[0].description
