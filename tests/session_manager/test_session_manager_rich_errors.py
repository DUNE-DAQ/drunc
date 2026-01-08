from concurrent import futures
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.session_manager_pb2_grpc import (
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)
from google.rpc import error_details_pb2, status_pb2

from drunc.session_manager.session_manager import SessionManager
from drunc.utils.grpc_utils import RichErrorServerInterceptor


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

        # Create client channel and stub
        self.channel = grpc.insecure_channel(self.server_address)
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


def test_list_all_configs_no_path_set(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    """
    Test when DUNEDAQ_DB_PATH is not set in the environment.
    """

    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub

    # Remove the DUNEDAQ_DB_PATH from the environment to simulate it's not set
    monkeypatch.delenv("DUNEDAQ_DB_PATH", raising=False)

    with pytest.raises(grpc.RpcError) as excinfo:
        stub.list_all_configs(generic_request)

    err = excinfo.value

    expected_error_msg = "DUNEDAQ_DB_PATH not set"
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert expected_error_msg in err.details()

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)

            # There should be a PreconditionFailure detail
            precond = error_details_pb2.PreconditionFailure()
            status.details[0].Unpack(precond)

            violation = precond.violations[0]
            assert violation.type == "MISSING_OR_INVALID"
            assert violation.subject == "DUNEDAQ_DB_PATH"
            assert expected_error_msg in violation.description


def test_no_config_files_rich_error(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub

    monkeypatch.setenv("DUNEDAQ_DB_PATH", "/fake_path")

    with pytest.raises(grpc.RpcError) as excinfo:
        stub.list_all_configs(generic_request)

    err = excinfo.value

    assert err.code() == grpc.StatusCode.NOT_FOUND
    assert "Configuration files not found" in err.details()

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)
            res_info = error_details_pb2.ResourceInfo()
            status.details[0].Unpack(res_info)

            assert res_info.resource_type == "SessionConfiguration"
            assert res_info.resource_name == ""

def test_config_parse_failure(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub

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
    assert "Configuration parse error" in err.details()

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)
            precond = error_details_pb2.PreconditionFailure()
            status.details[0].Unpack(precond)

            violation = precond.violations[0]
            assert violation.type == "CONFIG_PARSE_FAILURE"
            assert "mock_file_" in violation.subject
            assert violation.description == "Config failed"

def test_dals_missing_or_invalid(
    session_manager_rich_error_test_suite, generic_request, monkeypatch
):
    session_manager_rich_error_test_suite.setup_server_and_client()
    stub = session_manager_rich_error_test_suite.stub

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
    expected_error_msg = "Failed to get DALs"

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)
            precond = error_details_pb2.PreconditionFailure()
            status.details[0].Unpack(precond)

            violation = precond.violations[0]
            assert violation.type == "DALs_STRUCTURE_INVALID"
            assert "mock_file" in violation.subject
            assert expected_error_msg in violation.description
