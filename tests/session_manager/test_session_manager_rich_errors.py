from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.session_manager_pb2_grpc import (
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)
from google.rpc import code_pb2, error_details_pb2, status_pb2

from drunc.session_manager.session_manager import SessionManager
from drunc.utils.grpc_utils import respond_with_rich_error_status


class SessionManagerRichErrorTestSuite:
    """Test suite for rich error message propagation for Session Manager."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None

    def setup_server_and_client(self, method_name: str, rich_error_details: dict):
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

        def _stub(request, context):
            respond_with_rich_error_status(
                context,
                domain=rich_error_details.get("domain"),
                message=rich_error_details.get("message"),
                error_details=rich_error_details.get("details"),
            )

        setattr(self.servicer, method_name, _stub)

        # Configure and start the gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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


def test_list_all_configs_rich_error(
    session_manager_rich_error_test_suite, generic_request
):
    rich_error_details = {
        "domain": "SessionManager",
        "details": "Fake Error",
        "message": "Unhandled error in list_all_configs",
    }

    session_manager_rich_error_test_suite.setup_server_and_client(
        method_name="list_all_configs", rich_error_details=rich_error_details
    )

    with pytest.raises(grpc.RpcError) as exc:
        session_manager_rich_error_test_suite.stub.list_all_configs(generic_request)

    err = exc.value
    assert exc.value.code() == grpc.StatusCode.INTERNAL
    assert err.code() == grpc.StatusCode.INTERNAL

    # Rich error assertions
    found_status = None
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            found_status = status_pb2.Status()  # serialised google.rpc.Status
            found_status.ParseFromString(
                value
            )  # deserialise into a status_pb2.Status object

    assert found_status.message == rich_error_details["message"]
    assert found_status.code == code_pb2.INTERNAL

    # Unpack the ErrorInfo
    error_info = error_details_pb2.ErrorInfo()
    found_status.details[0].Unpack(error_info)
    assert error_info.domain == "drunc.SessionManager"
    assert error_info.reason == rich_error_details["message"]
    assert error_info.metadata["error"] == rich_error_details["details"]
