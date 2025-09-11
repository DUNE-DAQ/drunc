"""
This tests the full communication flow by starting a real gRPC server and
connecting to it using an actual client stub.
"""

from concurrent import futures
from unittest.mock import patch

import grpc
import pytest
from druncschema.session_manager_pb2_grpc import (
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)

from drunc.session_manager.session_manager import SessionManager
from drunc.tests.session_manager.dummy_requests import GENERIC_REQUEST
from drunc.tests.session_manager.dummy_responses import (
    DUMMY_ALLACTIVESESSIONS_RESPONSE,
    DUMMY_ALLCONFIGKEYS_RESPONSE,
    DUMMY_DESCRIBE_RESPONSE,
)


class FaultySessionManager(SessionManager):
    """
    Faulty implementation of SessionManager that simulates internal errors.
    """

    def describe(self, request, context):
        context.abort(grpc.StatusCode.INTERNAL, "Simulated internal error - describe")

    def list_all_sessions(self, request, context):
        context.abort(
            grpc.StatusCode.INTERNAL, "Simulated internal error - list_all_sessions"
        )

    def list_all_configs(self, request, context):
        context.abort(
            grpc.StatusCode.INTERNAL, "Simulated internal error - list_all_configs"
        )


@pytest.fixture(scope="function")
def grpc_server():
    """
    Start a real gRPC server.
    """
    servicer = SessionManager(name="dummy_session", configuration=[])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SessionManagerServicer_to_server(servicer, server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = SessionManagerStub(channel)

    yield stub, server, channel

    channel.close()
    server.stop(grace=0)


@pytest.fixture(scope="function")
def faulty_grpc_server():
    """
    Start a gRPC server with a FaultySessionManager that always fails.
    """

    servicer = FaultySessionManager(name="dummy_session", configuration=[])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SessionManagerServicer_to_server(servicer, server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = SessionManagerStub(channel)

    yield stub, server, channel

    channel.close()
    server.stop(grace=0)


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", DUMMY_DESCRIBE_RESPONSE),
        ("list_all_sessions", DUMMY_ALLACTIVESESSIONS_RESPONSE),
        ("list_all_configs", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
)
def test_grpc_methods_success(
    method_name, expected_response, grpc_server, mock_config_environment
):
    """
    Check successful gRPC responses from SessionManager.
    Args:
        method_name (str): Name of the RPC method to test.
        expected_response (protobuf): Expected response object.
        grpc_server (fixture): Real gRPC server fixture.
        mock_config_environment (MagicMock): Mocked configuration object.
    """

    stub, _, _ = grpc_server
    if method_name == "list_all_configs":
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config_environment,
        ):
            response = getattr(stub, method_name)(GENERIC_REQUEST)
    else:
        response = getattr(stub, method_name)(GENERIC_REQUEST)
    assert response == expected_response


@pytest.mark.parametrize(
    "method_name", ["describe", "list_all_sessions", "list_all_configs"]
)
def test_grpc_methods_faulty(method_name, faulty_grpc_server, mock_config_environment):
    """
    Check gRPC error handling when methods fail.
    Args:
        method_name (str): Name of the RPC method to test.
        faulty_grpc_server (fixture): Server with FaultySessionManager.
        mock_config_environment (MagicMock): Mocked configuration object.
    """
    stub, _, _ = faulty_grpc_server

    if method_name == "list_all_configs":
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config_environment,
        ):
            with pytest.raises(grpc.RpcError) as exc_info:
                getattr(stub, method_name)(GENERIC_REQUEST)
    else:
        with pytest.raises(grpc.RpcError) as exc_info:
            getattr(stub, method_name)(GENERIC_REQUEST)

    assert exc_info.value.code() == grpc.StatusCode.INTERNAL
    assert f"Simulated internal error - {method_name}" in exc_info.value.details()
