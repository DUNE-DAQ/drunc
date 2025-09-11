from unittest.mock import MagicMock

import grpc
import pytest
from druncschema.session_manager_pb2 import (
    DESCRIPTOR,
)
from grpc_testing import server_from_dictionary, strict_real_time

from drunc.session_manager.session_manager import SessionManager
from drunc.tests.session_manager.dummy_requests import GENERIC_REQUEST
from drunc.tests.session_manager.dummy_responses import (
    DUMMY_ALLACTIVESESSIONS_RESPONSE,
    DUMMY_ALLCONFIGKEYS_RESPONSE,
    DUMMY_DESCRIBE_RESPONSE,
)


@pytest.fixture(scope="function")
def grpc_servicer():
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
            endpoint_name (str): Name of the endpoint method to mock (e.g., 'kill', 'boot')
            expected_response: The response object to return from the mocked method
        Returns:
            tuple: (test_server, expected_response) for use in endpoint tests
        """
        # Mock the abstract implementation method for the specified endpoint
        mock_method = MagicMock(return_value=expected_response)
        setattr(grpc_servicer, endpoint_name, mock_method)
        # Register the servicer with the gRPC testing framework
        servicers = {DESCRIPTOR.services_by_name["SessionManager"]: grpc_servicer}
        test_server = server_from_dictionary(servicers, strict_real_time())
        return (test_server, expected_response)

    return create_server


def test_describe(grpc_test_server_factory):
    grpc_test_server, expected_response = grpc_test_server_factory(
        "describe", DUMMY_DESCRIBE_RESPONSE
    )
    describe_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name["describe"]
        ),
        request=GENERIC_REQUEST,
        invocation_metadata={},
        timeout=1,
    )
    response, metadata, code, details = describe_method.termination()
    assert expected_response == response
    assert code == grpc.StatusCode.OK


def test_list_all_sessions(grpc_test_server_factory):
    grpc_test_server, expected_response = grpc_test_server_factory(
        "list_all_sessions", DUMMY_ALLACTIVESESSIONS_RESPONSE
    )
    list_all_sessions_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name[
                "list_all_sessions"
            ]
        ),
        request=GENERIC_REQUEST,
        invocation_metadata={},
        timeout=1,
    )
    response, metadata, code, details = list_all_sessions_method.termination()
    assert expected_response == response
    assert code == grpc.StatusCode.OK


def test_list_all_configs(grpc_test_server_factory):
    grpc_test_server, expected_response = grpc_test_server_factory(
        "list_all_configs", DUMMY_ALLCONFIGKEYS_RESPONSE
    )
    list_all_configs_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name[
                "list_all_configs"
            ]
        ),
        request=GENERIC_REQUEST,
        invocation_metadata={},
        timeout=1,
    )
    response, metadata, code, details = list_all_configs_method.termination()
    assert expected_response == response
    assert code == grpc.StatusCode.OK
