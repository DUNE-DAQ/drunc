"""
Test grpc endpoints of the SessionManager class. a gROC server is simulated using grpc_testing.server_from_dictionary()
and individual endpoint methods are mocked directly on the servicer. The methods are invoked
using descriptors, bypassing the full gRPC transport layer.
"""

import grpc
import pytest
from druncschema.session_manager_pb2 import DESCRIPTOR


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", "describe_response"),
        ("list_all_sessions", "all_active_sessions_response"),
        ("list_all_configs", "all_config_keys_response"),
    ],
    indirect=[
        "expected_response"
    ],  # Tells pytest to treat 'expected_response' as fixture names
)
def test_endpoints(
    grpc_test_server_factory, method_name, expected_response, generic_request
):
    """
    Test that invoking the methods processes the correct requests and returns the correct response.
    """
    grpc_test_server, expected_response = grpc_test_server_factory(
        method_name, expected_response
    )

    method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name[method_name]
        ),
        request=generic_request,
        invocation_metadata={},
        timeout=1,
    )
    response, metadata, code, details = method.termination()
    assert expected_response == response
    assert code == grpc.StatusCode.OK
