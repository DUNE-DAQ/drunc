"""
Test grpc endpoints of the SessionManager class. a gROC server is simulated using grpc_testing.server_from_dictionary()
and individual endpoint methods are mocked directly on the servicer. The methods are invoked
using descriptors, bypassing the full gRPC transport layer.
"""

import grpc
import pytest
from druncschema.session_manager_pb2 import (
    DESCRIPTOR,
)

from drunc.tests.session_manager.dummy_requests import GENERIC_REQUEST
from drunc.tests.session_manager.dummy_responses import (
    DUMMY_ALLACTIVESESSIONS_RESPONSE,
    DUMMY_ALLCONFIGKEYS_RESPONSE,
    DUMMY_DESCRIBE_RESPONSE,
)


@pytest.mark.parametrize(
    "method_name, dummy_expected_response",
    [
        ("describe", DUMMY_DESCRIBE_RESPONSE),
        ("list_all_sessions", DUMMY_ALLACTIVESESSIONS_RESPONSE),
        ("list_all_configs", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
)
def test_endpoints(grpc_test_server_factory, method_name, dummy_expected_response):
    """
    Test that invoking the methods processes the correct requests and returns the correct response.
    """
    grpc_test_server, expected_response = grpc_test_server_factory(
        method_name, dummy_expected_response
    )

    method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["SessionManager"].methods_by_name[method_name]
        ),
        request=GENERIC_REQUEST,
        invocation_metadata={},
        timeout=1,
    )
    response, metadata, code, details = method.termination()
    assert expected_response == response
    assert code == grpc.StatusCode.OK
