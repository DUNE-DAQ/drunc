from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager_driver import SessionManagerDriver
from drunc.tests.session_manager.dummy_requests import GENERIC_REQUEST
from drunc.tests.session_manager.dummy_responses import (
    DUMMY_ALLACTIVESESSIONS_RESPONSE,
    DUMMY_ALLCONFIGKEYS_RESPONSE,
    DUMMY_DESCRIBE_RESPONSE,
)


class FakeRpcError(grpc.RpcError):
    def __init__(self, message="Dummy error"):
        super().__init__()
        self._message = message

    def code(self):
        return grpc.StatusCode.INTERNAL

    def details(self):
        return self._message


def mock_raise_exception(ex):
    raise ex


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


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", DUMMY_DESCRIBE_RESPONSE),
        ("list_all_sessions", DUMMY_ALLACTIVESESSIONS_RESPONSE),
        ("list_all_configs", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
)
def test_grpc_success(mock_driver, method_name, expected_response):
    # Configure mock stub to return expected response
    getattr(mock_driver._mock_stub, method_name).return_value = expected_response

    # Call the method under test
    response = getattr(mock_driver, method_name)()

    getattr(mock_driver._mock_stub, method_name).assert_called_once()

    call_args = getattr(mock_driver._mock_stub, method_name).call_args
    request = call_args[0][0]

    assert hasattr(request, "token")
    assert response == expected_response


@pytest.mark.parametrize(
    "method_name",
    [
        "describe",
        "list_all_sessions",
        "list_all_configs",
    ],
)
def test_grpc_error(mock_driver, method_name):
    # Set the side effect for the correct stub method
    getattr(mock_driver._mock_stub, method_name).side_effect = FakeRpcError

    with patch(
        "drunc.session_manager.session_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = lambda e: mock_raise_exception(e)

        with pytest.raises(FakeRpcError) as err:
            # Dynamically call the method on the driver
            getattr(mock_driver, method_name)(GENERIC_REQUEST)

        mock_handler.assert_called_once()
        assert str(err.value.details()) == "Dummy error"
