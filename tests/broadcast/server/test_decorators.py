import pytest
from unittest.mock import MagicMock, patch

from druncschema.token_pb2 import Token
from druncschema.request_response_pb2 import Request

from drunc.broadcast.server.decorators import broadcasted
from druncschema.broadcast_pb2 import BroadcastType

class MockException(Exception):
    pass

@pytest.fixture
def mock_obj():
    """Mock the object that has the .broadcast() method."""
    obj = MagicMock()
    obj.name = "test-node"
    return obj


@pytest.fixture(scope="function")
def mock_request():
    return Request(token=Token(user_name="test", token="tets-token"))
    

@pytest.fixture
def mock_context():
    return MagicMock()


def test_broadcasted_success(mock_obj, mock_request, mock_context):

    @broadcasted
    def dummy_command(obj, request, context):
        return "Success"

    result = dummy_command(mock_obj, mock_request, mock_context)

    assert result == "Success"
    
    assert mock_obj.broadcast.call_count == 2 # ACK and COMMAND_EXECUTION_SUCCESS
    
    # first call - ACK
    args, kwargs = mock_obj.broadcast.call_args_list[0]
    assert kwargs['message'] == "User 'test' executing 'dummy_command'"
    assert kwargs['btype'] == BroadcastType.ACK

    # second call - COMMAND_EXECUTION_SUCCESS
    # check no missing or additional arguments
    mock_obj.broadcast.assert_called_with(
        message="User 'test' successfully executed 'dummy_command'",
        btype=BroadcastType.COMMAND_EXECUTION_SUCCESS)


def test_broadcasted_failure(mock_obj, mock_request, mock_context):

    # command that raises an error
    @broadcasted
    def dummy_command(obj, request, context):
        raise MockException("Test exception")

    with pytest.raises(MockException):
        dummy_command(mock_obj, mock_request, mock_context)

    assert mock_obj.broadcast.call_count == 2 # ACK and Exception
    
    # check no missing or additional arguments passed to broadcast
    mock_obj.broadcast.assert_called_with(
        message="Command 'dummy_command' failed",
        btype=BroadcastType.UNHANDLED_EXCEPTION_RAISED)
    