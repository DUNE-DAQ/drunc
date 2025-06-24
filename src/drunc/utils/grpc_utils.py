import functools

import grpc
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf import any_pb2
from google.protobuf.any_pb2 import Any
from google.rpc import code_pb2

from drunc.exceptions import DruncCommandException, DruncException
from drunc.utils.utils import get_logger


class UnpackingError(DruncCommandException):
    def __init__(self, data, format):
        self.data = data
        self.format = format

        super().__init__(
            f"Cannot unpack '{data}' to '{format.DESCRIPTOR.name}'",
            code_pb2.INVALID_ARGUMENT,
        )


def unpack_error_response(name: str, text: str, token: Token) -> Response:
    """Create a response for unpacking errors.

    Args:
        name: The name of the command or service.
        text: The error message to include in the response.
        token: The token associated with the request.

    Returns:
        response: the response object containing the error message.
    """
    return Response(
        name=name,
        token=token,
        data=pack_to_any(PlainText(text=text)),
        flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
        children=[],
    )


def pack_to_any(data):
    any = any_pb2.Any()
    any.Pack(data)
    return any


def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        raise UnpackingError(data, format)
    req = format()
    data.Unpack(req)
    return req


class ServerUnreachable(DruncException):
    def __init__(self, message):
        self.message = message
        super(ServerUnreachable, self).__init__(message)


class ServerTimeout(DruncException):
    def __init__(self, message):
        self.message = message
        super(ServerTimeout, self).__init__(message)


def server_is_reachable(grpc_error):
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.UNAVAILABLE:
            return False

    elif hasattr(grpc_error, "_code"):
        if grpc_error._code == grpc.StatusCode.UNAVAILABLE:
            return False

    return True


def rethrow_if_unreachable_server(grpc_error):
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, "_state"):
            raise ServerUnreachable(grpc_error._state.details) from grpc_error
        elif hasattr(grpc_error, "_details"):
            raise ServerUnreachable(grpc_error._details) from grpc_error


def rethrow_if_timeout(grpc_error):
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise ServerTimeout(grpc_error._state.details) from grpc_error


def interrupt_if_unreachable_server(grpc_error):
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, "_state"):
            return grpc_error._state.details
        elif hasattr(grpc_error, "_details"):
            return grpc_error._details
