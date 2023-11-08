
from drunc.exceptions import DruncCommandException,DruncException
class UnpackingError(DruncCommandException):
    def __init__(self, data, format):
        self.data = data
        self.format = format

        from google.rpc import code_pb2
        super().__init__(f'Cannot unpack {data} to {format.DESCRIPTOR}', code_pb2.INVALID_ARGUMENT)

def pack_to_any(data):
    from google.protobuf import any_pb2
    any = any_pb2.Any()
    any.Pack(data)
    return any

def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        raise UnpackingError(data, format)
    req = format()
    data.Unpack(req)
    return req


# A simpler exception for simple error please!
class ServerUnreachable(DruncException):
    def __init__(self, message):
        self.message = message
        from google.rpc import code_pb2
        super(ServerUnreachable, self).__init__(message, code_pb2.UNAVAILABLE)

def server_is_reachable(grpc_error):
    import grpc
    if hasattr(grpc_error, '_state'):
        if grpc_error._state.code == grpc.StatusCode.UNAVAILABLE:
            return False

    elif hasattr(grpc_error, '_code'): # the async server case AC#%4tg%^1:"|5!!!!
        if grpc_error._code == grpc.StatusCode.UNAVAILABLE:
            return False

    return True

def rethrow_if_unreachable_server(grpc_error):
    # Come on ! Such a common error and I need to do all this crap to get the address of the service, not even it's own pre-defined message
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, '_state'):
            raise ServerUnreachable(grpc_error._state.details) from grpc_error
        elif hasattr(grpc_error, '_details'): # -1 for gRPC not throwing the same exception in case the server is async
            raise ServerUnreachable(grpc_error._details) from grpc_error

def interrupt_if_unreachable_server(grpc_error):
    # Come on ! Such a common error and I need to do all this crap to get the address of the service, not even it's own pre-defined message
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, '_state'):
            return grpc_error._state.details
        elif hasattr(grpc_error, '_details'): # -1 for gRPC not throwing the same exception in case the server is async
            return grpc_error._details


