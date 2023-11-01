

class UnpackingError(Exception):
    def __init__(self, data, format):
        self.data = data
        self.format = format
        super().__init__(f'Cannot unpack {data} to {format.DESCRIPTOR}')

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
class ServerUnreachable(Exception):
    def __init__(self, message):
        self.message = message
        super(ServerUnreachable, self).__init__(message)

def server_is_reachable(grpc_error):
    if hasattr(grpc_error, '_state'):
        import grpc
        if grpc_error._state.code == grpc.StatusCode.UNAVAILABLE:
            return False

    elif hasattr(grpc_error, '_code'): # the async server case
        import grpc
        if grpc_error._code == grpc.StatusCode.UNAVAILABLE:
            return False

    return True

def rethrow_if_unreachable_server(grpc_error):
    if not server_is_reachable(grpc_error):
        # Come on ! Such a common error and I need to do all this crap to get the address of the service, not even it it's own pre-defined message
        if hasattr(grpc_error, '_state'):
            raise ServerUnreachable(grpc_error._state.details) from grpc_error
        elif hasattr(grpc_error, '_details'): # -1 for gRPC not throwing the same exception in case the server is async
            raise ServerUnreachable(grpc_error._details) from grpc_error

