
from drunc.exceptions import DruncCommandException,DruncException

class UnpackingError(DruncCommandException):
    def __init__(self, data, format):
        self.data = data
        self.format = format

        from google.rpc import code_pb2
        super().__init__(f'Cannot unpack {data.TypeName()} to {format.__name__}', code_pb2.INVALID_ARGUMENT)


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


def unpack_request_data_to(data_type=None, pass_token=False):

    def decor(cmd):

        import functools

        @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
        def unpack_request(obj, request):
            from logging import getLogger
            log = getLogger('unpack_request_data_to_decorator')
            log.debug('Entering')

            ret = None
            log.debug('Executing wrapped function')

            kwargs = {}
            if pass_token:
                kwargs = {'token': request.token}

            if data_type is not None:
                data = unpack_any(request.data, data_type)
                ret = cmd(obj, data, **kwargs)
            else:
                ret = cmd(obj, **kwargs)

            log.debug('Exiting')

            return ret
        return unpack_request

    return decor


def async_unpack_request_data_to(data_type=None, pass_token=False):

    def decor(cmd):

        import functools

        @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
        async def unpack_request(obj, request):
            from logging import getLogger
            log = getLogger('unpack_request_data_to_decorator')
            log.debug('Entering')

            log.debug('Executing wrapped function')

            kwargs = {}
            if pass_token:
                kwargs = {'token': request.token}

            if data_type is not None:
                data = unpack_any(request.data, data_type)
                async for a in cmd(obj, data, **kwargs):
                    yield a
            else:
                async for a in cmd(obj, **kwargs):
                    yield a

            log.debug('Exiting')

        return unpack_request

    return decor


def pack_response(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    def pack_response(obj, *arg, **kwargs):
        from logging import getLogger
        log = getLogger('pack_response_decorator')
        log.debug('Entering')

        from druncschema.request_response_pb2 import Response
        from druncschema.token_pb2 import Token
        from google.protobuf.any_pb2 import Any

        log.debug('Executing wrapped function')
        ret = cmd(obj, *arg, **kwargs)

        new_token = Token() # empty token
        data = Any()
        data.Pack(ret)
        ret = Response(
            token = new_token,
            data = data
        )

        log.debug('Exiting')
        return ret

    return pack_response



def async_pack_response(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    async def pack_response(obj, *arg, **kwargs):
        from logging import getLogger
        log = getLogger('pack_response_decorator')
        log.debug('Entering')

        log.debug('Executing wrapped function')
        async for ret in cmd(obj, *arg, **kwargs):

            from druncschema.request_response_pb2 import Response
            from druncschema.token_pb2 import Token
            from google.protobuf.any_pb2 import Any

            new_token = Token() # empty token
            data = Any()
            data.Pack(ret)
            yield Response(
                token = new_token,
                data = data
            )
        log.debug('Exiting')

    return pack_response


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


