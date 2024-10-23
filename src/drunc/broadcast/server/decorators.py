from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.generic_pb2 import Stacktrace
from drunc.utils.grpc_utils import pack_to_any
import traceback

def broadcasted(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    def wrap(obj, request, context):
        from logging import getLogger
        log = getLogger('broadcasted_decorator')
        # hummmm I feel like creating a level myself, but...
        # https://docs.python.org/3/howto/logging.html#custom-levels
        # lets not
        log.debug('Entering')
        from druncschema.broadcast_pb2 import BroadcastType
        from drunc.exceptions import DruncCommandException

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' attempting to execute \'{cmd.__name__}\'',
            btype = BroadcastType.ACK
        )

        ret = None

        try:
            log.debug('Executing wrapped function')
            ret = cmd(obj, request) # we strip the context here, no need for that anymore

        except Exception as e:
            from drunc.utils.utils import print_traceback
            print_traceback()

            stack = traceback.format_exc().split("\n")
            from drunc.exceptions import DruncException
            flag = ResponseFlag.DRUNC_EXCEPTION_THROWN if isinstance(e, DruncException) else ResponseFlag.UNHANDLED_EXCEPTION_THROWN
            return Response(
                name = obj.name,
                token = request.token,
                data = pack_to_any(
                    Stacktrace(
                        text = stack,
                    )
                ),
                flag = flag,
                children = []
            )

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' successfully executed \'{cmd.__name__}\'',
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS
        )
        log.debug('Exiting')
        return ret

    return wrap



def async_broadcasted(cmd):

    import functools

    @functools.wraps(cmd) # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    async def wrap(obj, request, context):
        from logging import getLogger
        log = getLogger('async_broadcasted_decorator')
        log.debug('Entering')
        from druncschema.broadcast_pb2 import BroadcastType
        from drunc.exceptions import DruncCommandException

        obj.broadcast(
            message = f'User \'{request.token.user_name}\' attempting to execute \'{cmd.__name__}\'',
            btype = BroadcastType.ACK
        )

        try:
            log.debug('Executing wrapped function')
            async for a in cmd(obj, request):
                yield a

        except Exception as e:
            stack = traceback.format_exc().split("\n")
            from drunc.utils.utils import print_traceback
            print_traceback()
            from drunc.exceptions import DruncException
            flag = ResponseFlag.DRUNC_EXCEPTION_THROWN if isinstance(e, DruncException) else ResponseFlag.UNHANDLED_EXCEPTION_THROWN

            yield Response(
                name = obj.name,
                token = request.token,
                data = pack_to_any(
                    Stacktrace(
                        text = stack
                    )
                ),
                flag = flag,
                children = []
            )


        obj.broadcast(
            message = f'User \'{request.token.user_name}\' successfully executed \'{cmd.__name__}\'',
            btype = BroadcastType.COMMAND_EXECUTION_SUCCESS
        )
        log.debug('Exiting')


    return wrap
