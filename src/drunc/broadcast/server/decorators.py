import traceback

from druncschema.generic_pb2 import Stacktrace
from druncschema.request_response_pb2 import Response, ResponseFlag

from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger


def broadcasted(cmd):
    import functools

    @functools.wraps(
        cmd
    )  # this nifty decorator of decorator (!) is nicely preserving the cmd.__name__ (i.e. signature)
    def wrap(obj, request, context):
        log = get_logger("broadcasted_decorator", rich_handler=True)

        # hummmm I feel like creating a level myself, but...
        # https://docs.python.org/3/howto/logging.html#custom-levels
        # lets not
        log.debug("Entering")
        from druncschema.broadcast_pb2 import BroadcastType

        msg = f"User '{request.token.user_name}' executing '{cmd.__name__}'"

        log.debug(msg)

        obj.broadcast(message=msg, btype=BroadcastType.ACK)

        ret = None
        try:
            log.debug("Executing wrapped function")
            ret = cmd(obj, request, context)

        except Exception as e:
            log.exception(e)

            stack = traceback.format_exc().split("\n")
            from drunc.exceptions import DruncException

            flag = (
                ResponseFlag.DRUNC_EXCEPTION_THROWN
                if isinstance(e, DruncException)
                else ResponseFlag.UNHANDLED_EXCEPTION_THROWN
            )
            # Wrap the stack trace in a Response message to broadcast but still
            # raise the exception to the client so the interceptor can handle it

            error_wrap = Response(
                name=obj.name,
                token=request.token,
                data=pack_to_any(
                    Stacktrace(
                        text=stack,
                    )
                ),
                flag=flag,
                children=[],
            )

            obj.broadcast(
                message=f"Command '{cmd.__name__}' failed", 
                btype=BroadcastType.UNHANDLED_EXCEPTION_RAISED,
                data=error_wrap
            )

            raise e

        msg = f"User '{request.token.user_name}' successfully executed '{cmd.__name__}'"

        obj.broadcast(message=msg, btype=BroadcastType.COMMAND_EXECUTION_SUCCESS)
        log.debug(msg)

        log.debug("Exiting")
        return ret

    return wrap
