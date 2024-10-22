import logging

def in_control(cmd):
    log = logging.getLogger('in_control')
    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):

        if not obj.actor.token_is_current_actor(request.token):
            log.error(f'Actor is {obj.actor._token}, requester is {request.token}, user is not in control')

            from druncschema.request_response_pb2 import Response, ResponseFlag
            from druncschema.generic_pb2 import PlainText
            from drunc.utils.grpc_utils import pack_to_any

            return Response(
                name = obj.name,
                token = request.token,
                data = pack_to_any(
                    PlainText(
                        text = f"User {request.token.user_name} is not in control of {obj.__class__.__name__}",
                    )
                ),
                flag = ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
                children = []
            )

        return cmd(obj, request)

    return wrap


def with_command_lock(cmd):
    log = logging.getLogger('with_command_lock')

    from functools import wraps

    @wraps(cmd)
    def wrap(obj, request):

        if obj._command_lock.acquire(timeout=10):
            ret = cmd(obj, request)
            obj._command_lock.release()
            return ret
        else:
            log.error(f'Someone is already executing a command on this controller, lasting more than 10 seconds!')

            return Response(
                name = obj.name,
                token = request.token,
                data = pack_to_any(
                    PlainText(
                        text = f"Another command is being executed on {obj.__class__.__name__}",
                    )
                ),
                flag = ResponseFlag.FAILED
            )

    return wrap