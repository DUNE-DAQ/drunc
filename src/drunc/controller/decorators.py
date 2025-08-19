import time
import traceback
from functools import wraps

from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.opmon.FSM_pb2 import CommandTime
from druncschema.request_response_pb2 import Response, ResponseFlag

from drunc.exceptions import DruncException
from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import get_logger


def in_control(cmd):
    @wraps(cmd)
    def wrap(obj, request, context):
        if not obj.actor.token_is_current_actor(request.token):
            return Response(
                name=obj.name,
                token=request.token,
                data=pack_to_any(
                    PlainText(
                        text=f"User {request.token.user_name} is not in control of {obj.__class__.__name__}",
                    )
                ),
                flag=ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
                children=[],
            )
        return cmd(obj, request, context)

    return wrap


def publish_command_time(cmd):
    @wraps(cmd)
    def wrap(obj, *args, **kwargs):
        log = get_logger(f"controller.publish_command_time.{cmd}")

        cmd_start_time = time.time()
        try:
            log.debug("Executing wrapped function")
            ret = cmd(obj, *args, **kwargs)

        except Exception as e:
            log.exception(e)

            stack = traceback.format_exc().split("\n")

            flag = (
                ResponseFlag.DRUNC_EXCEPTION_THROWN
                if isinstance(e, DruncException)
                else ResponseFlag.UNHANDLED_EXCEPTION_THROWN
            )
            token = kwargs.get("token", None)
            return Response(
                name=obj.name,
                token=token,
                data=pack_to_any(
                    Stacktrace(
                        text=stack,
                    )
                ),
                flag=flag,
                children=[],
            )
        cmd_end_time = time.time()
        cmd_exe_time = cmd_end_time - cmd_start_time

        if (
            hasattr(obj, "controller_publisher")
            and obj.controller_publisher is not None
        ):
            payload = kwargs.get("payload", None)
            custom_origin = {"Command": cmd.__name__}

            if cmd.__name__ == "execute_fsm_command" and payload is not None:
                custom_origin = {"Command": payload.command_name}

            obj.controller_publisher(
                message=CommandTime(execution_time_ns=int(cmd_exe_time * 1e9)),
                custom_origin=custom_origin,
            )

        return ret

    return wrap
