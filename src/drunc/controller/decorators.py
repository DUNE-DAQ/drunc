import time
import traceback
from functools import wraps

from druncschema.controller_pb2 import AddressedCommand
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.opmon.FSM_pb2 import CommandTime
from druncschema.request_response_pb2 import Response, ResponseFlag

from drunc.controller.utils import address_command
from drunc.exceptions import DruncCommandException
from drunc.utils.grpc_utils import UnpackingError, pack_to_any, unpack_any
from drunc.utils.utils import get_logger


def in_control(cmd):
    @wraps(cmd)
    def wrap(obj, request):
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
        return cmd(obj, request)

    return wrap


def unpack_addressed_command_to(data_type=None):
    def decor(cmd):
        command_name = cmd.__name__
        logger = get_logger(f"controller.upack_add'ed_cmd.{command_name}")

        @wraps(cmd)
        def wrap(obj, request):
            try:
                if request.HasField("data"):
                    command = unpack_any(request.data, AddressedCommand)
                else:
                    command = AddressedCommand(
                        command_name=command_name,
                        command_data=None,
                        target=None,
                        execute_along_path=True,
                        execute_on_all_subsequent_children_in_path=True,
                    )
            except UnpackingError as e:
                logger.exception(e)
                return Response(
                    name=obj.name,
                    token=request.token,
                    data=pack_to_any(PlainText(text=str(e))),
                    flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
                    children=[],
                )

            if command.target == "/" or command.target is None or command.target == "":
                target = obj.name
            else:
                target = command.target

            try:
                addressed_commands = address_command(
                    obj=obj,
                    command_name=command_name,
                    command_data=command.command_data,
                    target=command.target,
                    execute_along_path=command.execute_along_path,
                    execute_on_all_subsequent_children_in_path=command.execute_on_all_subsequent_children_in_path,
                )
                logger.debug(f"Addressed commands: {addressed_commands}")
            except DruncCommandException as e:
                logger.exception(e)
                return Response(
                    name=obj.name,
                    token=request.token,
                    data=pack_to_any(PlainText(text=str(e))),
                    flag=ResponseFlag.FAILED,
                    children=[],
                )

            payload = None

            if data_type is not None:
                try:
                    payload = unpack_any(command.command_data, data_type)
                except UnpackingError as e:
                    logger.exception(e)
                    return Response(
                        name=obj.name,
                        token=request.token,
                        data=pack_to_any(PlainText(text=str(e))),
                        flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
                        children=[],
                    )

            kwargs = {
                "addressed_commands": addressed_commands,
                "execute_on_self": command.execute_along_path or obj.name == target,
                "token": request.token,
            }
            if payload is not None:
                kwargs["payload"] = payload

            ret = cmd(
                obj,
                **kwargs,
            )

            return ret

        return wrap

    return decor


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
            from drunc.exceptions import DruncException

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
