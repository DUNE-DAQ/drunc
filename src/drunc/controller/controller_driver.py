from functools import wraps

import grpc
from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    FSMCommandResponse,
    Status,
    StatusResponse,
)
from druncschema.controller_pb2_grpc import ControllerStub
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.token_pb2 import Token

from drunc.exceptions import DruncServerSideError
from drunc.utils.grpc_utils import (
    UnpackingError,
    copy_token,
    handle_grpc_error,
    unpack_any,
)
from drunc.utils.shell_utils import DecodedResponse
from drunc.utils.utils import get_logger


class ControllerDriver:
    def __init__(self, address: str, token: Token):
        self.log = get_logger("controller.ControllerDriver")
        self.address = address
        self.channel = grpc.insecure_channel(self.address)
        self.stub = ControllerStub(self.channel)
        self.token = copy_token(token)

    def pack_empty_addressed_command(cmd):
        @wraps(cmd)
        def wrapper(
            self,
            target: str = "",
            execute_along_path: bool = True,
            execute_on_all_subsequent_children_in_path: bool = True,
            **kwargs,
        ):
            command_name = cmd.__name__
            return cmd(
                self,
                addressed_command=AddressedCommand(
                    command_name=command_name,
                    command_data=None,
                    target=target,
                    execute_along_path=execute_along_path,
                    execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
                ),
                **kwargs,
            )

        return wrapper

    def status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        timeout: int | float = 60,
    ) -> StatusResponse:
        request = AddressedCommand(
            token=copy_token(self.token),
            command_name="status",
            command_data=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.status(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def describe(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        timeout: int | float = 60,
    ) -> DescribeResponse:
        request = AddressedCommand(
            token=copy_token(self.token),
            command_name="describe",
            command_data=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        try:
            response = self.stub.describe(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    def describe_fsm(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        key: str | None = None,
        timeout: int | float = 60,
    ) -> DescribeFSMResponse:
        request = AddressedCommand(
            token=copy_token(self.token),
            command_name="describe_fsm",
            command_data=None,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )

        if key is not None:
            request.command_data.Pack(PlainText(text=key))

        try:
            response = self.stub.describe_fsm(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return response

    @pack_empty_addressed_command
    def recompute_status(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "recompute_status",
            data=addressed_command,
            outformat=Status,
            timeout=timeout,
        )

    @pack_empty_addressed_command
    def take_control(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "take_control", data=addressed_command, outformat=PlainText, timeout=timeout
        )

    @pack_empty_addressed_command
    def who_is_in_charge(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "who_is_in_charge",
            data=addressed_command,
            outformat=PlainText,
            timeout=timeout,
        )

    @pack_empty_addressed_command
    def surrender_control(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "surrender_control",
            data=addressed_command,
            outformat=PlainText,
            timeout=timeout,
        )

    @pack_empty_addressed_command
    def execute_fsm_command(
        self, addressed_command: AddressedCommand, arguments, timeout: int | float = 60
    ) -> DecodedResponse:
        new_command = AddressedCommand()
        new_command.CopyFrom(addressed_command)
        new_command.command_data.Pack(arguments)
        return self.send_command(
            "execute_fsm_command",
            data=new_command,
            outformat=FSMCommandResponse,
            timeout=timeout,
        )

    @pack_empty_addressed_command
    def include(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "include", data=addressed_command, outformat=PlainText, timeout=timeout
        )

    @pack_empty_addressed_command
    def exclude(
        self, addressed_command: AddressedCommand, timeout: int | float = 60
    ) -> DecodedResponse:
        return self.send_command(
            "exclude", data=addressed_command, outformat=PlainText, timeout=timeout
        )

    @pack_empty_addressed_command
    def expert_command(
        self,
        addressed_command: AddressedCommand,
        json_string,
        timeout: int | float = 60,
    ) -> DecodedResponse:
        new_command = AddressedCommand()
        new_command.CopyFrom(addressed_command)
        new_command.command_data.Pack(PlainText(text=json_string))
        return self.send_command(
            "execute_expert_command",
            data=new_command,
            outformat=PlainText,
            timeout=timeout,
        )

    def handle_response(self, response, command, outformat):
        dr = DecodedResponse(
            name=response.name,
            token=response.token,
            flag=response.flag,
        )

        if response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            if response.HasField("data") and response.data not in [None, ""]:
                try:
                    dr.data = unpack_any(response.data, outformat)
                except UnpackingError as e:
                    self.log.error(f"Error unpacking data: {e}")
                    dr.data = response.data

        else:

            def text(verb="not executed", reason=""):
                return f"Command '{command}' {verb} on '{response.name}' (response flag '{ResponseFlag.Name(response.flag)}') {reason}"

            if not response.HasField("data"):
                return None

            error_txt = ""
            stack_txt = None

            if response.data.Is(Stacktrace.DESCRIPTOR):
                stack = unpack_any(response.data, Stacktrace)
                dr.data = stack
                stack_txt = "Stacktrace on remote server!\n"
                last_one = ""

                for l in stack.text:
                    stack_txt += l + "\n"
                    if l != "":
                        last_one = l
                error_txt = last_one

            elif response.data.Is(PlainText.DESCRIPTOR):
                txt = unpack_any(response.data, PlainText)
                error_txt = txt.text  # noqa: F841  (might need to revisit this)
                dr.data = error_txt

            if response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]:
                self.log.debug(text())
            elif response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
            ]:
                self.log.warning(text())
            else:
                self.log.error(text("failed", error_txt))

        for c_response in response.children:
            try:
                dr.children.append(self.handle_response(c_response, command, outformat))
            except DruncServerSideError as e:
                self.log.error(f"Exception thrown from child: {e}")

        return dr

    def send_command(
        self,
        command: str,
        data=None,
        outformat=None,
        timeout: int | float = 60,
    ):
        request = Request(token=copy_token(self.token))
        if data is not None:
            request.data.Pack(data)

        try:
            cmd = getattr(self.stub, command)
            response = cmd(request, timeout=timeout)
        except grpc.RpcError as e:
            handle_grpc_error(e)

        return self.handle_response(response, command, outformat)

    # TODO: do the equivalent of handle_response for controller, PM and SM.
