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
from druncschema.generic_pb2 import PlainText

from drunc.utils.grpc_utils import copy_token, handle_grpc_error
from drunc.utils.shell_utils import DecodedResponse, GRPCDriver


class ControllerDriver(GRPCDriver):
    def __init__(self, address: str, token, **kwargs):
        super().__init__(
            name="controller_driver", address=address, token=token, **kwargs
        )
        self.stub = ControllerStub(self.channel)

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
