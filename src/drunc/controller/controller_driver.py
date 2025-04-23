from functools import wraps

from drunc_messages.controller_pb2 import (
    AddressedCommand,
    FSMCommandResponse,
    FSMCommandsDescription,
    Status,
)
from drunc_messages.controller_pb2_grpc import ControllerStub
from drunc_messages.generic_pb2 import PlainText
from drunc_messages.request_response_pb2 import Description

from drunc_core.utils.shell_utils import DecodedResponse, GRPCDriver


class ControllerDriver(GRPCDriver):
    def __init__(self, address: str, token, **kwargs):
        super(ControllerDriver, self).__init__(
            name="controller_driver", address=address, token=token, **kwargs
        )

    def create_stub(self, channel):
        return ControllerStub(channel)

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

    @pack_empty_addressed_command
    def describe(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command(
            "describe", data=addressed_command, outformat=Description
        )

    @pack_empty_addressed_command
    def describe_fsm(
        self, addressed_command: AddressedCommand, key: str = None
    ) -> DecodedResponse:
        new_command = AddressedCommand()
        new_command.CopyFrom(addressed_command)
        new_command.command_data.Pack(PlainText(text=key))
        return self.send_command(
            "describe_fsm", data=new_command, outformat=FSMCommandsDescription
        )

    @pack_empty_addressed_command
    def status(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command("status", data=addressed_command, outformat=Status)

    @pack_empty_addressed_command
    def recompute_status(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command(
            "recompute_status", data=addressed_command, outformat=Status
        )

    @pack_empty_addressed_command
    def take_control(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command(
            "take_control", data=addressed_command, outformat=PlainText
        )

    @pack_empty_addressed_command
    def who_is_in_charge(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command(
            "who_is_in_charge", data=addressed_command, outformat=PlainText
        )

    @pack_empty_addressed_command
    def surrender_control(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command(
            "surrender_control", data=addressed_command, outformat=PlainText
        )

    @pack_empty_addressed_command
    def execute_fsm_command(
        self, addressed_command: AddressedCommand, arguments
    ) -> DecodedResponse:
        new_command = AddressedCommand()
        new_command.CopyFrom(addressed_command)
        new_command.command_data.Pack(arguments)
        return self.send_command(
            "execute_fsm_command", data=new_command, outformat=FSMCommandResponse
        )

    @pack_empty_addressed_command
    def include(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command("include", data=addressed_command, outformat=PlainText)

    @pack_empty_addressed_command
    def exclude(self, addressed_command: AddressedCommand) -> DecodedResponse:
        return self.send_command("exclude", data=addressed_command, outformat=PlainText)

    @pack_empty_addressed_command
    def expert_command(
        self, addressed_command: AddressedCommand, json_string
    ) -> DecodedResponse:
        new_command = AddressedCommand()
        new_command.CopyFrom(addressed_command)
        new_command.command_data.Pack(PlainText(text=json_string))
        return self.send_command(
            "execute_expert_command", data=new_command, outformat=PlainText
        )
