import abc
import getpass
from collections.abc import Mapping

import click
import grpc
from druncschema.generic_pb2 import PlainText, Stacktrace
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf.any_pb2 import Any
from rich.console import Console

from drunc.exceptions import (
    DruncServerSideError,
    DruncSetupException,
    DruncShellException,
)
from drunc.utils.grpc_utils import rethrow_if_unreachable_server, unpack_any
from drunc.utils.utils import get_logger, print_traceback, setup_root_logger


class InterruptedCommand(DruncShellException):
    """This exception gets thrown if we don't want to have a full stack, but still want to interrupt a **shell** command"""

    pass


def create_dummy_token_from_uname() -> Token:
    user = getpass.getuser()
    return (
        Token(  # fake token, but should be figured out from the environment/authoriser
            token=f"{user}-token", user_name=user
        )
    )


def add_traceback_flag():
    def wrapper(f0):
        f1 = click.option(
            "-t/-nt",
            "--traceback/--no-traceback",
            default=None,
            help="Print full exception traceback",
        )(f0)
        return f1

    return wrapper


class DecodedResponse:
    ## Warning! This should be kept in sync with druncschema/request_response.proto/Response class
    name = None
    token = None
    data = None
    flag = None
    children = []

    def __init__(self, name, token, flag, data=None, children=None):
        self.name = name
        self.token = token
        self.flag = flag
        self.data = data
        if children is None:
            self.children = []
        else:
            self.children = children

    @staticmethod
    def str(obj, prefix=""):
        text = f"{prefix} {obj.name} -> {obj.flag}\n"
        for v in obj.children:
            if v is None:
                continue
            text += DecodedResponse.str(v, prefix + "  ")
        return text

    def __str__(self):
        return DecodedResponse.str(self)


class GRPCDriver:
    def __init__(self, name: str, address: str, token: Token, aio_channel=False):
        self.log = get_logger("utils.GRPCDriver")

        if not address:
            raise DruncSetupException(
                f"You need to provide a valid IP address for the driver. Provided '{address}'"
            )

        self.address = address

        if aio_channel:
            self.channel = grpc.aio.insecure_channel(self.address)
        else:
            self.channel = grpc.insecure_channel(self.address)

        self.stub = self.create_stub(self.channel)
        self.token = Token()
        self.token.CopyFrom(token)

    @abc.abstractmethod
    def create_stub(self, channel):
        pass

    def _create_request(self, payload=None) -> Request:
        token2 = Token()
        token2.CopyFrom(self.token)
        data = Any()
        if payload is not None:
            data.Pack(payload)

        if payload:
            return Request(token=token2, data=data)
        else:
            return Request(token=token2)

    def __handle_grpc_error(self, error, command):
        rethrow_if_unreachable_server(error)
        # else:
        #     from drunc.utils.grpc_utils import interrupt_if_server_unreachable
        #     text = interrupt_if_unreachable_server(error)
        #     if text:
        #         self.log.error(text)

        # if hasattr(error, 'details'): #ARGG asyncio gRPC so different from synchronous one!!
        #     self.log.error(error.details())

        #     # of course, right now asyncio servers are not able to reply with a stacktrace (yet)
        #     # we just throw the client-side error and call it a day for now
        #     if rethrow:
        #         raise error

    def handle_response(self, response, command, outformat):
        dr = DecodedResponse(
            name=response.name,
            token=response.token,
            flag=response.flag,
        )
        if response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            if response.data not in [None, ""]:
                dr.data = unpack_any(response.data, outformat)

            for c_response in response.children:
                try:
                    dr.children.append(
                        self.handle_response(c_response, command, outformat)
                    )
                except DruncServerSideError as e:
                    self.log.error(f"Exception thrown from child: {e}")
            return dr

        else:

            def text(verb="not executed"):
                return f"Command '{command}' {verb} on '{response.name}' (response flag '{ResponseFlag.Name(response.flag)}')"

            if response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]:
                self.log.debug(text())
            elif response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
            ]:
                self.log.warn(text())
            else:
                self.log.error(text("failed"))

            if not response.HasField("data"):
                return None

            error_txt = ""
            stack_txt = None

            if response.data.Is(Stacktrace.DESCRIPTOR):
                stack = unpack_any(response.data, Stacktrace)
                # stack_txt = 'Stacktrace [bold red]on remote server![/bold red]\n' # Temporary - bold doesn't work
                stack_txt = "Stacktrace on remote server!\n"
                last_one = ""
                for l in stack.text:
                    stack_txt += l + "\n"
                    if l != "":
                        last_one = l
                error_txt = last_one

            elif response.data.Is(PlainText.DESCRIPTOR):
                txt = unpack_any(response.data, PlainText)
                error_txt = txt.text
            self.log.error(error_txt)

            if stack_txt:
                self.log.debug(stack_txt)

            dr.data = response.data
            for c_response in response.children:
                try:
                    dr.children.append(
                        self.handle_response(c_response, command, outformat)
                    )
                except DruncServerSideError as e:
                    self.log.error(f"Exception thrown from child: {e}")
            return dr

    def send_command(
        self, command: str, data=None, outformat=None, decode_children=False
    ):
        if not self.stub:
            raise DruncShellException("No stub initialised")

        cmd = getattr(self.stub, command)  # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = cmd(request)
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, command)
        return self.handle_response(response, command, outformat)

    async def send_command_aio(self, command: str, data=None, outformat=None):
        if not self.stub:
            raise DruncShellException("No stub initialised")

        cmd = getattr(self.stub, command)  # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = await cmd(request)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command)
        return self.handle_response(response, command, outformat)

    async def send_command_for_aio(self, command: str, data=None, outformat=None):
        if not self.stub:
            raise DruncShellException("No stub initialised")

        cmd = getattr(self.stub, command)  # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            async for s in cmd(request):
                yield self.handle_response(s, command, outformat)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command)


class ShellContext:
    def _reset(self, name: str, token_args: dict = {}, driver_args: dict = {}):
        self._console = Console()
        self._token = self.create_token(**token_args)
        self._drivers: Mapping[str, GRPCDriver] = self.create_drivers(**driver_args)

    def __init__(self, *args, **kwargs):
        setup_root_logger("NOTSET")
        try:
            self.reset(*args, **kwargs)
        except Exception as e:
            print_traceback(e)
            exit(1)

    @abc.abstractmethod
    def reset(self, **kwargs):
        pass

    @abc.abstractmethod
    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        pass

    @abc.abstractmethod
    def create_token(self, **kwargs) -> Token:
        pass

    @abc.abstractmethod
    def terminate(self) -> None:
        pass

    def set_driver(self, name: str, driver: GRPCDriver) -> None:
        if name in self._drivers:
            raise DruncShellException(f"Driver {name} already present in this context")
        self._drivers[name] = driver

    def get_driver(self, name: str = None) -> GRPCDriver:
        try:
            if name:
                return self._drivers[name]
            elif len(self._drivers) > 1:
                raise DruncShellException("More than one driver in this context")
            return list(self._drivers.values())[0]
        except KeyError:
            log = get_logger("utils.ShellContext")
            log.exception(
                "Controller-specific commands cannot be sent until the session is booted"
            )
            log.debug(f"Drivers available are {self._drivers.keys()}")
            raise SystemExit(
                1
            )  # used to avoid having to catch multiple Attribute errors when this function gets called

    def has_driver(self, name: str) -> bool:
        return name in self._drivers

    def delete_driver(self, name: str) -> None:
        log = get_logger("utils.ShellContext")
        if name in self._drivers:
            log.info(
                f"You will not be able to issue command to {self._drivers[name].name} anymore."
            )
            del self._drivers[name]
            log.info(f"Driver '{name}' has been deleted.")

    def get_token(self) -> Token:
        return self._token

    def print(self, *args, **kwargs) -> None:
        self._console.print(*args, **kwargs)  # rich tables require console printing

    def rule(self, *args, **kwargs) -> None:
        self._console.rule(*args, **kwargs)

    def print_status_summary(self) -> None:
        log = get_logger("utils.ShellContext")
        status = self.get_driver("controller").status().data
        if status.in_error:
            log.error(
                f"[red] FSM is in error ({status})[/red], not currently accepting new commands."
            )
        else:
            available_actions = [
                command.name.replace("_", "-")
                for command in self.get_driver("controller")
                .describe_fsm()
                .data.commands
            ]
            log.info(
                f"Current FSM status is [green]{status.state}[/green]. Available transitions are [green]{'[/green], [green]'.join(available_actions)}[/green]."
            )
