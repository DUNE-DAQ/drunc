import abc
from druncschema.token_pb2 import Token
from druncschema.request_response_pb2 import Request
from typing import Mapping
from drunc.exceptions import DruncShellException

class InterruptedCommand(DruncShellException):
    '''
    This exception gets thrown if we don't want to have a full stack, but still want to interrupt a **shell** command
    '''
    pass


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
        text = f'{prefix} {obj.name} -> {obj.flag}\n'
        for v in obj.children:
            if v is None:
                continue
            text += DecodedResponse.str(v, prefix+"  ")
        return text

    def __str__(self):
        return DecodedResponse.str(self)


class GRPCDriver:
    def __init__(self, name:str, address:str, token:Token, aio_channel=False):
        import logging
        self._log = logging.getLogger(name)
        import grpc
        from druncschema.token_pb2 import Token

        if not address:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(f'You need to provide a valid IP address for the driver. Provided \'{address}\'')

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
        from google.protobuf.any_pb2 import Any

        token2 = Token()
        token2.CopyFrom(self.token)
        data = Any()
        if payload is not None:
            data.Pack(payload)

        if payload:
            return Request(
                token = token2,
                data = data
            )
        else:
            return Request(
                token = token2
            )

    def __handle_grpc_error(self, error, command):
        from drunc.utils.grpc_utils import rethrow_if_unreachable_server#, interrupt_if_unreachable_server
        rethrow_if_unreachable_server(error)
        # else:
        #     text = interrupt_if_unreachable_server(error)
        #     if text:
        #         self._log.error(text)

        # if hasattr(error, 'details'): #ARGG asyncio gRPC so different from synchronous one!!
        #     self._log.error(error.details())

        #     # of course, right now asyncio servers are not able to reply with a stacktrace (yet)
        #     # we just throw the client-side error and call it a day for now
        #     if rethrow:
        #         raise error

    def handle_response(self, response, command, outformat):
        from druncschema.request_response_pb2 import ResponseFlag, Response
        from drunc.utils.grpc_utils import unpack_any
        dr = DecodedResponse(
            name = response.name,
            token = response.token,
            flag = response.flag,
        )
        if response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            if response.data not in [None, ""]:
                dr.data = unpack_any(response.data, outformat)

            from drunc.exceptions import DruncServerSideError
            for c_response in response.children:
                try:
                    dr.children.append(self.handle_response(c_response, command, outformat))
                except DruncServerSideError as e:
                    self._log.error(f"Exception thrown from child: {e}")
            return dr

        else:
            def text(verb="not executed"):
                return f'Command \'{command}\' {verb} on \'{response.name}\' (response flag \'{ResponseFlag.Name(response.flag)}\')'
            if response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
            ]:
                self._log.debug(text())
            elif response.flag in [
                ResponseFlag.NOT_EXECUTED_NOT_IN_CONTROL,
            ]:
                self._log.warn(text())
            else:
                self._log.error(text("failed"))

            if not response.HasField("data"): return None
            from druncschema.generic_pb2 import Stacktrace, PlainText, PlainTextVector
            from drunc.utils.grpc_utils import unpack_any

            error_txt = ''
            stack_txt = None

            if response.data.Is(Stacktrace.DESCRIPTOR):
                stack = unpack_any(response.data, Stacktrace)
                #stack_txt = 'Stacktrace [bold red]on remote server![/bold red]\n' # Temporary - bold doesn't work
                stack_txt = 'Stacktrace on remote server!\n'
                last_one = ""
                for l in stack.text:
                    stack_txt += l+"\n"
                    if l != "":
                        last_one = l
                error_txt = last_one

            elif response.data.Is(PlainText.DESCRIPTOR):
                txt = unpack_any(response.data, PlainText)
                error_txt = txt.text

            # if rethrow:
            #     from drunc.exceptions import DruncServerSideError
            #     raise DruncServerSideError(error_txt, stack_txt)


            dr.data = response.data
            from drunc.exceptions import DruncServerSideError
            for c_response in response.children:
                try:
                    dr.children.append(self.handle_response(c_response, command, outformat))
                except DruncServerSideError as e:
                    self._log.error(f"Exception thrown from child: {e}")
            return dr

            # raise DruncServerSideError(error_txt, stack_txt, server_response=dr)


    def send_command(self, command:str, data=None, outformat=None, decode_children=False):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = cmd(request)
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, command)
        return self.handle_response(response, command, outformat)


    async def send_command_aio(self, command:str, data=None, outformat=None):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = await cmd(request)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command)
        return self.handle_response(response, command, outformat)


    async def send_command_for_aio(self, command:str, data=None, outformat=None):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            async for s in cmd(request):
                yield self.handle_response(s, command, outformat)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command)



class ShellContext:
    def _reset(self, name:str, token_args:dict={}, driver_args:dict={}):
        from rich.console import Console
        self._console = Console()
        from logging import getLogger
        self._log = getLogger(name)
        self._token = self.create_token(**token_args)
        self._drivers: Mapping[str, GRPCDriver] = self.create_drivers(**driver_args)

    def __init__(self, *args, **kwargs):
        try:
            self.reset(*args, **kwargs)
        except Exception as e:
            from drunc.utils.utils import print_traceback
            print_traceback()
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

    def set_driver(self, name:str, driver:GRPCDriver) -> None:
        if name in self._drivers:
            raise DruncShellException(f"Driver {name} already present in this context")
        self._drivers[name] = driver

    def get_driver(self, name:str=None) -> GRPCDriver:
        try:
            if name:
                return self._drivers[name]
            elif len(self._drivers)>1:
                raise DruncShellException(f'More than one driver in this context')
            return list(self._drivers.values())[0]
        except KeyError:
            self._log.error(f'FSM Commands cannot be sent until the Session is booted')
            raise SystemExit(1) # used to avoid having to catch multiple Attribute errors when this function gets called

    def get_token(self) -> Token:
        return self._token

    def print(self, *args, **kwargs) -> None:
        self._console.print(*args, **kwargs)

    def rule(self, *args, **kwargs) -> None:
        self._console.rule(*args, **kwargs)

    def info(self, *args, **kwargs) -> None:
        self._log.info(*args, **kwargs)

    def warn(self, *args, **kwargs) -> None:
        self._log.warn(*args, **kwargs)

    def error(self, *args, **kwargs) -> None:
        self._log.error(*args, **kwargs)

    def debug(self, *args, **kwargs) -> None:
        self._log.debug(*args, **kwargs)

    def critical(self, *args, **kwargs) -> None:
        self._log.critical(*args, **kwargs)


    def print_status_summary(self) -> None:
        status = self.get_driver('controller').get_status().data.state
        available_actions = [command.name.replace("_", "-") for command in self.get_driver('controller').describe_fsm().data.commands]
        if status.find('(') == -1:
            self.print(f"Current FSM status is [green]{status}[/green]. Available transitions are [green]{'[/green], [green]'.join(available_actions)}[/green].")
        else:
            self.print(f"[red] FSM is in error ({status})[/red], not currently accepting new commands.")
        return


def create_dummy_token_from_uname() -> Token:
    from drunc.utils.shell_utils import create_dummy_token_from_uname
    import getpass
    user = getpass.getuser()

    from druncschema.token_pb2 import Token
    return Token ( # fake token, but should be figured out from the environment/authoriser
        token = f'{user}-token',
        user_name = user
    )


def add_traceback_flag():
    def wrapper(f0):
        import click
        f1 = click.option('-t/-nt','--traceback/--no-traceback', default=None, help='Print full exception traceback')(f0)
        return f1
    return wrapper
