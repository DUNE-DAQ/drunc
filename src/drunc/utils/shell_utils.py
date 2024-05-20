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
    token = None
    data = None
    response_flag = None
    response_children = {}

    def __init__(self, token, response_flag, data=None, response_children={}):
        self.token = token
        self.response_flag = response_flag
        self.data = data
        self.response_children = response_children

class GRPCDriver:
    def __init__(self, name:str, address:str, token:Token, aio_channel=False, rethrow_by_default=False):
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
        self.rethrow_by_default = rethrow_by_default

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

    def __handle_grpc_error(self, error, command, rethrow):
        if rethrow is None:
            rethrow = self.rethrow_by_default

        from drunc.utils.grpc_utils import rethrow_if_unreachable_server, interrupt_if_unreachable_server
        if rethrow:
            rethrow_if_unreachable_server(error)
        else:
            text = interrupt_if_unreachable_server(error)
            if text:
                self._log.error(text)

        # from grpc_status import rpc_status
        # status = rpc_status.from_call(error)

        # self._log.error(f'Error sending command "{command}" to stub')

        # from druncschema.generic_pb2 import Stacktrace, PlainText
        # from drunc.utils.grpc_utils import unpack_any

        # if hasattr(status, 'details'):
        #     for detail in status.details:
        #         if detail.Is(Stacktrace.DESCRIPTOR):
        #             stack = unpack_any(detail, Stacktrace)
        #             text = ''
        #             if rethrow:
        #                 text += 'Stacktrace [bold red]on remote server![/]\n'
        #                 for l in stack.text:
        #                     text += l+"\n"
        #             else:
        #                 text += 'Error [bold red]on remote server![/]\n'+'\n'.join(stack.text[:-2])
        #             self._log.error(text, extra={"markup": True})
        #             return
        #         elif detail.Is(PlainText.DESCRIPTOR):
        #             txt = unpack_any(detail, PlainText)
        #             self._log.error(txt)

        if hasattr(error, 'details'): #ARGG asyncio gRPC so different from synchronous one!!
            self._log.error(error.details())

            # of course, right now asyncio servers are not able to reply with a stacktrace (yet)
            # we just throw the client-side error and call it a day for now
            if rethrow:
                raise error

    def handle_response(self, response, outformat):
        from druncschema.request_response_pb2 import ResponseFlag, Response
        from drunc.utils.grpc_utils import unpack_any
        dr = DecodedResponse(
            token = response.token,
            response_flag = response.response_flag,
        )
        if response.response_flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            dr.data = unpack_any(response.data, outformat)

            for child, c_response in response.response_children.items():
                dr.response_children[child] = self.handle_response(c_response, outformat)

            return dr

        else:
            self._log.error(f'Command {command} failed with response flag {str(response.response_flag)}')
            if not response.HasField("data"): return None
            from druncschema.generic_pb2 import Stacktrace, PlainText, PlainTextVector
            from drunc.utils.grpc_utils import unpack_any

            if response.data.Is(Stacktrace.DESCRIPTOR):
                stack = unpack_any(response.data, Stacktrace)
                txt = 'Stacktrace [bold red]on remote server![/]\n'
                for l in stack.text:
                    txt += l+"\n"
                self._log.error(txt, extra={"markup": True})

            elif response.data.Is(PlainText.DESCRIPTOR):
                txt = unpack_any(response.data, PlainText)
                self._log.error(txt.text)

            elif response.data.Is(PlainTextVector.DESCRIPTOR):
                txt = unpack_any(response.data, PlainTextVector)
                for t in txt.text:
                    self._log.error(t)


    def send_command(self, command:str, data=None, rethrow=None, outformat=None, decode_children=False):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = cmd(request)
        except grpc.RpcError as e:
            self.__handle_grpc_error(e, command, rethrow = rethrow)

        return self.handle_response(response, outformat)


    async def send_command_aio(self, command:str, data=None, rethrow=None, outformat=None):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            response = await cmd(request)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command, rethrow = rethrow)
        return self.handle_response(response, outformat)


    async def send_command_for_aio(self, command:str, data=None, rethrow=None, outformat=None):
        import grpc
        if not self.stub:
            raise DruncShellException('No stub initialised')

        cmd = getattr(self.stub, command) # this throws if the command doesn't exist

        request = self._create_request(data)

        try:
            async for s in cmd(request):
                yield self.handle_response(s, outformat)

        except grpc.aio.AioRpcError as e:
            self.__handle_grpc_error(e, command, rethrow = rethrow)



class ShellContext:
    def _reset(self, name:str, print_traceback:bool=False, token_args:dict={}, driver_args:dict={}):
        self.print_traceback = print_traceback
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
        if name:
            return self._drivers[name]
        elif len(self._drivers)>1:
            raise DruncShellException(f'More than one driver in this context')
        return list(self._drivers.values())[0]

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
