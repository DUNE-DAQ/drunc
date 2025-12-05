import abc
import getpass
import socket
from collections.abc import Mapping

import click
from druncschema.token_pb2 import Token
from rich.console import Console

from drunc.exceptions import DruncShellException
from drunc.utils.utils import get_logger


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


def is_port_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """
    Checks if the requested port on the specified host is available. This allows us to
    validate that a specified configuration with a static address is available.

    Args:
        host - what hostname to check on
        port - the port number to check

    Returns:
        bool - true if available, false otherwise

    Raises:
        ValueError - if the hostnmame cannot be resolved to an IP address
    """

    # Address the localhost case separately
    if host in ["localhost", "127.0.0.1", "0.0.0.0"]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Set SO_REUSEADDR to allow fast rebinds after a crash
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                # Try to bind to the port on all interfaces
                s.bind(("0.0.0.0", port))
                return True
            except OSError as e:
                # If bind fails the port is NOT available
                if "Address already in use" in str(e):
                    return False
                # Handle other OS errors differently if needed
                raise e

    # Address the remote host case separately
    # Map the hostname to an ip address
    try:
        ip_address = socket.gethostbyname(host)
    except socket.gaierror:
        raise ValueError(f"Could not resolve hostname: {host}")

    # Attempt to create a connection to the address specified by the host and port
    # If the connection succeeds, something is listening at the specified address, i.e.
    # something is already listening
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            # Attempt to connect; if successful, the port is listening (in use)
            s.connect((ip_address, port))
            return False
        except (TimeoutError, OSError):
            # If the connection is refused, times out, or other OS error occurs, it's
            # not listening (available)
            print("Connection failed")
            return True


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
        text = (
            f"{prefix} {obj.name} -> response flag={obj.flag} type={type(obj.data)}\n"
        )
        for v in obj.children:
            if v is None:
                continue
            text += DecodedResponse.str(v, prefix + "  ")
        return text

    def __str__(self):
        return DecodedResponse.str(self)


class ShellContext:
    def _reset(self, name: str, token_args: dict = {}, driver_args: dict = {}):
        self._console = Console()
        self._token = self.create_token(**token_args)
        self._drivers: Mapping[str, object] = self.create_drivers(**driver_args)

    def __init__(self, *args, **kwargs):
        log = get_logger("utils.ShellContext")
        self.dynamic_commands = set()
        try:
            self.reset(*args, **kwargs)
        except Exception as e:
            log.exception(e)
            exit(1)

    @abc.abstractmethod
    def reset(self, **kwargs):
        pass

    @abc.abstractmethod
    def create_drivers(self, **kwargs) -> Mapping[str, object]:
        pass

    @abc.abstractmethod
    def create_token(self, **kwargs) -> Token:
        pass

    @abc.abstractmethod
    def terminate(self) -> None:
        pass

    def set_driver(self, name: str, driver: object) -> None:
        if name in self._drivers:
            raise DruncShellException(f"Driver {name} already present in this context")
        self._drivers[name] = driver

    def get_driver(self, name: str = None, quiet_fail: bool = False) -> object:
        try:
            if name:
                return self._drivers[name]
            elif len(self._drivers) > 1:
                raise DruncShellException("More than one driver in this context")
            return list(self._drivers.values())[0]
        except KeyError:
            if quiet_fail:
                return None
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
            log.info(f"You will not be able to issue commands to the {name} anymore.")
            del self._drivers[name]
            log.info(f"{name.capitalize()} driver has been deleted.")

    def get_token(self) -> Token:
        return self._token

    def print(self, *args, **kwargs) -> None:
        self._console.print(*args, **kwargs)  # rich tables require console printing

    def rule(self, *args, **kwargs) -> None:
        self._console.rule(*args, **kwargs)

    def print_status_summary(self) -> None:
        log = get_logger("utils.ShellContext")
        status = self.get_driver("controller").status().status
        describe_fsm = self.get_driver("controller").describe_fsm().description
        current_state = status.state
        if status.in_error:
            log.error(
                f"[red] FSM is in error ({status})[/red], not currently accepting new commands."
            )
        else:
            available_actions = [
                command.name.replace("_", "-") for command in describe_fsm.commands
            ]
            available_sequences = [
                seq.id.replace("_", "-") for seq in describe_fsm.sequences
            ]

            log.info(
                f"Current FSM status is [green]{current_state}[/green]. Available transitions are [green]{'[/green], [green]'.join(available_actions)}[/green]. Available sequence commands are [green]{'[/green], [green]'.join(available_sequences)}[/green]."
            )
