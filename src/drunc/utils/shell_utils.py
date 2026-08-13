"""Shell utilities for DRUNC."""

import abc
import getpass
from collections.abc import MutableMapping
from typing import (
    TYPE_CHECKING,
    Callable,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

import click
from druncschema.token_pb2 import Token
from rich.console import Console

from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncShellException
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.utils import get_logger

if TYPE_CHECKING:
    pass


class CommandLike(Protocol):
    """Protocol for command-like objects."""

    name: str


class SequenceLike(Protocol):
    """Protocol for sequence-like objects."""

    id: str


class FSMDescriptionLike(Protocol):
    """Protocol for FSM description-like objects."""

    commands: list[CommandLike]
    sequences: list[SequenceLike]


class DescribeFSMReplyLike(Protocol):
    """Protocol for describe FSM reply-like objects."""

    description: FSMDescriptionLike


class StatusLike(Protocol):
    """Protocol for status-like objects."""

    state: str
    in_error: bool


class StatusReplyLike(Protocol):
    """Protocol for status reply-like objects."""

    status: StatusLike


class ControllerDriverProtocol(Protocol):
    """Protocol for controller driver objects."""

    def status(self) -> StatusReplyLike:
        """Get the current status.

        Returns:
            StatusReplyLike: The current status.
        """
        ...

    def describe_fsm(self) -> DescribeFSMReplyLike:
        """Describe the FSM.

        Returns:
            DescribeFSMReplyLike: The FSM description.
        """
        ...


class ProcessManagerLogProtocol(Protocol):
    """Protocol for process-manager logging used by shell command logging."""

    def log_on_server(self, message: str) -> None:
        """Send a log message to the process-manager server."""
        ...


P = ParamSpec("P")
R = TypeVar("R")


class InterruptedCommand(DruncShellException):
    """Exception thrown to interrupt a shell command without a full stack trace."""

    pass


def create_dummy_token_from_uname() -> Token:
    """Create a dummy token from the current username.

    Returns:
        Token: A dummy token with the current username.
    """
    user = getpass.getuser()
    return (
        Token(  # fake token, but should be figured out from the environment/authoriser
            token=f"{user}-token", user_name=user
        )
    )


def add_traceback_flag() -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Add a traceback flag to a command.

    Returns:
        Callable: A decorator that adds the traceback flag.
    """

    def wrapper(f0: Callable[P, R]) -> Callable[P, R]:
        f1 = click.option(
            "-t/-nt",
            "--traceback/--no-traceback",
            default=None,
            help="Print full exception traceback",
        )(f0)
        return f1

    return wrapper


class DecodedResponse:
    """Decoded response object.

    Warning: This should be kept in sync with
    druncschema/request_response.proto/Response class
    """

    name = None
    token = None
    data = None
    flag = None
    children: list["DecodedResponse"] = []

    def __init__(
        self,
        name: str,
        token: Token,
        flag: object,
        data: object | None = None,
        children: list["DecodedResponse"] | None = None,
    ) -> None:
        """Initialize a DecodedResponse.

        Args:
            name: The name of the response.
            token: The token associated with the response.
            flag: The response flag.
            data: The response data. Defaults to None.
            children: Child responses. Defaults to None.
        """
        self.name = name
        self.token = token
        self.flag = flag
        self.data = data
        if children is None:
            self.children = []
        else:
            self.children = children

    @staticmethod
    def to_string(obj: "DecodedResponse", prefix: str = "") -> str:
        """Convert a DecodedResponse to a string representation.

        Args:
            obj: The DecodedResponse to convert.
            prefix: A prefix to add to the string. Defaults to empty string.

        Returns:
            str: The string representation of the response.
        """
        text = (
            f"{prefix} {obj.name} -> response flag={obj.flag} type={type(obj.data)}\n"
        )
        for v in obj.children:
            if v is None:
                continue
            text += DecodedResponse.to_string(v, prefix + "  ")
        return text

    def __str__(self) -> str:
        """Return string representation of the DecodedResponse.

        Returns:
            str: The string representation.
        """
        return DecodedResponse.to_string(self)


class ShellContext:
    """Base class for shell contexts."""

    shell_id: str | None = (
        None  # used for logging if its a PM shell or Unified shell etc
    )

    def get_shell_id(self) -> str | None:
        return self.shell_id

    def _reset(
        self,
        name: str,
        token_args: dict[str, object] = {},
        driver_args: dict[str, object] = {},
    ) -> None:
        self._console = Console()
        self._token = self.create_token(**token_args)
        self._drivers: MutableMapping[str, object] = self.create_drivers(**driver_args)

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize the shell context.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        log = get_logger("utils.ShellContext")
        self.dynamic_commands: set[str] = set()
        try:
            self.reset(*args, **kwargs)
        except Exception as e:
            log.exception(e)
            exit(1)

    @abc.abstractmethod
    def reset(self, *args: object, **kwargs: object) -> None:
        """Reset the shell context.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        pass

    @abc.abstractmethod
    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        """Create drivers for the context.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            MutableMapping[str, object]: A mapping of driver names to driver objects.
        """
        pass

    @abc.abstractmethod
    def create_token(self, **kwargs: object) -> Token:
        """Create a token for the context.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Token: A token object.
        """
        pass

    @abc.abstractmethod
    def terminate(self) -> None:
        """Terminate the shell context."""
        pass

    def set_driver(self, name: str, driver: object) -> None:
        """Set a driver in the context.

        Args:
            name: The name of the driver.
            driver: The driver object.

        Raises:
            DruncShellException: If a driver with the same name already exists.
        """
        if name in self._drivers:
            raise DruncShellException(f"Driver {name} already present in this context")
        self._drivers[name] = driver

    def get_driver(self, name: str | None = None, quiet_fail: bool = False) -> object:
        """Get a driver from the context.

        Args:
            name: The name of the driver. If None, returns the only driver if there is exactly one.
            quiet_fail: If True, return None on failure instead of raising an exception.

        Returns:
            object: The driver object, or None if quiet_fail is True and the driver is not found.

        Raises:
            DruncShellException: If there are multiple drivers and no name is specified.
            SystemExit: If the driver is not found and quiet_fail is False.
        """
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

    def get_pm_driver(self, quiet_fail: bool = False) -> ProcessManagerDriver:
        """
        Get the process manager driver from the context.

        Args:
            quiet_fail: If True, return None on failure instead of raising an exception.

        Returns:
            ProcessManagerDriver: The process manager driver.

        Raises:
            RuntimeError: If the process manager driver is not initialized.
        """
        pmd = self.get_driver("process_manager", quiet_fail=quiet_fail)

        if not pmd or isinstance(pmd, ProcessManagerDriver):
            raise RuntimeError("ProcessManagerDriver is not loaded!")

        return pmd

    def get_controller_driver(self, quiet_fail: bool = False) -> ControllerDriver:
        """
        Get the root controller driver from the context.

        Args:
            quiet_fail: If True, return None on failure instead of raising an exception.

        Returns:
            ControllerDriver: The process manager driver.

        Raises:
            RuntimeError: If the process manager driver is not initialized.
        """
        ctrld = self.get_driver("controller", quiet_fail=quiet_fail)

        if not ctrld or isinstance(ctrld, ControllerDriver):
            raise RuntimeError("ControllerDriver is not loaded!")

        return ctrld

    def has_driver(self, name: str) -> bool:
        """Check if a driver exists in the context.

        Args:
            name: The name of the driver.

        Returns:
            bool: True if the driver exists, False otherwise.
        """
        return name in self._drivers

    def delete_driver(self, name: str) -> None:
        """Delete a driver from the context.

        Args:
            name: The name of the driver to delete.
        """
        log = get_logger("utils.ShellContext")
        if name in self._drivers:
            log.info(f"You will not be able to issue commands to the {name} anymore.")
            del self._drivers[name]
            log.info(f"{name.capitalize()} driver has been deleted.")

    def get_token(self) -> Token:
        """Get the token from the context.

        Returns:
            Token: The token object.
        """
        return self._token

    def print(self, *args: object, **kwargs: object) -> None:
        """Print to the console.

        Args:
            *args: Positional arguments to pass to the console.
            **kwargs: Keyword arguments to pass to the console.
        """
        self._console.print(*args, **kwargs)  # type: ignore[arg-type]

    def rule(self, *args: object, **kwargs: object) -> None:
        """Print a rule to the console.

        Args:
            *args: Positional arguments to pass to the console.
            **kwargs: Keyword arguments to pass to the console.
        """
        self._console.rule(*args, **kwargs)  # type: ignore[arg-type]

    def print_status_summary(self) -> None:
        """Print a summary of the FSM status and available transitions."""
        log = get_logger("utils.ShellContext")
        controller = cast(ControllerDriverProtocol, self.get_driver("controller"))
        status = controller.status().status
        describe_fsm = controller.describe_fsm().description
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


def log_pm_cmd(obj: ShellContext) -> None:
    """Log a process-manager shell command with only explicitly provided arguments.

    The current Click command context is inspected and only parameters whose source is
    ``COMMANDLINE`` are included in the log message. This keeps defaulted values out
    of the message while still recording the command name, optional session name, and
    shell identity.

    These are sent over via  so that it can be displayed in the process manager
    shell

    Args:
        obj (ShellContext): Active shell context used to send the log message.
    """

    ctx_cmd = click.get_current_context(silent=True)
    cmd_name = ctx_cmd.command.name if ctx_cmd and ctx_cmd.command else None
    parms_dict: dict[str, str] = {}
    if ctx_cmd and ctx_cmd.command:
        for param in ctx_cmd.command.params:
            name = param.name
            if name is None:
                continue
            if (
                ctx_cmd.get_parameter_source(name)
                == click.core.ParameterSource.COMMANDLINE
            ):
                parms_dict[name] = f"{ctx_cmd.params[name]!r}"

    args = f" with arguments {parms_dict}" if parms_dict else ""
    session = f" for session {obj.session_name}" if hasattr(obj, "session_name") else ""
    msg = f"{getpass.getuser()} sent {cmd_name}{args}{session} via {obj.get_shell_id()}"
    pm_driver = cast(ProcessManagerLogProtocol, obj.get_driver("process_manager"))
    pm_driver.log_on_server(msg)
