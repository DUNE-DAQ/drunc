"""Shell utilities for DRUNC."""

import abc
import getpass
import logging
import os
import sys
from collections.abc import MutableMapping
from typing import Any

import click
from druncschema.token_pb2 import Token
from rich.console import Console

from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncShellException
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.utils import get_logger, resolve_localhost_to_hostname


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


class ShellContext:
    """Base class for shell contexts."""

    # used for logging if its a PM shell or Unified shell etc
    shell_id: str | None = None

    def get_shell_id(self) -> str | None:
        """
        Get the shell ID.

        This is primarily used by the superclasses of the ShellContext to identify the
        type of shell (e.g., PM shell, Unified shell) for logging purposes.

        Args:
            None

        Returns:
            str | None: The shell ID, or None if not set.

        Raises:
            None
        """
        return self.shell_id

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the shell context.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        log = get_logger("utils.ShellContext")
        self.session_name: str | None = None
        self.dynamic_commands: set[str] = set()
        try:
            self.reset(*args, **kwargs)
        except Exception as e:
            log.exception(e)
            exit(1)

    def reset(self, address: str = "") -> None:
        """Reset the shell context.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.address = resolve_localhost_to_hostname(address)
        self.console = Console()
        self._console = self.console
        self._token = create_dummy_token_from_uname()
        self._drivers: MutableMapping[str, object] = self.create_drivers()

    @abc.abstractmethod
    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        """Create drivers for the context.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            MutableMapping[str, object]: A mapping of driver names to driver objects.
        """
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
            log.critical(f"Drivers available are {self._drivers.keys()}")
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

        # Check the type for mypy and runtime safety.
        if not isinstance(pmd, ProcessManagerDriver):
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

        if not isinstance(ctrld, ControllerDriver):
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
        self.console.print(*args, **kwargs)  # type: ignore[arg-type]

    def rule(self, *args: object, **kwargs: object) -> None:
        """Print a rule to the console.

        Args:
            *args: Positional arguments to pass to the console.
            **kwargs: Keyword arguments to pass to the console.
        """
        self.console.rule(*args, **kwargs)  # type: ignore[arg-type]

    def print_status_summary(self) -> None:
        """Print a summary of the FSM status and available transitions."""
        log = get_logger("utils.ShellContext")
        controller = self.get_controller_driver()
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


def shell_cmd_message(
    session_name: str | None = None, shell_id: str | None = None
) -> str:
    """Create a log message for a shell command with only explicitly provided arguments.

    The current Click command context is inspected and only parameters whose source is
    ``COMMANDLINE`` are included in the log message. This keeps defaulted values out
    of the message while still recording the command name, optional session name, and
    shell identity.

    These are sent over via  so that it can be displayed in the process manager
    shell

    Args:
        session_name: optional name of the current session
        shell_id: optional name of the shell (e.g. process manager, run control, ...)
    """

    ctx_cmd = click.get_current_context(silent=True)
    cmd_name = ctx_cmd.command.name if ctx_cmd and ctx_cmd.command else None

    params_dict: dict[str, str] = {}
    if ctx_cmd and ctx_cmd.command:
        for param in ctx_cmd.command.params:
            name = param.name
            if name is None:
                continue
            if (
                ctx_cmd.get_parameter_source(name)
                == click.core.ParameterSource.COMMANDLINE
            ):
                params_dict[name] = f"{ctx_cmd.params[name]!r}"

    msg = f"{getpass.getuser()} sent {cmd_name}"
    if params_dict:
        msg += f" with arguments {params_dict}"
    if session_name is not None:
        msg += f" for session {session_name}"
    if shell_id is not None:
        msg += f" via {shell_id}"

    return msg


def log_pm_cmd(obj: ShellContext) -> None:
    msg = shell_cmd_message(session_name=obj.session_name, shell_id=obj.get_shell_id())
    obj.get_pm_driver().send_log(msg)


def check_and_cd_execution_path(log: logging.Logger, execution_path: str) -> None:
    """
    Check if the provided execution path is a writable directory and change the current
    working directory to it.

    Args:
        log: Logger object for logging errors.
        execution_path: The path to check and change to.

    Returns:
        None

    Raises:
        SystemExit: If the execution path is not a writable directory.
    """
    if execution_path:
        if not os.path.isdir(execution_path) or not os.access(execution_path, os.W_OK):
            log.error(
                f"[red]Execution path '{execution_path}' is not a writable directory[/red]"
            )
            sys.exit(1)
        log.info(f"[green]Changing execution path to '{execution_path}'[/green]")
        os.chdir(execution_path)
    else:
        log.debug("[green]No execution path provided, using the default path[/green]")
