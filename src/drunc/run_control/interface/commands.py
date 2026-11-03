import click
from druncschema.common_pb2 import LoggerTarget
from functools import update_wrapper
from drunc.run_control.interface.context import RunControlContext
from drunc.utils.utils import get_logger, log_echo

from typing import cast

from click.decorators import FC

from drunc.process_manager.interface.cli_argument import add_query_options_no_session

from druncschema.process_manager_pb2 import (
    ProcessInstance,
    ProcessQuery,
)

from druncschema.request_response_pb2 import ResponseFlag
from rich.table import Table

from drunc.utils.shell_utils import log_rc_cmd

from rich.markup import escape

from drunc.process_manager.interface.commands import (
    flush_decorators,
    flush_impl,
    kill_decorators,
    kill_impl,
    logs_decorators,
    logs_impl,
    ps_decorators,
    ps_impl,
    restart_impl,
)

from drunc.utils.shell_utils import InterruptedCommand, format_table_width, log_pm_cmd

from drunc.process_manager.utils import tabulate_process_instance_list


def session_empty(f: FC) -> FC:
    """
    Decorator to inject the session name into the command function.

    This is used to wrap the relevant unified shell commands, as the unified shell is
    intended to only operate a single session at a time, and the session name is stored
    in the context object.

    Args:
        f (FC): The command function to wrap.

    Returns:
        FC: The wrapped command function with the session name injected.

    Raises:
        None
    """

    @click.pass_context
    def wrapper(ctx: click.core.Context, *args: object, **kwargs: object) -> object:
        """
        Wrapper function to inject the session name into the command function.

        Args:
            ctx (click.core.Context): The click context object.
            *args (object): Positional arguments to pass to the command function.
            **kwargs (object): Keyword arguments to pass to the command function.

        Returns:
            object: The result of invoking the command function with the session name
                injected.

        Raises:
            None
        """
        kwargs["session"] = None
        return ctx.invoke(f, *args, **kwargs)

    # The update_wrapper function is used to update the internal python methods for the
    # functions so that the click decorators can be used with the session_injector
    # decorator. This is necessary because the click decorators rely on the function
    # signature to determine the parameters that are passed to the function
    return cast(FC, update_wrapper(wrapper, f))


@click.command("echo")
@click.argument("text", required=True)
@click.option(
    "--target-server",
    type=str,
    default="",
    help="Server to use the log command on. Default value of '' will send the log message to all the servers, e.g. the process manager and the root controller.",
)
@click.option(
    "--server/--local",
    default=False,
    help="Send the message to the server via RPC (default: log locally only).",
)
@click.option(
    "--logger",
    type=click.Choice(["echo", "main"]),
    default="echo",
    callback=lambda context, parameter, value: LoggerTarget.Value(value.upper()),
    help="Which server-side logger to target when --server is used.",
)
@click.option(
    "-s",
    "--severity",
    type=str,
    default="INFO",
    help=(
        "Severity level of the log message (default INFO). Options: DEBUG, INFO, "
        "WARNING, ERROR, CRITICAL"
    ),
)
@click.option("--target", type=str, help="The session target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the session application path",
    default=False,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the session application path",
    default=True,
)
@click.pass_obj
def echo(
    obj: RunControlContext,
    text: str,
    server: bool,
    target_server: str,
    logger: int,
    target: str,
    severity: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    """
    Log a message locally or send it to selected servers.

    Without server mode, the message is logged locally. In server mode, it can be sent
    to the process manager, controller, or both.

    Args:
        obj: The unified shell context.
        text: The log message text.
        target_server: The server to target, or all servers when empty.
        logger: The selected logger target.
        severity: The log severity level.
    """
    log = get_logger("run_control.echo")
    log.debug("Logging message to server(s)...")

    if not server:
        (log_echo if logger == LoggerTarget.ECHO else log).info(text)
        return

    obj.get_run_control_driver().send_log(text=text, severity=severity, logger=logger)

    #! to be added later!
    # if target_server in ["", "process_manager"]:
    #     obj.get_pm_driver().send_log(text=text, severity=severity, logger=logger)

    # if target_server in ["", "controller"] and obj.has_driver("controller"):
    #     obj.get_controller_driver().send_log(
    #         text=text,
    #         severity=severity,
    #         target=target,
    #         logger=logger,
    #         execute_along_path=execute_along_path,
    #         execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    #     )


@click.command("connect-to-pm")
@click.argument("address", required=True)
@click.pass_obj
def connect_to_pm(obj: RunControlContext, address: str):
    # switch log_echo to real log
    log_echo.warning(f"trying to connect to pm at {address}")

    obj.get_run_control_driver().connect_to_pm(address=address)


@click.command("ps")
@session_empty
@add_query_options_no_session(at_least_one=False, all_processes_by_default=True)
@ps_decorators
@click.pass_obj
def ps(
    obj: RunControlContext,
    query: ProcessQuery,
    long_format: bool,
    width: int | None,
    fit: bool,
) -> None:

    log_rc_cmd(obj)
    log = get_logger("run_control.shell")    


    log.info(f"Running ps with query {query}")


    # session is empty since its gonna be injected later on


    results = obj.get_run_control_driver().ps(query)

    log.info("received results")


    # Copy pasted from ps_impl!
    session_name = (
    getattr(query, "session", None) or getattr(obj, "session_name", None) or ""
    )
    title = f"Processes running{f' in session {session_name}' if session_name else ''}"
    # If there are processes running, tabulate them, otherwise log that there are no
    # processes running.
    if results.values:
        table = tabulate_process_instance_list(
            results,
            title=title,
            long=long_format,
            width=width,
        )

        table = format_table_width(obj, table, fit)
        obj.print(table, soft_wrap=not fit)
    else:
        if session_name:
            log.info(
                f"No processes running in session [green]{escape(session_name)}[/green]"
            )
        else:
            log.info("No processes running")


@click.command("set-session-config")
@click.option("--configuration-file", default="", help="Configuration file.")
@click.option("--configuration-id", default="", help="Configuration ID.")
@click.option("--session-name", default="", help="Session name.")
@click.pass_obj
def set_session_config(
    obj: RunControlContext,
    configuration_file: str,
    configuration_id: str,
    session_name: str,
) -> None:
    """Update session settings; empty values leave existing settings unchanged."""
    if not any((configuration_file, configuration_id, session_name)):
        raise click.UsageError("Provide at least one configuration option.")

    log_rc_cmd(obj)
    response = obj.get_run_control_driver().set_session_config(
        configuration_file=configuration_file,
        configuration_id=configuration_id,
        session_name=session_name,
    )
    if response.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
        raise click.ClickException(
            f"Configuration update failed: {ResponseFlag.Name(response.flag)}"
        )

    obj.print("Session configuration updated.")


@click.command("print-session-config")
@click.pass_obj
def print_session_config(obj: RunControlContext) -> None:
    """Display the server's current session configuration."""
    log_rc_cmd(obj)
    config = obj.get_run_control_driver().print_session_config()

    table = Table(title="Session configuration")
    table.add_column("Setting")
    table.add_column("Value")
    table.add_row("Configuration file", config.configuration_file)
    table.add_row("Configuration ID", config.configuration_id)
    table.add_row("Session name", config.session_name)
    table.add_row("Valid", str(config.valid))
    obj.print(table)


@click.command("valid")
@click.pass_obj
def valid(obj: RunControlContext) -> None:
    """Set the server's session configuration valid flag to true."""
    log_rc_cmd(obj)
    response = obj.get_run_control_driver().valid()
    if response.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
        raise click.ClickException(
            f"Setting valid flag failed: {ResponseFlag.Name(response.flag)}"
        )

    obj.print("Session configuration valid flag set to True.")
