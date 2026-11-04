import getpass
from functools import update_wrapper
from typing import cast

import click
from click.decorators import FC
from druncschema.common_pb2 import LoggerTarget
from druncschema.process_manager_pb2 import (
    ProcessQuery,
)
from druncschema.request_response_pb2 import ResponseFlag
from rich.table import Table

from drunc.process_manager.interface.cli_argument import add_query_options_no_session
from drunc.process_manager.interface.commands import (
    ps_decorators,
)
from drunc.process_manager.utils import print_ps_table
from drunc.run_control.interface.context import RunControlContext
from drunc.utils.shell_utils import (
    InterruptedCommand,
    log_rc_cmd,
)
from drunc.utils.utils import get_logger, log_echo


def session_empty(f: FC) -> FC:
    """
    Add later
    """

    @click.pass_context
    def wrapper(ctx: click.core.Context, *args: object, **kwargs: object) -> object:
        """
        Add later
        """
        kwargs["session"] = None
        return ctx.invoke(f, *args, **kwargs)

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
    log = get_logger("run_control.shell")
    log.warning(f"Connecting to pm at {address}")
    obj.get_run_control_driver().connect_to_pm(address=address)


@click.command("ps")
@session_empty  # injected at the run control server
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

    results = obj.get_run_control_driver().ps(query)
    log.info("received results")

    print_ps_table(query, results, obj, log, long_format, width, fit)


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


@click.command("validate")
@click.pass_obj
def validate(obj: RunControlContext) -> None:
    """Set the server's session configuration valid flag to true."""
    log_rc_cmd(obj)
    response = obj.get_run_control_driver().validate()
    if response.flag != ResponseFlag.EXECUTED_SUCCESSFULLY:
        raise click.ClickException(
            f"Setting valid flag failed: {ResponseFlag.Name(response.flag)}"
        )

    obj.print("Session configuration valid flag set to True.")


@click.command("boot")
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    default=None,
    help="Manual override allows for overwriting logs or not, by appending timestamp info. Default (None) is to follow what is used in the initialisation of the unified shell.",
)
@click.option(
    "-cl",
    "--controller-log-level",
    default=None,
    help="Overrides the config-defined log level of the controller",
)
@click.option(
    "--sleep-between-app-boot",
    type=float,
    default=0.1,
    help="Sleep between app boot, in seconds. This may be useful if you have are using SSHPM, and have SSHD's maxstartups setting set to a low value.",
)
@click.pass_obj
def boot(
    obj: RunControlContext,
    override_logs: bool | None,
    controller_log_level: str | None,
    sleep_between_app_boot: float = 0,
) -> None:
    log_rc_cmd(obj)
    log = get_logger("run_control.shell")
    user = getpass.getuser()

    if override_logs is None:
        override_logs_boot = obj.override_logs
    else:
        override_logs_boot = override_logs

    # needs error handling
    try:
        obj.get_run_control_driver().boot(
            log_level=controller_log_level,
            user=user,
            override_logs=override_logs_boot,
            sleep_between_app_boot=sleep_between_app_boot,
        )
    except InterruptedCommand:
        return
    except Exception as e:
        log.exception(e)
        raise e

    return
