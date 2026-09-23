import click
from druncschema.common_pb2 import LoggerTarget

from drunc.run_control.interface.context import RunControlContext
from drunc.utils.utils import get_logger, log_echo


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

    obj.get_driver("run_control").send_log(text=text, severity=severity, logger=logger)

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
