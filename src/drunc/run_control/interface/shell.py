import getpass
import os

import click
import click_shell
from daqpytools.logging import logging_log_levels

from drunc.run_control.interface.commands import (
    end_session,
    logs,
    start_session,
    validate_session,
)
from drunc.run_control.interface.commands import log_on_server as log
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    format_name_for_cli,
    get_logger,
    get_root_logger,
)


@click_shell.shell(
    prompt="dune-run-control > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~") + "/.dune-run-control-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.option(
    "-a",
    "--run-control-address",
    type=str,
    default="localhost:50051",
    help="Specicy the address of the run control server to connect to",
)
@click.pass_context
def run_control_shell(
    ctx: click.core.Context, run_control_address: str, log_level: str
) -> None:
    get_root_logger("INFO")
    rc_log = get_logger(
        logger_name="run_control.iface",
        rich_handler=True,
    )
    rc_log.setLevel(logging_log_levels[log_level.upper()])
    rc_log.info("Initializing the run control shell")
    ctx.obj.reset(address=run_control_address)

    try:
        ctx.obj.get_driver(
            "run_control"
        ).validate_communication()  # TODO- assign the outcome and validate it
    except ServerUnreachable as e:
        rc_log.critical("Could not connect to the process manager")
        rc_log.exception(
            e
        )  # TODO: Keep this for dev branch, remove it for production branch
        exit(1)

    ctx.obj.get_driver("run_control").log_on_server(
        f"{getpass.getuser()} connected from {ctx.obj.shell_id}"
    )

    rc_log.info(
        f"[green]{getpass.getuser()}[/green] connected to the run control through a [green]dune-run-control-shell[/green] via address [green]{run_control_address}[/green]"
    )

    def cleanup():
        ctx.obj.get_driver("run_control").log_on_server(
            f"{getpass.getuser()} disconnecting from {ctx.obj.shell_id}"
        )
        ctx.obj.terminate()
        rc_log.info(
            f"[green]{getpass.getuser()}[/green] disconnected from the run control"
        )

    ctx.call_on_close(cleanup)

    # Add commands to the shell
    run_control_commands = [validate_session, start_session, end_session, log, logs]
    for cmd in run_control_commands:
        ctx.command.add_command(cmd, format_name_for_cli(cmd.name))

    rc_log.info("Ready")
