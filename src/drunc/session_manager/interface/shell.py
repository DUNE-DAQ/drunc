import os

import click
import click_shell
from daqpytools.logging import logging_log_levels

from drunc.session_manager.interface.commands import (
    describe,
    list_all_configs,
    list_all_sessions,
)
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    get_logger,
    get_root_logger,
)


@click_shell.shell(
    prompt="drunc-session-manager > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~/.drunc-sm-shell.history"),
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level (default is 'INFO').",
)
@click.argument("session-manager-address", type=str)
@click.pass_context
def session_manager_shell(ctx, session_manager_address: str, log_level: str) -> None:
    get_root_logger(log_level)
    log = get_logger("session_manager.shell", rich_handler=True)

    def cleanup() -> None:
        log.info("Exiting session manager shell")

    ctx.obj.reset(address=session_manager_address)
    ctx.command.add_command(describe, "describe")
    ctx.command.add_command(list_all_sessions, "list_all_sessions")
    ctx.command.add_command(list_all_configs, "list_all_configs")
    ctx.call_on_close(cleanup)

    log.info("Starting session manager shell")
