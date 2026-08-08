import getpass
import os
from typing import cast

import click
import click_shell
from daqpytools.logging import HandlerType, add_handler, logging_log_levels

from drunc.process_manager.interface.commands import (
    boot,
    dummy_boot,
    echo,
    flush,
    kill,
    log_on_server,
    logs,
    ps,
    restart,
    terminate,
    wait,
)
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    format_name_for_cli,
    get_logger,
    get_root_logger,
    validate_command_facility,
)


@click_shell.shell(
    prompt="drunc-process-manager > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~") + "/.drunc-pm-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.argument("process-manager-address", type=str, callback=validate_command_facility)
@click.pass_context
def process_manager_shell(
    ctx: click.core.Context, process_manager_address: str, log_level: str
) -> None:
    """
    Shell interface for the process manager.

    This shell allows users to interact with the process manager through a command-line
    interface.

    Additional commands can be added to this shell by defining new functions and
    registering them with the shell.

    Args:
        ctx: The Click context object.
        process_manager_address (str): The address of the process manager to connect to.
        log_level (str): The log level for logging messages.

    Returns:
        None

    Raises:
        ServerUnreachable: If the process manager is unreachable at the specified
            address.
    """
    get_root_logger(log_level)
    process_manager_log = get_logger(
        logger_name="process_manager",
        rich_handler=True,
    )
    process_manager_shell_log = get_logger("process_manager.shell")

    ctx.obj.reset(address=process_manager_address)

    try:
        desc = ctx.obj.get_driver("process_manager").describe()
    except ServerUnreachable as e:
        process_manager_shell_log.critical("Could not connect to the process manager")
        process_manager_shell_log.exception(
            e
        )  # TODO: Keep this for dev branch, remove it for production branch
        # process_manager_shell_log.error(e.message) # TODO: Keep this for production branch, remove this from dev branch
        exit(1)

    ctx.obj.get_driver("process_manager").log_on_server(
        f"{getpass.getuser()} connected from {ctx.obj.shell_id}"
    )

    # Manually add file handler to process manager log
    # Not possible to initialise logger immediately as it requires
    # knowledge of the log path
    if desc.info:
        add_handler(process_manager_log, HandlerType.File, True, path=desc.info)

    process_manager_log.info(
        f"[green]{getpass.getuser()}[/green] connected to the process manager through a [green]drunc-process-manager-shell[/green] via address [green]{process_manager_address}[/green]"
    )
    process_manager_shell_log.info(
        f"Connected to {process_manager_address}, running '{desc.name}.{desc.session}' (name.session), starting listening..."
    )

    def cleanup() -> None:
        """
        Cleanup function to be called when the shell is closed.
        """
        ctx.obj.get_driver("process_manager").log_on_server(
            f"{getpass.getuser()} disconnecting from {ctx.obj.shell_id}"
        )
        ctx.obj.terminate()
        process_manager_log.info(
            f"[green]{getpass.getuser()}[/green] disconnected from the process manager through a [green]drunc-process-manager-shell[/green]"
        )

    ctx.call_on_close(cleanup)

    # Register all the click commands to the shell
    ## List of commands to be exposed in the shell
    exposed_process_manager_commands = [
        boot,
        wait,
        terminate,
        kill,
        flush,
        logs,
        log_on_server,
        restart,
        echo,
        ps,
        dummy_boot,
    ]

    ## Cast the command group to click.core.Group to avoid type errors
    command_group = cast(click.core.Group, ctx.command)

    ## Add each command to the shell's command group with a formatted name
    for cmd in exposed_process_manager_commands:
        command_group.add_command(cmd, format_name_for_cli(cmd.name or ""))

    process_manager_shell_log.info("Ready")
