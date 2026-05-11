import getpass
import os

import click
import click_shell
from daqpytools.logging import HandlerType, add_handler, logging_log_levels

from drunc.process_manager.interface.commands import (
    boot,
    dummy_boot,
    flush,
    kill,
    logs,
    ps,
    restart,
    terminate,
    wait,
)
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
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
def process_manager_shell(ctx, process_manager_address: str, log_level: str) -> None:
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
    if desc.HasField("broadcast"):
        ctx.obj.start_listening(desc.broadcast)

    def cleanup():
        ctx.obj.terminate()
        process_manager_log.warning(
            f"[green]{getpass.getuser()}[/green] disconnected from the process manager through a [green]drunc-process-manager-shell[/green]"
        )

    ctx.call_on_close(cleanup)

    ctx.command.add_command(boot, "boot")
    ctx.command.add_command(wait, "wait")
    ctx.command.add_command(terminate, "terminate")
    ctx.command.add_command(kill, "kill")
    ctx.command.add_command(flush, "flush")
    ctx.command.add_command(logs, "logs")
    ctx.command.add_command(restart, "restart")
    ctx.command.add_command(ps, "ps")
    ctx.command.add_command(dummy_boot, "dummy_boot")

    process_manager_shell_log.info("Ready")
