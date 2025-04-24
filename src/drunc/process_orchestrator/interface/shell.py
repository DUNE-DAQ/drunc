import asyncio
import getpass
import os

import click
import click_shell
from drunc_core.utils.grpc_utils import ServerUnreachable
from drunc_core.utils.utils import (
    CONTEXT_SETTINGS,
    create_logger_handler,
    get_logger,
    log_levels,
    setup_root_logger,
    validate_command_facility,
)

from drunc.process_orchestrator.interface.commands import (
    boot,
    dummy_boot,
    flush,
    kill,
    logs,
    ps,
    restart,
    terminate,
)


@click_shell.shell(
    prompt="drunc-process-orchestrator > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~") + "/.drunc-pm-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default=os.getenv("DRUNC_LOG_LEVEL", "INFO"),
    help="Set the log level, if not set, it will be set to the environment variable DRUNC_LOG_LEVEL, if that variable is not set, it will be set to INFO",
)
@click.argument(
    "process-orchestrator-address", type=str, callback=validate_command_facility
)
@click.pass_context
def process_orchestrator_shell(
    ctx, process_orchestrator_address: str, log_level: str
) -> None:
    setup_root_logger(log_level)
    process_orchestrator_shell_log = get_logger("process_orchestrator.shell")
    create_logger_handler(rich_handler=True)

    ctx.obj.reset(address=process_orchestrator_address)

    try:
        desc = asyncio.get_event_loop().run_until_complete(
            ctx.obj.get_driver("process_orchestrator").describe()
        )
    except ServerUnreachable as e:
        process_orchestrator_shell_log = get_logger(
            logger_name="process_orchestrator.shell", rich_handler=True
        )
        process_orchestrator_shell_log.critical(
            "Could not connect to the process orchestrator"
        )
        process_orchestrator_shell_log.exception(
            e
        )  # TODO: Keep this for dev branch, remove it for production branch
        # process_orchestrator_shell_log.error(e.message) # TODO: Keep this for production branch, remove this from dev branch
        exit(1)

    process_orchestrator_log = get_logger(
        logger_name="process_orchestrator",
        log_file_path=desc.data.info,
        override_log_file=False,
        rich_handler=True,
    )

    process_orchestrator_log.info(
        f"[green]{getpass.getuser()}[/green] connected to the process orchestrator through a [green]drunc-process-orchestrator-shell[/green] via address [green]{process_orchestrator_address}[/green]"
    )
    process_orchestrator_shell_log.info(
        f"Connected to {process_orchestrator_address}, running '{desc.data.name}.{desc.data.session}' (name.session), starting listening..."
    )
    if desc.data.HasField("broadcast"):
        ctx.obj.start_listening(desc.data.broadcast)

    def cleanup():
        ctx.obj.terminate()
        process_orchestrator_log.warning(
            f"[green]{getpass.getuser()}[/green] disconnected from the process orchestrator through a [green]drunc-process-orchestrator-shell[/green]"
        )

    ctx.call_on_close(cleanup)

    ctx.command.add_command(boot, "boot")
    ctx.command.add_command(terminate, "terminate")
    ctx.command.add_command(kill, "kill")
    ctx.command.add_command(flush, "flush")
    ctx.command.add_command(logs, "logs")
    ctx.command.add_command(restart, "restart")
    ctx.command.add_command(ps, "ps")
    ctx.command.add_command(dummy_boot, "dummy_boot")

    process_orchestrator_shell_log.info("Ready")
