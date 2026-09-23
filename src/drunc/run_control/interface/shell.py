import os
from typing import cast

import click
import click_shell

from drunc.run_control.interface.commands import echo
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    format_name_for_cli,
    get_logger,
    get_root_logger,
    validate_command_facility,
)


@click_shell.shell(
    prompt="drunc-run-control > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~") + "/.drunc-rc-shell.history",
)
@click.argument("run-control-address", type=str, callback=validate_command_facility)
@click.pass_context
def rc_shell(ctx: click.core.Context, run_control_address: str):
    get_root_logger("INFO")
    rc_log = get_logger(
        logger_name="run_control",
        rich_handler=True,
    )

    rc_shell_log = get_logger("run_control.shell")

    ctx.obj.reset(address=run_control_address)

    try:
        rc_log.info("running a logger instance")
        rc_shell_log.info("getting describe from run control driver")
        desc = ctx.obj.get_driver("run_control").describe()
    except ServerUnreachable as e:
        rc_shell_log.critical("Could not connect to the run control")
        rc_shell_log.exception(e)
        exit(1)
    except Exception as e:
        rc_shell_log.critical(f"failed somehow: {e}")
        rc_shell_log.info("dont forget to remove this fork :)")
        exit(1)

    rc_shell_log.info(
        f"Connected to {run_control_address}, running '{desc.name}.{desc.session}' (name.session), starting listening..."
    )

    def cleanup() -> None:
        ctx.obj.get_driver("run_control").send_log(
            f"disconnecting from {ctx.obj.shell_id}"
        )
        ctx.obj.terminate()
        rc_log.info("disconnected from process manager thingy")

    ctx.call_on_close(cleanup)

    exposed_run_control_commands = [echo]

    # cast the command group
    command_group = cast(click.core.Group, ctx.command)

    for cmd in exposed_run_control_commands:
        command_group.add_command(cmd, format_name_for_cli(cmd.name or ""))

    rc_log.info("ready")
