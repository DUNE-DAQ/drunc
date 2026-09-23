import os

import click
import click_shell

from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
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
    except ServerUnreachable as e:
        rc_shell_log.critical("Could not connect to the run control")
        rc_shell_log.exception(e)
        exit(1)
    except Exception as e:
        rc_shell_log.critical(f"failed somehow: {e}")
        rc_shell_log.info("dont forget to remove this fork :)")
        exit(1)
