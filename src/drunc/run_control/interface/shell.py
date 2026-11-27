import getpass
import os

import click
import click_shell

from drunc.run_control.interface.commands import (
    boot,
    connect_to_pm,
    echo,
    logs,
    print_session_config,
    ps,
    set_session_config,
    terminate,
    validate,
    wait,
)
from drunc.run_control.interface.context import SHELL_ID, RunControlContext
from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.shell_utils import create_dummy_token_from_uname
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    format_name_for_cli,
    get_logger,
    get_root_logger,
    resolve_localhost_to_hostname,
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

    # Initialise run control driver.
    run_control_address = resolve_localhost_to_hostname(run_control_address)
    run_control_driver = RunControlDriver(
        run_control_address, create_dummy_token_from_uname()
    )
    ctx.obj = RunControlContext(run_control_driver)

    rc_log.info("running a logger instance")
    rc_shell_log.info("getting describe from run control driver")

    try:
        desc = run_control_driver.describe()
    except ServerUnreachable as e:
        rc_shell_log.critical("Could not connect to the run control")
        rc_shell_log.exception(e)
        exit(1)

    rc_log.info(
        f"[green]{getpass.getuser()}[/green] connected to the run control through a "
        f"[green]drunc-run-control-shell[/green] via address [green]{run_control_address}[/green]"
    )

    rc_shell_log.info(
        f"Connected to {run_control_address}, running '{desc.name}.{desc.session}' (name.session), "
        "starting listening..."
    )

    run_control_driver.send_log(f"{getpass.getuser()} connected from {SHELL_ID}")

    def cleanup() -> None:
        run_control_driver.send_log(f"disconnecting from {SHELL_ID}")
        rc_log.info("disconnected from process manager thingy")

    ctx.call_on_close(cleanup)

    exposed_run_control_commands = [
        wait,
        echo,
        connect_to_pm,
        ps,
        set_session_config,
        print_session_config,
        validate,
        boot,
        logs,
        terminate,
    ]

    for cmd in exposed_run_control_commands:
        rc_shell.add_command(cmd, format_name_for_cli(cmd.name or ""))

    rc_log.info("Ready")
