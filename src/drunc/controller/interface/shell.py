import getpass
import os

import click
import click_shell

from drunc.controller.interface.commands import (
    connect,
    disconnect,
    exclude,
    expert_command,
    include,
    recompute_status,
    status,
    surrender_control,
    take_control,
    wait,
    who_am_i,
    who_is_in_charge,
)
from drunc.controller.interface.shell_utils import (
    controller_cleanup_wrapper,
    controller_setup,
    generate_fsm_command,
)
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.utils import (
    CONTEXT_SETTINGS,
    create_logger_handler,
    get_logger,
    log_levels,
    setup_root_logger,
    validate_command_facility,
)


@click_shell.shell(
    prompt="drunc-controller > ",
    chain=True,
    context_settings=CONTEXT_SETTINGS,
    hist_file=os.path.expanduser("~") + "/.drunc-controller-shell.history",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.argument("controller-address", type=str, callback=validate_command_facility)
@click.pass_context
def controller_shell(ctx, controller_address: str, log_level: str) -> None:
    setup_root_logger(log_level)
    controller_shell_log = get_logger("controller.shell")
    create_logger_handler(rich_handler=True)

    controller_shell_log.debug("Resetting the context instance address")
    ctx.obj.reset(address=controller_address)
    ctx.call_on_close(controller_cleanup_wrapper(ctx.obj))

    desc = None
    controller_shell_log.debug(
        f"[green]{getpass.getuser()}[/green] connecting to the [green]controller[/green] through a [green]controller-shell[/green] via address [green]{controller_address}[/green]"
    )
    try:
        desc = controller_setup(ctx.obj, controller_address)
    except ServerUnreachable as e:
        controller_shell_log.critical("Could not connect to the controller")
        controller_shell_log.exception(
            e
        )  # TODO: Keep this for dev branch, remove it for production branch
        # controller_shell_log.error(e.message) # TODO: Keep this for production branch, remove this from dev branch
        exit(1)

    controller_shell_log.warning(
        f"[green]{getpass.getuser()}[/green] connected to the [green]{ctx.obj.get_driver('controller').describe().data.name}[/green] through a [green]controller-shell[/green] via address [green]{controller_address}[/green]"
    )

    # TODO: work out how to make the following lines legit without breaking the wrapper
    # def cleanup():
    #     ctx.call_on_close(controller_shell_log.warning(f"[green]{getpass.getuser()}[/green] disconnected from the [green]{ctx.obj.get_driver('controller').describe().data.name}[/green] through a [green]controller-shell[/green]"))
    # ctx.call_on_close(cleanup)

    transitions = (
        ctx.obj.get_driver("controller").describe_fsm(key="all-transitions").data
    )

    transitions = (
        ctx.obj.get_driver("controller").describe_fsm(key="all-transitions").data
    )

    ctx.command.add_command(status, "status")
    ctx.command.add_command(recompute_status, "recompute-status")
    ctx.command.add_command(connect, "connect")
    ctx.command.add_command(disconnect, "disconnect")
    ctx.command.add_command(take_control, "take-control")
    ctx.command.add_command(surrender_control, "surrender-control")
    ctx.command.add_command(who_am_i, "whoami")
    ctx.command.add_command(who_is_in_charge, "who-is-in-charge")
    for transition in transitions.commands:
        ctx.command.add_command(*generate_fsm_command(ctx.obj, transition, desc.name))
    ctx.command.add_command(include, "include")
    ctx.command.add_command(exclude, "exclude")
    ctx.command.add_command(wait, "wait")
    ctx.command.add_command(expert_command, "expert-command")
