import click
import click_shell
import os

from drunc.controller.interface.context import ControllerContext
from drunc.utils.utils import log_levels
from drunc.utils.shell_utils import add_traceback_flag


@click_shell.shell(prompt='drunc-controller > ', chain=True, hist_file=os.path.expanduser('~')+'/.drunc-controller-shell.history')
@click.argument('controller-address', type=str)
@click.option('-t', '--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level

    update_log_level(log_level)

    ctx.obj.reset(
        print_traceback = traceback,
        address = controller_address,
    )

    from drunc.controller.interface.shell_utils import controller_setup, controller_cleanup_wrapper
    ctx.call_on_close(controller_cleanup_wrapper(ctx.obj))
    controller_setup(ctx.obj, controller_address)

    from drunc.controller.interface.commands import (
        describe, ls, status, connect, take_control, surrender_control, who_am_i, who_is_in_charge, fsm, include, exclude
    )
    ctx.command.add_command(describe, 'describe')
    ctx.command.add_command(ls, 'ls')
    ctx.command.add_command(status, 'status')
    ctx.command.add_command(connect, 'connect')
    ctx.command.add_command(take_control, 'take_control')
    ctx.command.add_command(surrender_control, 'surrender-control')
    ctx.command.add_command(who_am_i, 'whoami')
    ctx.command.add_command(who_is_in_charge, 'who-is-in-charge')
    ctx.command.add_command(fsm, 'fsm')
    ctx.command.add_command(include, 'include')
    ctx.command.add_command(exclude, 'exclude')
