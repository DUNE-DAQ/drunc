import click
import click_shell
from drunc.utils.utils import log_levels
import os
from drunc.utils.utils import validate_command_facility

@click_shell.shell(prompt='drunc-unified-shell > ', chain=True, hist_file=os.path.expanduser('~')+'/.drunc-unified-shell.history')
@click.option('-t', '--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.argument('process-manager-address', type=str, callback=validate_command_facility)
@click.pass_context
def unified_shell(ctx, process_manager_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj.reset(
        print_traceback = traceback,
        address_pm = process_manager_address,
    )

    from drunc.utils.grpc_utils import ServerUnreachable

    try:
        import asyncio
        desc = asyncio.get_event_loop().run_until_complete(
            ctx.obj.get_driver().describe(rethrow=True)
        )
    except ServerUnreachable as e:
        ctx.obj.critical(f'Could not connect to the process manager')
        raise e

    ctx.obj.info(f'{process_manager_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    if desc.HasField('broadcast'):
        ctx.obj.start_listening_pm(
            broadcaster_conf = desc.broadcast,
        )

    def cleanup():
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    from drunc.unified_shell.commands import boot
    ctx.command.add_command(boot, 'boot')

    from drunc.process_manager.interface.commands import kill, flush, logs, restart, ps
    ctx.command.add_command(kill, 'kill')
    ctx.command.add_command(flush, 'flush')
    ctx.command.add_command(logs, 'logs')
    ctx.command.add_command(restart, 'restart')
    ctx.command.add_command(ps, 'ps')

    from drunc.controller.interface.commands import (
        describe, ls, status, connect, take_control, surrender_control, who_am_i, who_is_in_charge, fsm, include, exclude
    )
    ctx.command.add_command(describe, 'describe')
    ctx.command.add_command(ls, 'ls')
    ctx.command.add_command(status, 'status')
    ctx.command.add_command(connect, 'connect')
    ctx.command.add_command(take_control, 'take-control')
    ctx.command.add_command(surrender_control, 'surrender-control')
    ctx.command.add_command(who_am_i, 'whoami')
    ctx.command.add_command(who_is_in_charge, 'who-is-in-charge')
    ctx.command.add_command(fsm, 'fsm')
    ctx.command.add_command(include, 'include')
    ctx.command.add_command(exclude, 'exclude')
