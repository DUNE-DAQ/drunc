import click_shell
import click
import os

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,validate_command_facility

@click_shell.shell(prompt='drunc-process-manager > ', chain=True, context_settings=CONTEXT_SETTINGS, hist_file=os.path.expanduser('~')+'/.drunc-pm-shell.history')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.argument('process-manager-address', type=str, callback=validate_command_facility)
@click.pass_context
def process_manager_shell(ctx, process_manager_address:str, log_level:str) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj.reset(
        address = process_manager_address,
    )

    try:
        import asyncio
        desc = asyncio.get_event_loop().run_until_complete(
            ctx.obj.get_driver('process_manager').describe()
        )
    except Exception as e:
        ctx.obj.critical(f'Could not connect to the process manager, use --log-level DEBUG for more information')
        ctx.obj.debug(f'Error: {e}')
        ctx.obj.print(f'\nIf you are sure that the process manager exists, on the NP04 cluster, this error usually happens when you have the web proxy enabled. Try to disable it by doing:\n\n[yellow]source ~np04daq/bin/web_proxy.sh -u[/]\n')
        exit(1)

    ctx.obj.info(f'{process_manager_address} is \'{desc.data.name}.{desc.data.session}\' (name.session), starting listening...')
    if desc.data.HasField('broadcast'):
        ctx.obj.start_listening(desc.data.broadcast)

    def cleanup():
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    from drunc.process_manager.interface.commands import boot, terminate, kill, flush, logs, restart, ps, dummy_boot
    ctx.command.add_command(boot, 'boot')
    ctx.command.add_command(terminate, 'terminate')
    ctx.command.add_command(kill, 'kill')
    ctx.command.add_command(flush, 'flush')
    ctx.command.add_command(logs, 'logs')
    ctx.command.add_command(restart, 'restart')
    ctx.command.add_command(ps, 'ps')
    ctx.command.add_command(dummy_boot, 'dummy_boot')