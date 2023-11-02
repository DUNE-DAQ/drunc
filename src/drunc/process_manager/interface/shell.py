import click_shell
import click

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels

@click_shell.shell(prompt='drunc-process-manager > ', chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.option('-t','--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.argument('process-manager-address', type=str)
@click.pass_context
def process_manager_shell(ctx, process_manager_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj.reset(
        print_traceback = traceback,
        address = process_manager_address,
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
    ctx.obj.start_listening(desc.broadcast)

    def cleanup():
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    from drunc.process_manager.interface.commands import boot, kill, flush, logs, restart, ps
    ctx.command.add_command(boot, 'boot')
    ctx.command.add_command(kill, 'kill')
    ctx.command.add_command(flush, 'flush')
    ctx.command.add_command(logs, 'logs')
    ctx.command.add_command(restart, 'restart')
    ctx.command.add_command(ps, 'ps')