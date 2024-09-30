import click
import getpass

from drunc.utils.utils import run_coroutine, log_levels
from drunc.process_manager.interface.context import ProcessManagerContext
from drunc.process_manager.interface.cli_argument import validate_conf_string

@click.command('boot')
@click.option(
    '-u','--user',
    type=str,
    default=getpass.getuser(),
    help='Select the process of a particular user (default $USER)'
)
@click.option(
    '-l', '--log-level',
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default='INFO',
    help='Set the log level'
)
@click.option(
    '--override-logs/--no-override-logs',
    default=True
)
@click.pass_obj
@run_coroutine
async def boot(
    obj:ProcessManagerContext,
    user:str,
    log_level:str,
    override_logs:bool,
    ) -> None:


    from drunc.utils.shell_utils import InterruptedCommand
    try:
        results = obj.get_driver('process_manager').boot(
            conf = obj.boot_configuration,
            user = user,
            session_name = obj.session_name,
            log_level = log_level,
            override_logs = override_logs,
        )
        async for result in results:
            if not result: break
            obj.print(f'\'{result.data.process_description.metadata.name}\' ({result.data.uuid.uuid}) process started')
    except InterruptedCommand:
        return

    controller_address = obj.get_driver('process_manager').controller_address
    if controller_address:
        obj.print(f'Controller endpoint is \'{controller_address}\'')
        obj.print(f'Connecting this shell to it...')
        obj.set_controller_driver(controller_address)
        from drunc.controller.interface.shell_utils import controller_setup
        controller_setup(obj, controller_address)

    else:
        obj.error(f'Could not understand where the controller is!')
        return


