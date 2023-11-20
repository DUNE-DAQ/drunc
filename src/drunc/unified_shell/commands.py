import click
import getpass

from drunc.utils.shell_utils import add_traceback_flag
from drunc.utils.utils import run_coroutine, log_levels
from drunc.process_manager.interface.cli_argument import accept_configuration_type
from drunc.process_manager.interface.context import ProcessManagerContext


@click.command('boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@accept_configuration_type()
@add_traceback_flag()
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.argument('session-name', type=str)
@click.pass_obj
@run_coroutine
async def boot(obj:ProcessManagerContext, user:str, conf_type:str, session_name:str, boot_configuration:str, log_level:str, traceback:bool) -> None:

    from drunc.utils.shell_utils import InterruptedCommand
    try:
        results = obj.get_driver('process_manager').boot(
            conf = boot_configuration,
            conf_type = conf_type,
            user = user,
            session_name = session_name,
            log_level = log_level,
            rethrow = traceback,
        )
        async for result in results:
            if not result: break
            obj.print(f'\'{result.process_description.metadata.name}\' ({result.uuid.uuid}) process started')
    except InterruptedCommand:
        return

    controller_address = obj.get_driver('process_manager').controller_address
    if controller_address:
        obj.print(f'Controller endpoint is \'{controller_address}\'')
        obj.print(f'Connecting this shell to it...')
        from drunc.exceptions import DruncException
        try:
            obj.set_controller_driver(controller_address, obj.print_traceback)
            from drunc.controller.interface.shell_utils import controller_setup
            controller_setup(obj, controller_address)
        except DruncException as de:
            if traceback:
                raise de
            else:
                obj.error(de)

    else:
        obj.error(f'Could not understand where the controller is!')
        return


