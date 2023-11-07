import click
import getpass

from drunc.utils.shell_utils import add_traceback_flag
from drunc.utils.utils import run_coroutine, log_levels
from drunc.process_manager.interface.cli_argument import accept_configuration_type, add_query_options
from drunc.process_manager.interface.context import ProcessManagerContext

from druncschema.process_manager_pb2 import ProcessQuery


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

    controller_address = obj.get_driver('process_manager').controller_address
    if controller_address:
        from rich.panel import Panel
        obj.print(Panel("Controller endpoint: '{controller_address}', point your 'drunc-controller-shell' to it.", padding=(2,6), style='violet', border_style='violet'), justify='center')
    else:
        obj.error(f'Could not understand where the controller is! You can look at the logs of the controller to see its address')
        return



@click.command('kill')
@add_query_options(at_least_one=False)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def kill(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver('process_manager').kill(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result, 'Killed process', False))


@click.command('flush')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def flush(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver('process_manager').flush(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result, 'Flushed process', False))


@click.command('logs')
@add_query_options(at_least_one=True)
@click.option('--how-far', type=int, default=100, help='How many lines one wants')
@click.option('--grep', type=str, default=None)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def logs(obj:ProcessManagerContext, how_far:int, grep:str, query:ProcessQuery, traceback:bool) -> None:
    from druncschema.process_manager_pb2 import LogRequest, LogLine

    log_req = LogRequest(
        how_far = how_far,
        query = query,
    )

    uuid = None
    from rich.markup import escape
    from drunc.utils.grpc_utils import unpack_any

    async for result in obj.get_driver('process_manager').logs(
        log_req,
        rethrow = traceback,
        ):
        if not result: break

        if uuid is None:
            uuid = result.uuid.uuid
            obj.rule(f'[yellow]{uuid}[/yellow] logs')

        line = result.line

        if line[-1] == '\n':
            line = line[:-1]

        if grep is not None and grep not in line:
            continue

        line = escape(line)

        if grep is not None:
            line = line.replace(grep, f'[u]{grep}[/]')

        obj.print(line)

    obj.rule(f'End')


@click.command('restart')
@add_query_options(at_least_one=True)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def restart(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver('process_manager').restart(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    obj.print(result)


@click.command('ps')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def ps(obj:ProcessManagerContext, query:ProcessQuery, long_format:bool, traceback:bool) -> None:
    results = await obj.get_driver('process_manager').ps(
        query=query,
        rethrow = traceback,
    )

    if not results: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(results, title='Processes running', long=long_format))


