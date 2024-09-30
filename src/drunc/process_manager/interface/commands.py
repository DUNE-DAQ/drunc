import click
import getpass

from drunc.utils.utils import run_coroutine, log_levels
from drunc.process_manager.interface.cli_argument import add_query_options
from drunc.process_manager.interface.context import ProcessManagerContext

from druncschema.process_manager_pb2 import ProcessQuery, ProcessInstanceList
from drunc.process_manager.interface.cli_argument import validate_conf_string

@click.command('boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.option('--override-logs/--no-override-logs', default=True)
@click.argument('boot-configuration', type=str, callback=validate_conf_string)
@click.argument('session-name', type=str)
@click.pass_obj
@run_coroutine
async def boot(
    obj:ProcessManagerContext,
    user:str,
    session_name:str,
    boot_configuration:str,
    log_level:str,
    override_logs:bool,
    ) -> None:

    from drunc.utils.shell_utils import InterruptedCommand
    try:
        results = obj.get_driver('process_manager').boot(
            conf = boot_configuration,
            user = user,
            session_name = session_name,
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
        from rich.panel import Panel
        obj.print(Panel(f"Controller endpoint: '{controller_address}', point your 'drunc-controller-shell' to it.", padding=(2,6), style='violet', border_style='violet'), justify='center')
    else:
        obj.error(f'Could not understand where the controller is! You can look at the logs of the controller to see its address')
        return

@click.command('dummy_boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.option('-n','--n-processes', type=int, default=1, help='Select the number of dummy processes to boot (default 1)')
@click.option('-s','--sleep', type=int, default=10, help='Select the timeout duration in seconds (default 30)')
@click.option('--n_sleeps', type=int, default=6, help='Select the number of timeouts (default 5)')
@click.argument('session-name', type=str)
@click.pass_obj
@run_coroutine
async def dummy_boot(obj:ProcessManagerContext, user:str, n_processes:int, sleep:int, n_sleeps:int, session_name:str) -> None:

    from drunc.utils.shell_utils import InterruptedCommand
    try:
        results = obj.get_driver('process_manager').dummy_boot(
            user = user,
            session_name = session_name,
            n_processes = n_processes,
            sleep = sleep,
            n_sleeps = n_sleeps,
        )
        async for result in results:
            if not result: break
            obj.print(f'\'{result.data.process_description.metadata.name}\' ({result.data.uuid.uuid}) process started')
    except InterruptedCommand:
        return


@click.command('terminate')
@add_query_options(at_least_one=False)
@click.pass_obj
@run_coroutine
async def terminate(obj:ProcessManagerContext, query:ProcessQuery) -> None:
    result = await obj.get_driver('process_manager').terminate(
        query = query,
    )
    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result.data, 'Terminated process', False))

@click.command('kill')
@add_query_options(at_least_one=False)
@click.pass_obj
@run_coroutine
async def kill(obj:ProcessManagerContext, query:ProcessQuery) -> None:
    result = await obj.get_driver('process_manager').kill(
        query = query,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result.data, 'Killed process', False))


@click.command('flush')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.pass_obj
@run_coroutine
async def flush(obj:ProcessManagerContext, query:ProcessQuery) -> None:
    result = await obj.get_driver('process_manager').flush(
        query = query,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result.data, 'Flushed process', False))


@click.command('logs')
@add_query_options(at_least_one=True)
@click.option('--how-far', type=int, default=100, help='How many lines one wants')
@click.option('--grep', type=str, default=None)
@click.pass_obj
@run_coroutine
async def logs(obj:ProcessManagerContext, how_far:int, grep:str, query:ProcessQuery) -> None:
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
        ):
        if not result: break

        if uuid is None:
            uuid = result.data.uuid.uuid
            obj.rule(f'[yellow]{uuid}[/yellow] logs')

        line = result.data.line
        if line == "":
            obj.print('')
            continue

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
@click.pass_obj
@run_coroutine
async def restart(obj:ProcessManagerContext, query:ProcessQuery) -> None:
    result = await obj.get_driver('process_manager').restart(
        query = query,
    )

    if not result: return

    obj.print(result.data)


@click.command('ps')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@run_coroutine
async def ps(obj:ProcessManagerContext, query:ProcessQuery, long_format:bool) -> None:
    results = await obj.get_driver('process_manager').ps(
        query=query,
    )

    if not results: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(results.data, title='Processes running', long=long_format))


