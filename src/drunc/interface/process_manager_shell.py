import asyncio
import click
import click_shell
import os
from functools import wraps

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,  update_log_level
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance, ProcessDescription, ProcessRestriction, ProcessMetadata, ProcessQuery, LogRequest
from drunc.utils.utils import now_str
from typing import Optional

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


def generate_query(ctx, param, uuid):
    uid = ProcessUUID(
        uuid=uuid
    )
    query = ProcessQuery(
        partition = ctx.params.get('partition'),
        name = ctx.params.get('name'),
        user = ctx.params.get('user'),
        uuid = uid if uuid else None,
        force = False,
    )
    return query

def process_query_option():
    def wrapper(f0):
        f1 = click.option('-p','--partition', type=str, default=None, help='Select the processes on a particular partition')(f0)
        f2 = click.option('-n','--name'     , type=str, default=None, help='Select the process of a particular name')(f1)
        f3 = click.option('-u','--user'     , type=str, default=None, help='Select the process of a particular user')(f2)
        return click.option('--uuid',  'query', type=str, default=None, help='Select the process of a particular UUID', callback=generate_query)(f3)
    return wrapper

class PMContext:
    def __init__(self, pmd:Optional[ProcessManagerDriver]=None) -> None:
        self.pmd = pmd
        from rich.console import Console
        from drunc.utils.utils import CONSOLE_THEMES
        self._console = Console(theme=CONSOLE_THEMES)
        self.print_traceback = False

    def print(self, text):
        self._console.print(text)


@click_shell.shell(prompt='pm > ', chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.option('-t', '--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.option('--pm-conf', type=click.Path(exists=True), default=os.getenv('DRUNC_DATA')+'/process-manager.json', help='Where the process-manager configuration is')
@click.pass_obj
def process_manager_shell(obj:PMContext, pm_conf:str, log_level:str, traceback:bool) -> None:
    obj.print_traceback = traceback

    pm_conf_data = {}
    with open(pm_conf) as f:
        import json
        pm_conf_data = json.loads(f.read())

    from drunc.process_manager.process_manager_driver import ProcessManagerDriver
    obj.pmd = ProcessManagerDriver(pm_conf_data)

    update_log_level(log_level)


@process_manager_shell.command('boot')
@click.option('-p','--partition', type=str, default=None, help='Select the processes on a particular partition')
@click.option('-u','--user'     , type=str, default=os.getlogin(), help='Select the process of a particular user (default $USER)')
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.pass_obj
@coroutine
async def boot(obj:PMContext, user:str, partition:str, boot_configuration:str) -> None:
    results = obj.pmd.boot(boot_configuration, user, partition)
    async for result in results:
        print(result)


@process_manager_shell.command('kill')
@process_query_option()
@click.pass_obj
@coroutine
async def kill(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str) -> None:
    result = await obj.pmd.kill(query = query)
    obj.print(result)


@process_manager_shell.command('killall')
@process_query_option()
@click.option('-f', '--force', is_flag=True, default=False)
@click.pass_obj
@coroutine
async def killall(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str, force:bool) -> None:
    query.force = force
    result = await obj.pmd.killall(query = query)
    obj.print(result)

@process_manager_shell.command('flush')
@process_query_option()
@click.pass_obj
@coroutine
async def flush(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str) -> None:
    result = await obj.pmd.flush(query = query)
    obj.print("Flushed processes:")
    obj.print(result)

@process_manager_shell.command('logs')
@process_query_option()
@click.option('--how-far', type=int, default=10, help='How many lines one wants')
@click.pass_obj
@coroutine
async def logs(obj:PMContext, how_far:int, name:str, user:str, query:ProcessQuery, partition:str) -> None:
    log_req = LogRequest(
        how_far = how_far,
        query = query,
    )

    uuid = None
    async for result in obj.pmd.logs(log_req):
        if uuid is None:
            uuid = result.uuid.uuid
            obj.print(f'\n\n\'{uuid}\' logs start:')
        obj.print(result.line[:-1]) # knock the return carriage at the end of the line
    obj.print(f'\'{uuid}\' logs end\n\n')


@process_manager_shell.command('restart')
@process_query_option()
@click.pass_obj
@coroutine
async def restart(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str) -> None:
    result = await obj.pmd.restart(query = query)
    obj.print(result)


@process_manager_shell.command('is-alive')
@process_query_option()
@click.pass_obj
@coroutine
async def is_alive(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str) -> None:
    result = await obj.pmd.is_alive(query = query)

    if result.status_code == ProcessInstance.StatusCode.RUNNING:
        obj.print(f'Process {uuid} (name: {result.process_description.metadata.name}) is alive')
    else:
        obj.print(f'[danger]Process {uuid} (name: {result.process_description.metadata.name}) is dead, error code: {result.return_code}[/danger]')


@process_manager_shell.command('ps')
@process_query_option()
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@coroutine
async def ps(obj:PMContext, name:str, user:str, query:ProcessQuery, partition:str, long_format:bool) -> None:
    results = await obj.pmd.list_process(query=query)

    from rich.table import Table
    t = Table(title='Processes running')
    t.add_column('partition')
    t.add_column('user')
    t.add_column('friendly name')
    t.add_column('uuid')
    t.add_column('alive')
    t.add_column('exit-code')
    if long_format:
        t.add_column('executable')

    for result in results.values:
        m = result.process_description.metadata
        row = [m.partition, m.user, m.name, result.uuid.uuid]
        alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'
        row += [alive, f'{result.return_code}']
        if long_format:
            executables = [e.exec for e in result.process_description.executable_and_arguments]
            row += ['; '.join(executables)]
        t.add_row(*row)
    obj.print(t)
