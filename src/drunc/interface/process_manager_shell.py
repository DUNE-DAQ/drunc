import asyncio
import click
import click_shell
import os
import getpass
from functools import wraps

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,  update_log_level
from druncschema.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance, ProcessDescription, ProcessRestriction, ProcessMetadata, ProcessQuery
from druncschema.token_pb2 import Token
from drunc.utils.utils import now_str
from typing import Optional
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from functools import update_wrapper

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


def generate_query(f,at_least_one):
    @click.pass_context
    def new_func(ctx, session, name, user, uuid, **kwargs):
        if uuid is None and session is None and name is None and user is None and at_least_one:
            raise click.BadParameter('You need to provide at least a \'--uuid\', \'--session\', \'--user\' or \'--name\'!')

        puuid = ProcessUUID(
            uuid=uuid
        )
        query = ProcessQuery(
            session = session,
            name = name,
            user = user,
            uuid = puuid if uuid else None,
            force = False,
        )

        return ctx.invoke(f, query=query,**kwargs)
    return update_wrapper(new_func, f)

def tabulate_process_instance_list(pil, title, long=False):
    from rich.table import Table
    t = Table(title=title)
    t.add_column('session')
    t.add_column('user')
    t.add_column('friendly name')
    t.add_column('uuid')
    t.add_column('alive')
    t.add_column('exit-code')
    if long:
        t.add_column('executable')

    for result in pil.values:
        m = result.process_description.metadata
        row = [m.session, m.user, m.name, result.uuid.uuid]
        alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'
        row += [alive, f'{result.return_code}']
        if long:
            executables = [e.exec for e in result.process_description.executable_and_arguments]
            row += ['; '.join(executables)]
        t.add_row(*row)

    return t


def add_query_options(at_least_one):
    def wrapper(f0):
        f1 = click.option('-s','--session', type=str, default=None, help='Select the processes on a particular session')(f0)
        f2 = click.option('-n','--name'   , type=str, default=None, help='Select the process of a particular name')(f1)
        f3 = click.option('-u','--user'   , type=str, default=None, help='Select the process of a particular user')(f2)
        f4 = click.option('--uuid'        , type=str, default=None, help='Select the process of a particular UUID')(f3)
        return generate_query(f4,at_least_one)
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
    def rule(self, text):
        self._console.rule(text)

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

    obj.pmd = ProcessManagerDriver(pm_conf_data, token = Token(token='123', user_name=getpass.getuser()))

    update_log_level(log_level)


@process_manager_shell.command('boot')
@click.option('-u','--user'   , type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.argument('session-name', type=str)
@click.pass_obj
@coroutine
async def boot(obj:PMContext, user:str, session_name:str, boot_configuration:str) -> None:

    results = obj.pmd.boot(boot_configuration, user, session_name)
    async for result in results:
        obj.print(f'\'{result.process_description.metadata.name}\' ({result.uuid.uuid}) process started')


@process_manager_shell.command('kill')
@add_query_options(at_least_one=True)
@click.pass_obj
@coroutine
async def kill(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.kill(query = query)
    obj.print(result)


@process_manager_shell.command('killall')
@add_query_options(at_least_one=False)
@click.option('-f', '--force', is_flag=True, default=False)
@click.pass_obj
@coroutine
async def killall(obj:PMContext, query:ProcessQuery, force:bool) -> None:
    query.force = force
    result = await obj.pmd.killall(query = query)
    obj.print(tabulate_process_instance_list(result, 'Killed process', False))

@process_manager_shell.command('flush')
@add_query_options(at_least_one=False)
@click.pass_obj
@coroutine
async def flush(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.flush(query = query)
    obj.print(tabulate_process_instance_list(result, 'Flushed process', False))

@process_manager_shell.command('logs')
@add_query_options(at_least_one=True)
@click.option('--how-far', type=int, default=100, help='How many lines one wants')
@click.pass_obj
@coroutine
async def logs(obj:PMContext, how_far:int, query:ProcessQuery) -> None:
    from druncschema.process_manager_pb2 import LogRequest, LogLine

    log_req = LogRequest(
        how_far = how_far,
        query = query,
    )

    uuid = None
    from rich.markup import escape
    from drunc.utils.grpc_utils import unpack_any

    async for result in obj.pmd.logs(log_req):

        if uuid is None:
            uuid = result.uuid.uuid
            obj.rule(f'[yellow]{uuid}[/yellow] logs')

        line = result.line

        if line[-1] == '\n':
            line = line[:-1]

        line = escape(line)

        console_print = True
        for c in ['[',']']: # If these are here, it probably means that this is already a rich formatted string
            if c in line:
                console_print = False
                break

        if console_print:
            obj.print(line)
        else:
            print(line)

    obj.rule(f'End')


@process_manager_shell.command('restart')
@add_query_options(at_least_one=True)
@click.pass_obj
@coroutine
async def restart(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.restart(query = query)
    obj.print(result)


@process_manager_shell.command('ps')
@add_query_options(at_least_one=False)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@coroutine
async def ps(obj:PMContext, query:ProcessQuery, long_format:bool) -> None:
    results = await obj.pmd.ps(query=query)
    obj.print(tabulate_process_instance_list(results, title='Processes running', long=long_format))


