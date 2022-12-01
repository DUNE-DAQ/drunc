import asyncio
import click
import click_shell
import os
from functools import wraps

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels,  update_log_level
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance, ProcessDescription, ProcessRestriction, ProcessMetadata
from drunc.utils.utils import now_str

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

class PMContext:
    def __init__(self, pmd:ProcessManagerDriver) -> None:
        self.pmd = pmd
        from rich.console import Console
        from drunc.utils.utils import CONSOLE_THEMES
        self._console = Console(theme=CONSOLE_THEMES)

    def print(self, text):
        self._console.print(text)

@click_shell.shell(prompt='pm > ', chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_obj
def pm_shell(obj:PMContext, log_level:str) -> None:
    update_log_level(log_level)

@pm_shell.command('boot')
@click.option('-p','--partition', type=str, default=None, help='Select the processes on a particular partition')
@click.option('-n','--name'     , type=str, default=None, help='Select the process of a particular name')
@click.option('-u','--user'     , type=str, default=os.getlogin(), help='Select the process of a particular user (default $USER)')
@click.pass_obj
@coroutine
async def boot(obj:PMContext, name:str, user:str, partition:str) -> None:
    result = await obj.pmd.boot(
        BootRequest(
            process_description = ProcessDescription(
                metadata = ProcessMetadata(
                    user = user,
                    partition = partition,
                    name = name,
                ),
                executable_and_arguments = {
                    'sleep': ProcessDescription.StringList(values = ['3600'])
                }
            ),
            process_restriction = ProcessRestriction(
                allowed_hosts = ['localhost']
            )
        )
    )
    obj.print(result)

@pm_shell.command('kill')
@click.argument('uuid', type=str)
@click.pass_obj
@coroutine
async def kill(obj:PMContext, uuid:str) -> None:
    result = await obj.pmd.kill(
        ProcessUUID(uuid=uuid)
    )
    obj.print(result)

@pm_shell.command('ps')
@click.option('-p','--partition', type=str, default=None, help='Select the processes on a particular partition')
@click.option('-n','--name'     , type=str, default=None, help='Select the process of a particular name')
@click.option('-u','--user'     , type=str, default=None, help='Select the process of a particular user')
@click.option('--uuid'          , type=str, default=None, help='Select the process of a particular UUID')
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@coroutine
async def ps(obj:PMContext, name:str, user:str, uuid:str, partition:str, long_format:bool) -> None:
    uid = ProcessUUID(
        uuid=uuid
    )

    pm = ProcessMetadata(
        partition = partition,
        name = name,
        user = user,
        uuid = uid
    )

    results = await obj.pmd.list_process(selector=pm)

    from rich.table import Table
    t = Table(title='Process list')
    t.add_column('partition')
    t.add_column('user')
    t.add_column('friendly name')
    t.add_column('uuid')
    if long_format:
        t.add_column('executable')
    t.add_column('alive')

    for result in results.values:
        m = result.process_description.metadata
        row = [m.partition, m.user, m.name, m.uuid.uuid]
        if long_format:
            executables = list(result.process_description.executable_and_arguments.keys())
            row += [';'.join(executables)]
        alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'
        row += [alive]
        t.add_row(*row)
    obj.print(t)
