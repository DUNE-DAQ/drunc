import asyncio
import click
import click_shell
import os
import getpass
from functools import wraps

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels
from druncschema.process_manager_pb2 import ProcessQuery
from druncschema.token_pb2 import Token
from drunc.process_manager.interface.cli_argument import accept_configuration_type, add_query_options
from drunc.process_manager.utils import tabulate_process_instance_list
from drunc.process_manager.process_manager_driver import ProcessManagerDriver

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))
        #return asyncio.run(f(*args, **kwargs))
    return wrapper


class PMContext:
    def __init__(self, address:str=None, print_traceback:bool=False) -> None:
        self.print_traceback = True
        from rich.console import Console
        from drunc.utils.utils import CONSOLE_THEMES
        self._console = Console(theme=CONSOLE_THEMES)
        import logging
        self._log = logging.getLogger("PMContext")
        self._log.info('initialising PMContext')

        user=getpass.getuser()
        self.token = Token(token=f'{user}-token', user_name=user)
        if address:
            self.pmd = ProcessManagerDriver(
                address,
                self.token
            )

        self.print_traceback = print_traceback
        self.status_receiver = None

    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.conf_types import ConfTypes

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = broadcaster_conf,
            conf_type = ConfTypes.Protobuf
        )


    def terminate(self):
        self.status_receiver.stop()

    def print(self, text):
        self._console.print(text)

    def rule(self, text):
        self._console.rule(text)


@click_shell.shell(prompt='pm > ', chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.option('-t', '--traceback', is_flag=True, default=True, help='Print full exception traceback')
@click.argument('process-manager-address', type=str)
@click.pass_context
def process_manager_shell(ctx, process_manager_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj = PMContext(
        address = process_manager_address,
        print_traceback = traceback
    )

    desc = asyncio.get_event_loop().run_until_complete(
        ctx.obj.pmd.describe()
    )

    ctx.obj._log.info(f'{process_manager_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    ctx.obj.start_listening(desc.broadcast)

    def cleanup():
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)



@process_manager_shell.command('boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@accept_configuration_type()
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.argument('session-name', type=str)
@click.pass_obj
@coroutine
async def boot(obj:PMContext, user:str, conf_type:str, session_name:str, boot_configuration:str, log_level:str) -> None:

    results = obj.pmd.boot(
        conf = boot_configuration,
        conf_type = conf_type,
        user = user,
        session_name = session_name,
        log_level = log_level
    )
    async for result in results:
        obj.print(f'\'{result.process_description.metadata.name}\' ({result.uuid.uuid}) process started')


@process_manager_shell.command('kill')
@add_query_options(at_least_one=False)
@click.pass_obj
@coroutine
async def kill(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.kill(query = query)
    obj.print(tabulate_process_instance_list(result, 'Killed process', False))


@process_manager_shell.command('flush')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.pass_obj
@coroutine
async def flush(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.flush(query = query)
    obj.print(tabulate_process_instance_list(result, 'Flushed process', False))

@process_manager_shell.command('logs')
@add_query_options(at_least_one=True)
@click.option('--how-far', type=int, default=100, help='How many lines one wants')
@click.option('--grep', type=str, default=None)
@click.pass_obj
@coroutine
async def logs(obj:PMContext, how_far:int, grep:str, query:ProcessQuery) -> None:
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

        if grep is not None and grep not in line:
            continue

        line = escape(line)

        if grep is not None:
            line = line.replace(grep, f'[u]{grep}[/]')

        # console_print = True
        # for c in ['[',']']: # If these are here, it probably means that this is already a rich formatted string
        #     if c in line:
        #         console_print = False
        #         break

        # if console_print:
        obj.print(line)
        # else:
        #     print(line)

    obj.rule(f'End')


@process_manager_shell.command('restart')
@add_query_options(at_least_one=True)
@click.pass_obj
@coroutine
async def restart(obj:PMContext, query:ProcessQuery) -> None:
    result = await obj.pmd.restart(query = query)
    obj.print(result)


@process_manager_shell.command('ps')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@coroutine
async def ps(obj:PMContext, query:ProcessQuery, long_format:bool) -> None:
    results = await obj.pmd.ps(query=query)

    obj.print(tabulate_process_instance_list(results, title='Processes running', long=long_format))


