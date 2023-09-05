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
    def __init__(self, pm_conf:str=None, print_traceback:bool=False) -> None:
        self.print_traceback = True
        from rich.console import Console
        from drunc.utils.utils import CONSOLE_THEMES
        self._console = Console(theme=CONSOLE_THEMES)
        import logging
        self._log = logging.getLogger("PMContext")
        self._log.info('initialising PMContext')
        if pm_conf is None:
            return

        self.pm_conf_data = {}
        with open(pm_conf) as f:
            import json
            self.pm_conf_data = json.loads(f.read())

        user=getpass.getuser()
        self.token = Token(token=f'{user}-token', user_name=user)
        self.pmd = ProcessManagerDriver(
            self.pm_conf_data,
            self.token
        )

        self.print_traceback = print_traceback
        self.status_receiver = None

    def start_listening(self, topic):
        from drunc.broadcast.client.kafka_stdout_broadcast_handler import KafkaStdoutBroadcastHandler
        from druncschema.broadcast_pb2 import BroadcastMessage
        self.status_receiver = KafkaStdoutBroadcastHandler(
            conf = self.pm_conf_data['broadcaster'],
            topic = topic,
            message_format = BroadcastMessage,
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
@click.argument('pm-conf', type=click.Path(exists=True))
@click.pass_context
def process_manager_shell(ctx, pm_conf:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level
    update_log_level(log_level)

    ctx.obj = PMContext(
        pm_conf = pm_conf,
        print_traceback = traceback
    )

    desc = asyncio.get_event_loop().run_until_complete(
        ctx.obj.pmd.describe()
    )

    ctx.obj._log.info(f'{ctx.obj.pmd.pm_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    ctx.obj.start_listening(
        f'{desc.name}.{desc.session}'
    )

    def cleanup():
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)



@process_manager_shell.command('boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@accept_configuration_type()
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.argument('session-name', type=str)
@click.pass_obj
@coroutine
async def boot(obj:PMContext, user:str, conf_type:str, session_name:str, boot_configuration:str) -> None:

    results = obj.pmd.boot(boot_configuration, user, session_name, conf_type)
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
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@click.pass_obj
@coroutine
async def ps(obj:PMContext, query:ProcessQuery, long_format:bool) -> None:
    results = await obj.pmd.ps(query=query)

    obj.print(tabulate_process_instance_list(results, title='Processes running', long=long_format))


