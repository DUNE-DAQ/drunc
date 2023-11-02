
import getpass
from typing import Mapping

from drunc.utils.utils import CONTEXT_SETTINGS, log_levels
from druncschema.process_manager_pb2 import ProcessQuery
from druncschema.token_pb2 import Token
from drunc.utils.shell_utils import ShellContext, GRPCDriver, add_traceback_flag
from drunc.utils.utils import run_coroutine
from drunc.process_manager.interface.cli_argument import accept_configuration_type, add_query_options

class ProcessManagerContext(ShellContext): # boilerplatefest
    status_receiver = None

    def reset(self, address:str=None, print_traceback:bool=False):
        self.address = address
        super(ProcessManagerContext, self)._reset(
            print_traceback = print_traceback,
            name = 'process_manager_context',
            token_args = {},
            driver_args = {
                'print_traceback': print_traceback
            },
        )

    def create_drivers(self, print_traceback, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}

        from drunc.process_manager.process_manager_driver import ProcessManagerDriver

        return {
            'process_manager_driver': ProcessManagerDriver(
                self.address,
                self._token,
                aio_channel = True,
                rethrow_by_default = print_traceback
            )
        }

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.conf_types import ConfTypes

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = broadcaster_conf,
            conf_type = ConfTypes.Protobuf
        )
        from rich import print as rprint
        rprint(f':ear: Listening to the Process Manager at {self.address}')

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()

import click
import click_shell

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



@process_manager_shell.command('boot')
@click.option('-u','--user', type=str, default=getpass.getuser(), help='Select the process of a particular user (default $USER)')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@accept_configuration_type()
@add_traceback_flag()
@click.argument('boot-configuration', type=click.Path(exists=True))
@click.argument('session-name', type=str)
@click.pass_obj
@run_coroutine
async def boot(obj:ProcessManagerContext, user:str, conf_type:str, session_name:str, boot_configuration:str, log_level:str, traceback:bool) -> None:

    results = obj.get_driver().boot(
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


@process_manager_shell.command('kill')
@add_query_options(at_least_one=False)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def kill(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver().kill(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result, 'Killed process', False))


@process_manager_shell.command('flush')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def flush(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver().flush(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(result, 'Flushed process', False))


@process_manager_shell.command('logs')
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

    async for result in obj.get_driver().logs(
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


@process_manager_shell.command('restart')
@add_query_options(at_least_one=True)
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def restart(obj:ProcessManagerContext, query:ProcessQuery, traceback:bool) -> None:
    result = await obj.get_driver().restart(
        query = query,
        rethrow = traceback,
    )

    if not result: return

    obj.print(result)


@process_manager_shell.command('ps')
@add_query_options(at_least_one=False, all_processes_by_default=True)
@click.option('-l','--long-format', is_flag=True, type=bool, default=False, help='Whether to have a long output')
@add_traceback_flag()
@click.pass_obj
@run_coroutine
async def ps(obj:ProcessManagerContext, query:ProcessQuery, long_format:bool, traceback:bool) -> None:
    results = await obj.get_driver().ps(
        query=query,
        rethrow = traceback,
    )

    if not results: return

    from drunc.process_manager.utils import tabulate_process_instance_list
    obj.print(tabulate_process_instance_list(results, title='Processes running', long=long_format))


