import click
import click_shell
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.communication.command_pb2 import Command
from drunc.utils.utils import now_str
import asyncio
from functools import wraps

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

class RCContext:
    def __init__(self, rc:Controller) -> None:
        self.rc = rc

@click_shell.shell(prompt='drunc > ', chain=True)
@click.pass_obj
def rc_shell(obj:RCContext) -> None:
    pass

@rc_shell.command('some-command')
@click.pass_obj
@coroutine
async def some_command(obj:RCContext) -> None:
    import json
    import random
    results = obj.rc.execute_command(
        command = Command (
            command_name = 'some-command',
            command_data = json.dumps({'wait_for': random.random()*2.}),
            controlled_name = "",
            controller_name = obj.rc.name,
            datetime = now_str()
        )
    )
    async for result in results:
        print(result)
    # asyncio.run(
    # )
