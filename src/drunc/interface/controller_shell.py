import click
import click_shell
from drunc.controller.controller import Controller
from drunc.communication.controller_pb2 import Command
from drunc.utils.utils import now_str
import asyncio
from functools import wraps

def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

class RCContext:
    def __init__(self, rc:Controller, name:str) -> None:
        self.rc = rc
        self.name = name

@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('configuration', type=str)
@click.pass_obj
def rc_shell(obj:RCContext, configuration:str) -> None:
    from drunc.controller.controller import Controller
    ctrlr = Controller(obj.name, configuration)
    obj.rc = ctrlr

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
