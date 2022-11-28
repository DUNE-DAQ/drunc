import asyncio
import grpc
from drunc.communication.command_pb2 import Command, CommandResponse
from drunc.communication.command_pb2_grpc import CommandProcessorServicer
from drunc.communication.child_channel import ChildChannel
from drunc.utils.utils import now_str
from typing import Optional
import aiostream

class Controller(CommandProcessorServicer):
    def __init__(self, name:str):
        super().__init__()
        self.name = name
        self.parent_ports = [] # type: list[int]
        self.children = {} # type: dict[str, ChildChannel]

    def wait_for_commands(self) -> None:
        pass

    def add_spectator(self, port:int) -> None:
        pass

    def add_controlled_children(self, name:str, address:str) -> None:
        if name in self.children:
            raise RuntimeError(f'Child {name} already exists!')
        self.children[name] = ChildChannel(address)

    def rm_controlled_children(self, name:str) -> None:
        if name not in self.children:
            raise RuntimeError(f'Child {name} doesn\'t exists!')
        self.children[name].close()
        del self.children[name]

    # @aiostream.core.operator
    async def execute_command_one_child(self, command:Command, child) -> list[CommandResponse]:
        # ret = []
        return child.send_command(command)
            # yield i
        #     ret += [i]
        # return ret


    async def execute_command(self, command:Command, context: grpc.aio.ServicerContext=None) -> CommandResponse:
        print(f'{self.name} executing {command}')

        yield CommandResponse(
            response_code = CommandResponse.ACK,
            response_text = 'ACK',
            command_name  = command.command_name,
            command_data  = command.command_data,
            controller_name = command.controller_name,
            controlled_name = self.name,
            datetime = now_str()
        )

        # import time
        import random
        import json
        await asyncio.sleep(json.loads(command.command_data)['wait_for'])

        if self.children:
            tasks = []

            print('Propagating to children...')
            commands_data = {
                'child1': Command(
                    command_name = 'some-command-for-child1',
                    command_data = json.dumps({'wait_for': 4}),
                    controlled_name = "",
                    controller_name = self.name,
                    datetime = now_str()
                ),
                'child2': Command(
                    command_name = 'some-command-for-child2',
                    command_data = json.dumps({'wait_for': 5}),
                    controlled_name = "",
                    controller_name = self.name,
                    datetime = now_str()
                ),
            }
            print(commands_data.values())
            from aiostream import stream

            child_command_stream = stream.combine.merge( # BOOOH! this combines the async generators, pretty sweet
                *[child.send_command(commands_data[name]) for name, child in self.children.items()]
            )
            print('start streaming')
            async with child_command_stream.stream() as streamer:
                async for s in streamer:
                    yield s

        yield CommandResponse(
            response_code = CommandResponse.DONE,
            response_text = 'DONE',
            command_name  = command.command_name,
            command_data  = command.command_data,
            controller_name = command.controller_name,
            controlled_name = self.name,
            datetime = now_str()
        )
