import asyncio
import grpc
from drunc.communication.command_pb2 import Command, CommandResponse, Ping
from drunc.communication.command_pb2_grpc import CommandProcessorServicer, PingProcessorServicer
from drunc.communication.child_channel import ChildChannel
from drunc.utils.utils import now_str
from typing import Optional
import aiostream

class Controller(CommandProcessorServicer, PingProcessorServicer):
    def __init__(self, name:str):
        super().__init__()
        self.name = name
        self.parent_ports = [] # type: list[int]
        self.children = {} # type: dict[str, ChildChannel]
        self.pinging = True

        # asyncio.create_task(self.ping_children())

    def __del__(self) -> None:
        self.pinging = False

    def wait_for_commands(self) -> None:
        pass

    async def ping_thread(self) -> None:
        while self.pinging:
            p = await self.ping_children()
            await asyncio.sleep(0.5)

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

    async def ping_children(self) -> None:
        # yield Ping(
        #     controller_name = ping.controller_name,
        #     controlled_name = self.name,
        #     datetime = now_str(),
        #     # propagate = False
        # )
        # if ping.propagate:
        from aiostream import stream

        child_ping_stream = stream.combine.merge( # BOOOH! this combines the async generators, pretty sweet
            *[
                child.ping(
                    Ping(
                        controller_name = self.name,
                        controlled_name = name,
                        datetime = now_str(),
                    )
                ) for name, child in self.children.items()
            ]
        )
        async with child_ping_stream.stream() as streamer:
            async for s in streamer:
                yield s

    async def ping(self, ping:Ping) -> Ping:
        yield Ping(
            controller_name = ping.controller_name,
            controlled_name = self.name,
            datetime = now_str(),
        )

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
