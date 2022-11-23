import asyncio
import grpc
from drunc.communication.command_pb2 import Command, CommandResponse
from drunc.communication.command_pb2_grpc import CommandProcessorServicer
from drunc.communication.child_channel import ChildChannel
from drunc.utils.utils import now_str
from typing import Optional
# class ChildrenCommander:
#     def __init__(self, port:int):
#         self.name = f'child{port}'
#         self.port = port

#     async def send_command(self, command:Command) -> CommandResponse:
#         '''
#         snip
#         '''
#         if True:
#             return CommandResponse(
#                 response_code = CommandResponse.DONE,
#                 response_text = 'DONE',
#                 command_name  = command.command_name,
#                 command_data  = command.command_data,
#                 controller_name = command.controller_name,
#                 controlled_name = self.name,
#                 datetime = now_str(),
#             )
#         return CommandResponse(
#             response_code = CommandResponse.FAILED,
#             response_text = 'something went wrong',
#             command_name  = command.command_name,
#             command_data  = command.command_data,
#             controller_name = command.controller_name,
#             controlled_name = self.name,
#             datetime = now_str(),
#         )


class Controller(CommandProcessorServicer):
    def __init__(self, name:str):
        super().__init__()
        self.name = name
        self.parent_ports = [] # type: list[int]
        self.children = {} # type: dict[str, ChildChannel]
        # self.children_commanders = [] # type: list[ChildrenController]
        # self.command_sender = ChildChannel()


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

    async def execute_command_one_child(self, command:Command, child) -> CommandResponse:
        async for i in child.send_command(command):
            yield i

    async def execute_command(self, command:Command, context: grpc.aio.ServicerContext=None) -> CommandResponse:
        print(command.command_name)

        yield CommandResponse(
            response_code = CommandResponse.ACK,
            response_text = 'ACK',
            command_name  = command.command_name,
            command_data  = command.command_data,
            controller_name = command.controller_name,
            controlled_name = self.name,
            datetime = now_str()
        )

        import time
        time.sleep(1)

        tasks = []
        if self.children:
            print('Propagating to children...')
            for childname, child in self.children.items():
                print(f'Children: {childname}')
                tasks += [self.execute_command_one_child(command, child)]
        
        for task in tasks:
            async for r in task:
                yield r
        
        yield CommandResponse(
            response_code = CommandResponse.DONE,
            response_text = 'DONE',
            command_name  = command.command_name,
            command_data  = command.command_data,
            controller_name = command.controller_name,
            controlled_name = self.name,
            datetime = now_str()
        )
