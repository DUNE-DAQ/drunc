import asyncio
import grpc
from typing import Optional
from drunc.communication.command_pb2 import Command, CommandResponse
from drunc.communication.command_pb2_grpc import CommandProcessorStub


class ChildChannel():
    # def __init__(self, cmd_address:str, status_address:str):
    def __init__(self, cmd_address:str):
        # self.sender_uri = sender_uri
        # self.user = user
        self.cmd_address = cmd_address # TODO Multiplex
        # self.status_address = status_address
        self.command_channel = grpc.aio.insecure_channel(self.cmd_address)
        # self.status_channel = grpc.aio.insecure_channel(self.status_address)
        self.command_stub = CommandProcessorStub(self.command_channel)
        # self.status_stub = RetrieveStatusStub(self.command_channel)
        
    def close(self):
        print('Closing the connection')
        self.command_channel.close()
        
    async def send_command(self, command:Command) -> CommandResponse:
        print(f'Sending_command {command.command_name}, state of the channel {self.command_channel.get_state()}')

        async for response in self.command_stub.execute_command(command):
            yield response

