import asyncio
import grpc
from typing import Optional
# from drunc.communication.controller_pb2 import Command, CommandResponse
# from drunc.communication.controller_pb2_grpc import ControllerStub

class AppController():
    # def __init__(self, cmd_address:str, status_address:str):
    def __init__(self, config:dict):
        # self.sender_uri = sender_uri
        # self.user = user
        self.cmd_address = config['cmd_address'] # TODO Multiplex
        self.status_address = status_address
        self.command_channel = grpc.aio.insecure_channel(self.cmd_address)
        # self.status_channel = grpc.aio.insecure_channel(self.status_address)
        self.controller_stub = ControllerStub(self.command_channel)
        self.status_stub = RetrieveStatusStub(self.command_channel)

    def close(self):
        print('Closing the connection')
        self.command_channel.close()

