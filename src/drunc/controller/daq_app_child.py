import asyncio
import grpc
from typing import Optional
from drunc.controller.child_node import ChildNode, ChildNodeType
# from drunc.communication.controller_pb2_grpc import ControllerStub

class DAQAppChild(ChildNode):

    def __init__(self, config:dict):
        super().__init__(ChildNodeType.kDAQApplication, config['name'])
        # self.sender_uri = sender_uri
        # self.user = user
        self.cmd_address = config['cmd_address'] # TODO Multiplex
        self.status_address = status_address
        self.command_channel = grpc.aio.insecure_channel(self.cmd_address)
        # self.status_channel = grpc.aio.insecure_channel(self.status_address)
        self.controller_stub = ControllerStub(self.command_channel)
        self.status_stub = RetrieveStatusStub(self.command_channel)

    def propagate_command(self, command, data, token):
        self.log.info(f'Sending command {command} to {self.name}')
        #raise NotImplementedError('TODO')

    def close(self):
        self.log.info('Closing the connection')
        self.command_channel.close()

