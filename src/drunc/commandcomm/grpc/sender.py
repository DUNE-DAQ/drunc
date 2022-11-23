import asyncio

from typing import Optional

import drunc.commandcomm.grpc.command_pb2
import drunc.commandcomm.grpc.command_pb2_grpc

class GRPCCommandSender:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def send(message, address:str) -> None:
        async with grpc.aio.insecure_channel(address) as channel:
            stub = command_pb2_grpc.CommandReceiverStub(channel)
            response = await stub.ExecuteCommand(command_pb2.CommandMessage(name='you'))
    
        print("Greeter client received: " + response.message)
        
        
