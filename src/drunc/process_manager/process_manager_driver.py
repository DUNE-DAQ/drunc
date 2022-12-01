import asyncio

from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance, ProcessInstanceList, ProcessMetadata
from drunc.communication.process_manager_pb2_grpc import ProcessManagerStub


class ProcessManagerDriver:
    def __init__(self, pm_address:str):
        import grpc
        self.pm_address = pm_address
        self.pm_channel = grpc.aio.insecure_channel(self.pm_address)
        self.pm_stub = ProcessManagerStub(self.pm_channel)
        

    async def boot(self, boot_request:BootRequest) -> ProcessUUID:
        return await self.pm_stub.boot(boot_request)

    async def kill(self, uuid:ProcessUUID) -> ProcessInstance:
        return await self.pm_stub.kill(uuid)

    async def list_process(self, selector:ProcessMetadata) -> ProcessInstanceList:
        return await self.pm_stub.list_process(selector)
