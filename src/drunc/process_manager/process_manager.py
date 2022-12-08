import asyncio
import grpc
from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessInstance
from drunc.communication.process_manager_pb2_grpc import ProcessManagerServicer
import abc

class ProcessManager(abc.ABC, ProcessManagerServicer):

    def __init__(self):
        pass

    @abc.abstractmethod
    def boot(self, boot_request:BootRequest) -> ProcessUUID:
        pass

    @abc.abstractmethod
    def restart(self, uuid:ProcessUUID) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def is_alive(self, uuid:ProcessUUID) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def kill(self, uuid:ProcessUUID) -> ProcessInstance:
        pass
