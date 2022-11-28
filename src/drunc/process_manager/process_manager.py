import asyncio
import grpc
from drunc.communication.process_manager_pb2 import BootRequest, ProcessUUID, ProcessStatus
from drunc.communication.process_manager_pb2_grpc import ProcessManagerServicer
from abc import ABC

class ProcessManager(ABC, ProcessManagerServicer):

    def __init__(self):
        pass


    @abstractmethod
    def boot(self, boot_request:BootRequest) -> ProcessUUID:
        pass
        

    @abstractmethod
    def resurrect(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    @abstractmethod
    def is_alive(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    @abstractmethod
    def kill(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    @abstractmethod
    def poll(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
    
    
        
