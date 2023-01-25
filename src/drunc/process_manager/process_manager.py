import asyncio
import grpc
from drunc.communication.process_manager_pb2 import BootRequest, ProcessQuery, ProcessUUID, ProcessInstance, LogLine, LogRequest
from drunc.communication.process_manager_pb2_grpc import ProcessManagerServicer
import abc

class ProcessManager(abc.ABC, ProcessManagerServicer):

    def __init__(self):
        pass

    @abc.abstractmethod
    def boot(self, boot_request:BootRequest) -> ProcessUUID:
        pass

    @abc.abstractmethod
    def restart(self, query:ProcessQuery) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def is_alive(self, query:ProcessQuery) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def kill(self, query:ProcessQuery) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def killall(self, query:ProcessQuery) -> ProcessInstance:
        pass

    @abc.abstractmethod
    def logs(self, query:LogRequest) -> LogLine:
        pass

    @staticmethod
    def get(conf:dict):
        from rich.console import Console
        console = Console()

        if conf['type'] == 'ssh':
            console.print(f'Starting \'SSHProcessManager\'')
            from drunc.process_manager.ssh_process_manager import SSHProcessManager
            return SSHProcessManager(conf)
        else:
            raise RuntimeError(f'ProcessManager type {pm_conf_data["type"]} is unsupported!')

        
