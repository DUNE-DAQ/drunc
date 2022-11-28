import sh

from drunc.process_manager.process_manager import ProcessManager


class SSHProcessManager(ProcessManager):
    def __init__(self):
        self.process_store = {} # dict[str, Process]
        pass

    def boot(self, boot_request:BootRequest) -> ProcessUUID:
        import uuid
        this_uuid = uuid.uuid4()
        self.process_store[uuid] = sh.ssh(
            boot_request.executable,
            _bg=True,
            _bg_exc=False,
            _new_session=True,
        )
        print(type(self.process_store[this_uuid]))
        return this_uuid

    def resurrect(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    def is_alive(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    def kill(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
        
    def poll(self, uuid:ProcessUUID) -> ProcessStatus:
        pass
    
