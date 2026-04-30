from drunc.process_manager.ssh_process_manager import SSHProcessManager
from drunc.processes.ssh_process_lifetime_manager_from_forked_process import (
    SSHProcessLifetimeManagerShellOnForkedProcess,
)


class SSHProcessManagerShell(SSHProcessManager):
    def __init__(self, configuration, **kwargs):
        super().__init__(
            configuration=configuration,
            LifetimeManagerClass=SSHProcessLifetimeManagerShellOnForkedProcess,
            **kwargs,
        )
