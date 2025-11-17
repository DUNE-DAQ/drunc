from drunc.process_manager.ssh_process_manager import SSHProcessManager
from drunc.processes.ssh_process_lifetime_manager_shell import (
    SSHProcessLifetimeManagerShell,
)


class SSHProcessManagerShell(SSHProcessManager):
    def __init__(self, configuration, **kwargs):
        super().__init__(
            configuration=configuration,
            class_lifetime_manager=SSHProcessLifetimeManagerShell,
            **kwargs,
        )
