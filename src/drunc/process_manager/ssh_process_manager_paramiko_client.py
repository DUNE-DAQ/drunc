from typing import cast

from drunc.process_manager.configuration import ProcessManagerConfHandler
from drunc.process_manager.ssh_process_manager import SSHProcessManager
from drunc.processes.ssh_process_lifetime_manager import ProcessLifetimeManager
from drunc.processes.ssh_process_lifetime_manager_paramiko import (
    SSHProcessLifetimeManagerParamiko,
)


class SSHProcessManagerParamikoClient(SSHProcessManager):
    def __init__(
        self,
        configuration: ProcessManagerConfHandler,
        name: str = "process_manager",
        **kwargs: object,
    ) -> None:
        super().__init__(
            configuration=configuration,
            LifetimeManagerClass=cast(
                type[ProcessLifetimeManager],
                SSHProcessLifetimeManagerParamiko,
            ),
            name=name,
            **kwargs,
        )
