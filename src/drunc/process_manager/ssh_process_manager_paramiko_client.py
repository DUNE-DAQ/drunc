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
            # SSHProcessLifetimeManagerParamiko does not implement the full
            # ProcessLifetimeManager interface, so mypy
            # rejects it as "abstract" here without this cast.
            # Since it's not in active development the mypy check is bypassed,
            # if paramiko is picked up again the cast should be removed.
            LifetimeManagerClass=cast(
                type[ProcessLifetimeManager],
                SSHProcessLifetimeManagerParamiko,
            ),
            name=name,
            **kwargs,
        )
