from collections.abc import MutableMapping

from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.shell_utils import ShellContext


class ProcessManagerContext(ShellContext):
    shell_id = "process_manager_shell"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super(ProcessManagerContext, self).__init__(*args, **kwargs)

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {
            "process_manager": ProcessManagerDriver(
                self.address,
                self._token,
            )
        }
