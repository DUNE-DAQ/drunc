from collections.abc import MutableMapping

from drunc.controller.controller_driver import ControllerDriver
from drunc.utils.shell_utils import ShellContext


class ControllerContext(ShellContext):
    shell_id = "controller_shell"

    def __init__(self) -> None:
        self.took_control = False
        super(ControllerContext, self).__init__()

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {"controller": ControllerDriver(self.address, self._token)}
