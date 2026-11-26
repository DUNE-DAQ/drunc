from collections.abc import MutableMapping

from drunc.controller.controller_driver import ControllerDriver
from drunc.utils.shell_utils import ShellContext
from drunc.utils.utils import get_logger


class ControllerContext(ShellContext):
    shell_id = "controller_shell"

    def __init__(self) -> None:
        self.log = get_logger("controller.interface.ControllerContext")
        self.took_control = False
        super(ControllerContext, self).__init__()

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {"controller": ControllerDriver(self.address, self._token)}
