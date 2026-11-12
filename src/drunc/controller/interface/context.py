from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.controller.controller_driver import ControllerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)


class ControllerContext(ShellContext):
    shell_id = "controller_shell"

    def __init__(self) -> None:
        self.took_control = False
        super(ControllerContext, self).__init__()

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {"controller": ControllerDriver(self.address, self._token)}

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()
