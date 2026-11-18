from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.controller.controller_driver import ControllerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import get_logger, resolve_localhost_to_hostname


class ControllerContext(ShellContext):  # boilerplatefest
    shell_id = "controller_shell"

    def __init__(self) -> None:
        self.log = get_logger("controller.iface", rich_handler=True)
        self.status_receiver = None
        self.took_control = False
        super(ControllerContext, self).__init__()

    def reset(self, *args: object, **kwargs: object) -> None:
        address_raw = kwargs.get("address")
        if address_raw is None and args:
            address_raw = args[0]
        address = str(address_raw) if address_raw is not None else ""
        self.address = resolve_localhost_to_hostname(address)
        super(ControllerContext, self)._reset(
            name="controller_context", token_args={}, driver_args={}
        )

    def set_controller_driver(self, address: str) -> None:
        self.address = address
        self.set_driver("controller", ControllerDriver(address, self._token))

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {"controller": ControllerDriver(self.address, self._token)}

    def create_token(self, **kwargs: object) -> Token:
        return create_dummy_token_from_uname()

    def terminate(self) -> None:
        if self.status_receiver:
            self.status_receiver.stop()
