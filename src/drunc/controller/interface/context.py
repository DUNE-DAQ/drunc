from collections.abc import Mapping

from drunc_core.broadcast.client.broadcast_handler import BroadcastHandler
from drunc_core.broadcast.client.configuration import BroadcastClientConfHandler
from drunc_core.utils.configuration import ConfTypes
from drunc_core.utils.shell_utils import (
    GRPCDriver,
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc_core.utils.utils import resolve_localhost_to_hostname
from drunc_messages.token_pb2 import Token

from drunc.controller.controller_driver import ControllerDriver


class ControllerContext(ShellContext):  # boilerplatefest
    def __init__(self):
        self.status_receiver = None
        self.took_control = False
        super(ControllerContext, self).__init__()

    def reset(self, address: str = None):
        self.address = resolve_localhost_to_hostname(address)
        super(ControllerContext, self)._reset(
            name="controller_context", token_args={}, driver_args={}
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}
        return {"controller": ControllerDriver(self.address, self._token)}

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def start_listening_controller(self, broadcaster_conf):
        bcch = BroadcastClientConfHandler(
            data=broadcaster_conf, type=ConfTypes.ProtobufAny
        )
        self.status_receiver = BroadcastHandler(broadcast_configuration=bcch)

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
