from collections.abc import Mapping

from druncschema.token_pb2 import Token

from drunc.controller.controller_driver import ControllerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import resolve_localhost_to_hostname


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

    def create_drivers(self, **kwargs) -> Mapping[str, object]:
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

    def get_endpoint_display_host_overrides(self) -> dict[str, str]:
        """
        Return display hostname overrides for status-table rendering.

        Returns an empty dict because this context connects directly to a
        controller without a process manager, so no per-process hostname
        metadata is available.

        Returns:
            dict[str, str]: Mapping of {process_name: hostname}.
                            Always empty for this context.
        """
        return {}

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
