from collections.abc import Mapping

from druncschema.token_pb2 import Token

from drunc.broadcast.client.broadcast_handler import BroadcastHandler
from drunc.broadcast.client.configuration import BroadcastClientConfHandler
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.configuration import ConfTypes
from drunc.utils.shell_utils import (
    GRPCDriver,
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import get_logger, resolve_localhost_to_hostname


class ProcessManagerContext(ShellContext):  # boilerplatefest
    def __init__(self, *args, **kwargs):
        self.status_receiver = None
        super(ProcessManagerContext, self).__init__(*args, **kwargs)

    def reset(self, address: str = None):
        self.address = resolve_localhost_to_hostname(address)
        super(ProcessManagerContext, self)._reset(
            name="process_manager_context",
            token_args={},
            driver_args={},
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}
        return {
            "process_manager": ProcessManagerDriver(
                self.address,
                self._token,
                aio_channel=True,
            )
        }

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def start_listening(self, broadcaster_conf):
        bcch = BroadcastClientConfHandler(
            data=broadcaster_conf,
            type=ConfTypes.ProtobufAny,
        )
        self.status_receiver = BroadcastHandler(bcch)
        get_logger("process_manager.shell").info(
            f":ear: Listening to the Process Manager at {self.address}"
        )

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
