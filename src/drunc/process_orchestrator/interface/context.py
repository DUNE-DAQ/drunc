from collections.abc import Mapping

from drunc_core.broadcast.client.broadcast_handler import BroadcastHandler
from drunc_core.broadcast.client.configuration import BroadcastClientConfHandler
from drunc_core.utils.configuration import ConfTypes
from drunc_core.utils.shell_utils import (
    GRPCDriver,
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc_core.utils.utils import get_logger, resolve_localhost_to_hostname
from drunc_messages.token_pb2 import Token

from drunc.process_orchestrator.driver import (
    ProcessOrchestratorDriver,
)


class ProcessOrchestratorContext(ShellContext):  # boilerplatefest
    def __init__(self, *args, **kwargs):
        self.status_receiver = None
        super(ProcessOrchestratorContext, self).__init__(*args, **kwargs)

    def reset(self, address: str = None):
        self.address = resolve_localhost_to_hostname(address)
        super(ProcessOrchestratorContext, self)._reset(
            name="process_orchestrator_context",
            token_args={},
            driver_args={},
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}
        return {
            "process_orchestrator": ProcessOrchestratorDriver(
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
        get_logger("process_orchestrator.shell").info(
            f":ear: Listening to the Process Orchestrator at {self.address}"
        )

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
