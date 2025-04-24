from collections.abc import Mapping

from drunc_core.broadcast.client.broadcast_handler import BroadcastHandler
from drunc_core.broadcast.client.configuration import BroadcastClientConfHandler
from drunc_core.utils.configuration import ConfTypes
from drunc_core.utils.shell_utils import (
    GRPCDriver,
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc_messages.token_pb2 import Token

from drunc.controller.driver import ControllerDriver
from drunc.process_orchestrator.driver import (
    ProcessOrchestratorDriver,
)


class UnifiedShellContext(ShellContext):  # boilerplatefest
    def __init__(self):
        self.status_receiver_process_orchestrator = None
        self.status_receiver_controller = None
        self.took_control = False
        self.process_orchestrator_process = None
        self.address_process_orchestrator = ""
        self.address_controller = ""
        self.configuration_file = ""
        self.configuration_id = ""
        self.session_name = ""
        super(UnifiedShellContext, self).__init__()

    def reset(self, address_process_orchestrator: str = ""):
        self.address_process_orchestrator = address_process_orchestrator
        super(UnifiedShellContext, self)._reset(name="unified_shell")

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        ret = {}
        if self.address_process_orchestrator != "":
            ret["process_orchestrator"] = ProcessOrchestratorDriver(
                self.address_process_orchestrator,
                self._token,
                aio_channel=True,
            )
        if self.address_controller != "":
            ret["controller"] = ControllerDriver(
                self.address,
                self._token,
                aio_channel=False,
            )
        return ret

    def set_controller_driver(self, address_controller, **kwargs) -> None:
        self.address_controller = address_controller

        if address_controller is None:
            del self._drivers["controller"]
            return

        self._drivers["controller"] = ControllerDriver(
            self.address_controller,
            self._token,
            aio_channel=False,
        )

    def create_token(self, **kwargs) -> Token:
        token = create_dummy_token_from_uname()
        return token

    def start_listening_process_orchestrator(self, broadcaster_conf) -> None:
        bcch = BroadcastClientConfHandler(
            type=ConfTypes.ProtobufAny,
            data=broadcaster_conf,
        )
        self.status_receiver_process_orchestrator = BroadcastHandler(
            broadcast_configuration=bcch
        )

    def start_listening_controller(self, broadcaster_conf) -> None:
        bcch = BroadcastClientConfHandler(
            type=ConfTypes.ProtobufAny,
            data=broadcaster_conf,
        )
        self.status_receiver_controller = BroadcastHandler(broadcast_configuration=bcch)

    def terminate(self) -> None:
        if self.status_receiver_process_orchestrator:
            self.status_receiver_process_orchestrator.stop()
        if self.status_receiver_controller:
            self.status_receiver_controller.stop()
