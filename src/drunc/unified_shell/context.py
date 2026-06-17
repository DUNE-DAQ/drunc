from collections.abc import Mapping
from enum import Enum

from druncschema.token_pb2 import Token

from drunc.utils.shell_utils import ShellContext


class UnifiedShellMode(Enum):
    INTERACTIVE = "interactive"
    BATCH = "batch"
    SEMIBATCH = "semibatch"


class UnifiedShellContext(ShellContext):  # boilerplatefest
    def __init__(self):
        self.log = None
        self.status_receiver_pm = None
        self.status_receiver_controller = None
        self.took_control = False
        self.pm_process = None
        self.address_pm = ""
        self.address_controller = ""
        self.configuration_file = ""
        self.configuration_id = ""
        self.session_name = ""
        self.override_logs = True
        self.running_mode = UnifiedShellMode.INTERACTIVE
        self.batch_commands: list(str) = []
        super(UnifiedShellContext, self).__init__()

    def reset(self, address_pm: str = ""):
        self.address_pm = address_pm
        super(UnifiedShellContext, self)._reset(name="unified_shell")

    def create_drivers(self, **kwargs) -> Mapping[str, object]:
        ret = {}
        if self.address_pm != "":
            from drunc.process_manager.process_manager_driver import (
                ProcessManagerDriver,
            )

            ret["process_manager"] = ProcessManagerDriver(
                self.address_pm,
                self._token,
            )
        if self.address_controller != "":
            from drunc.controller.controller_driver import ControllerDriver

            ret["controller"] = ControllerDriver(
                self.address,
                self._token,
            )
        return ret

    def set_controller_driver(self, address_controller, **kwargs) -> None:
        self.address_controller = address_controller
        from drunc.controller.controller_driver import ControllerDriver

        if address_controller is None:
            del self._drivers["controller"]
            return

        self._drivers["controller"] = ControllerDriver(
            self.address_controller,
            self._token,
        )

        # This will raise an exception if the driver already exists
        # self.set_driver("controller", driver)

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname

        token = create_dummy_token_from_uname()
        return token

    def start_listening_pm(self, broadcaster_conf) -> None:
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler

        bcch = BroadcastClientConfHandler.from_pbany(data=broadcaster_conf)
        self.status_receiver_pm = BroadcastHandler(broadcast_configuration=bcch)

    def start_listening_controller(self, broadcaster_conf) -> None:
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler

        bcch = BroadcastClientConfHandler.from_pbany(data=broadcaster_conf)
        self.status_receiver_controller = BroadcastHandler(broadcast_configuration=bcch)

    def terminate(self) -> None:
        if self.status_receiver_pm:
            self.status_receiver_pm.stop()
        if self.status_receiver_controller:
            self.status_receiver_controller.stop()
