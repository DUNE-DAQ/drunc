from collections.abc import Mapping
from enum import Enum

import grpc
from druncschema.process_manager_pb2 import ProcessQuery
from druncschema.token_pb2 import Token

from drunc.utils.grpc_utils import ServerTimeout, ServerUnreachable
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
        self.no_stop_error_batch_mode = False
        self.session_uses_local_connectivity_service: bool | None = None
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

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname

        token = create_dummy_token_from_uname()
        return token

    def get_endpoint_display_host_overrides(self) -> dict[str, str]:
        """
        Return a mapping of process name -> preferred display hostname for endpoint
        rendering in the UI.

        These values are cosmetic only. The controller's advertised endpoint remains
        the authoritative connect address.

        Returns:
            dict[str, str]: Mapping from process name to preferred display hostname.
        """
        # The PM driver may not be registered if the user connected directly to a
        # controller without going through the process manager (e.g. standalone boot).
        # In that case hostname overrides are unavailable and we fall back to
        # get_hostname_smart in the endpoint rendering path.
        pm_driver = self.get_driver("process_manager", quiet_fail=True)
        if not pm_driver:
            return {}

        if not self.session_name:
            raise RuntimeError("session name must be set before querying process list")
        query = ProcessQuery(names=[".*"], session=self.session_name)
        try:
            proc_list = pm_driver.ps(query)
        except (ServerUnreachable, ServerTimeout, grpc.RpcError):
            return {}

        overrides: dict[str, str] = {}

        for proc in proc_list.values:
            metadata = proc.process_description.metadata
            proc_name = getattr(metadata, "name", "")
            host_name = getattr(metadata, "hostname", "")

            if not proc_name or not host_name:
                continue

            overrides[proc_name] = host_name

        return overrides

    def terminate(self) -> None:
        if self.status_receiver_pm:
            self.status_receiver_pm.stop()
        if self.status_receiver_controller:
            self.status_receiver_controller.stop()
