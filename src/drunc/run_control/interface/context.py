from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import get_logger, get_root_logger, resolve_localhost_to_hostname


class RunControlContext(ShellContext):
    shell_id = "run_control_context"

    def __init__(self, *args, **kwargs):
        get_root_logger("INFO")
        self.log = get_logger("run_control.context", rich_handler=True)
        self.log.debug("Initializing RunControlContext")
        super(RunControlContext, self).__init__(*args, **kwargs)

    def reset(self, address: str = "", **kwargs):
        self.log.debug("Resetting RunControlContext with address: %s", address)
        self.address = resolve_localhost_to_hostname(address)
        self.log.debug("Resolved address: %s", self.address)
        super(RunControlContext, self)._reset(
            name="run_control_context",
            token_args={},
            driver_args={},
        )
        self.log.debug("RunControlContext reset complete")

    def create_drivers(self, **kwargs) -> MutableMapping[str, object]:
        self.log.debug(
            "Creating drivers for RunControlContext with address: %s", self.address
        )
        if not self.address:
            return {}
        return {
            "run_control": RunControlDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs) -> Token:
        self.log.debug("Creating token for RunControlContext")
        return create_dummy_token_from_uname()

    def terminate(self):
        self.log.debug("Terminating RunControlContext")
        pass
