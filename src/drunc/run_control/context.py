from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import RunControlDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)

# from drunc.utils.utils import resolve_localhost_to_hostname


class RunControlContext(ShellContext):
    shell_id = "run_control_context"

    def __init__(self, *args, **kwargs):
        self.status_receiver = None
        super(RunControlContext, self).__init__(*args, **kwargs)

    def create_drivers(self, **kwargs) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {
            "run_control": RunControlDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def terminate(self):
        pass
