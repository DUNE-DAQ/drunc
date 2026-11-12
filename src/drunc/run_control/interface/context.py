from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.shell_utils import ShellContext, create_dummy_token_from_uname


class RunControlContext(ShellContext):
    shell_id = "run_control_shell"

    def __init__(self, *args, **kwargs) -> None:
        super(RunControlContext, self).__init__(*args, **kwargs)

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
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
