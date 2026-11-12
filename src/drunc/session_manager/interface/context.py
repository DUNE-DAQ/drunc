from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager_driver import SessionManagerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)


class SessionManagerContext(ShellContext):
    shell_id = "session_manager_shell"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_drivers(self, **kwargs) -> dict[str, object]:
        if not self.address:
            return {}
        return {
            "session_manager": SessionManagerDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()
