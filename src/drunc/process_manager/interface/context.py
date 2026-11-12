from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)


class ProcessManagerContext(ShellContext):
    shell_id = "process_manager_shell"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super(ProcessManagerContext, self).__init__(*args, **kwargs)

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {
            "process_manager": ProcessManagerDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs: object) -> Token:
        """
        Create a dummy token for the user in the process manager context.

        This is a placeholder implementation and should be replaced with actual token
        creation logic as needed.
        """
        return create_dummy_token_from_uname()
