from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import resolve_localhost_to_hostname


class ProcessManagerContext(ShellContext):  # boilerplatefest
    shell_id = "process_manager_shell"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super(ProcessManagerContext, self).__init__(*args, **kwargs)

    def reset(self, **kwargs: object) -> None:
        address = kwargs.get("address")
        resolved_address = address if isinstance(address, str) else ""
        self.address = resolve_localhost_to_hostname(resolved_address)
        super(ProcessManagerContext, self)._reset(
            name="process_manager_context",
            token_args={},
            driver_args={},
        )

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        del kwargs
        if not self.address:
            return {}
        return {
            "process_manager": ProcessManagerDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs: object) -> Token:
        del kwargs
        return create_dummy_token_from_uname()

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
