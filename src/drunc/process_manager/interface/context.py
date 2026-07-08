from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.utils.shell_utils import (
    ShellContext,
    create_dummy_token_from_uname,
)
from drunc.utils.utils import resolve_localhost_to_hostname


class ProcessManagerContext(ShellContext):
    def __init__(self, *args, **kwargs):
        self.status_receiver = None
        super(ProcessManagerContext, self).__init__(*args, **kwargs)

    def reset(self, address: str = "", **kwargs):
        self.address = resolve_localhost_to_hostname(address)
        super(ProcessManagerContext, self)._reset(
            name="process_manager_context",
            token_args={},
            driver_args={},
        )

    def create_drivers(self, **kwargs) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {
            "process_manager": ProcessManagerDriver(
                self.address,
                self._token,
            )
        }

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
