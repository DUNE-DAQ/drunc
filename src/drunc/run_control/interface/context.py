from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.shell_utils import ShellContext, create_dummy_token_from_uname
from drunc.utils.utils import resolve_localhost_to_hostname


class RunControlContext(ShellContext):
    shell_id = "run_control"

    def __init__(self, *args, **kwargs) -> None:
        self.status_receiver = None
        super(RunControlContext, self).__init__(*args, **kwargs)

    def reset(self, *args, **kwargs) -> None:
        address_raw = kwargs.get("address")
        if address_raw is None and args:
            address_raw = args[0]
        address = str(address_raw) if address_raw is not None else ""
        self.address = resolve_localhost_to_hostname(address)
        super(RunControlContext, self)._reset(
            name="run_control_context", token_args={}, driver_args={}
        )

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        if not self.address:
            return {}
        return {
            "run_control": RunControlDriver(
                self.address,
                self._token,
            )
        }

    def terminate(self) -> None:
        if self.status_receiver:
            self.status_receiver.stop()
