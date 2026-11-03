from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.shell_utils import ShellContext, create_dummy_token_from_uname
from drunc.utils.utils import get_logger, resolve_localhost_to_hostname


class RunControlContext(ShellContext):
    shell_id = "run_control_shell"

    def __init__(self, *args, **kwargs) -> None:
        self.status_receiver = None
        self.log = None
        self.address_rc = None
        self.log_debug = get_logger("etestg", rich_handler=True)
        super(RunControlContext, self).__init__(*args, **kwargs)

    def reset(self, *args, **kwargs) -> None:
        if "address_rc" in kwargs:
            address = kwargs["address_rc"]
            self.address_rc = (
                resolve_localhost_to_hostname(str(address))
                if address is not None
                else None
            )
        super(RunControlContext, self)._reset(
            name="run_control_context", token_args={}, driver_args={}
        )

    def create_token(self, **kwargs) -> Token:
        return create_dummy_token_from_uname()

    def create_drivers(self, **kwargs: object) -> MutableMapping[str, object]:
        ret: MutableMapping[str, object] = {}
        if self.address_rc:
            ret["run_control"] = RunControlDriver(
                self.address_rc,
                self._token,
            )
        return ret

    def terminate(self) -> None:
        if self.status_receiver:
            self.status_receiver.stop()
