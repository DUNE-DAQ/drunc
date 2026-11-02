from collections.abc import MutableMapping

from druncschema.token_pb2 import Token

from drunc.run_control.run_control_driver import RunControlDriver
from drunc.utils.shell_utils import ShellContext, create_dummy_token_from_uname
from drunc.utils.utils import resolve_localhost_to_hostname, get_logger


class RunControlContext(ShellContext):
    shell_id = "run_control_shell"

    def __init__(self, *args, **kwargs) -> None:
        self.status_receiver = None
        self.log = None
        self.address_rc = None
        self.address_pm = None
        self.log_debug = get_logger("etestg", rich_handler=True)
        super(RunControlContext, self).__init__(*args, **kwargs)

    def reset(self, *args, **kwargs) -> None:
        self.log_debug.info("in here")

        def get_reset_address(address_key: str):
            address_raw = kwargs[address_key]
            if address_raw is None:
                return None
            address = str(address_raw) if address_raw is not None else ""
            return resolve_localhost_to_hostname(address)

        # these are the possibe keys. Will need to check for the existence of this and then only apply it to the relevant ones
        address_keys = ("address_rc", "address_pm")
        for address_key in address_keys:
            if address_key in kwargs:
                setattr(self, address_key, get_reset_address(address_key))

        # you'll have to check the address again!
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

        if self.address_pm:
            from drunc.process_manager.process_manager_driver import (
                ProcessManagerDriver,
            )

            ret["process_manager"] = ProcessManagerDriver(
                self.address_pm,
                self._token,
            )

        # This will come later
        # if self.address_controller != "":
        #     from drunc.controller.controller_driver import ControllerDriver

        #     ret["controller"] = ControllerDriver(
        #         self.address_controller,
        #         self._token,
        #     )
        return ret

    def terminate(self) -> None:
        if self.status_receiver:
            self.status_receiver.stop()
