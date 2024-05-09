from drunc.utils.shell_utils import ShellContext, GRPCDriver, add_traceback_flag
from druncschema.token_pb2 import Token
from typing import Mapping

class UnifiedShellContext(ShellContext): # boilerplatefest
    status_receiver_pm = None
    status_receiver_controller = None
    took_control = False
    pm_process = None
    address_pm = ''
    address_controller = ''

    def reset(self, address_pm:str='', print_traceback:bool=False):
        self.address_pm = address_pm
        super(UnifiedShellContext, self)._reset(
            print_traceback = print_traceback,
            name = 'unified',
            token_args = {},
            driver_args = {
                'print_traceback': print_traceback
            },
        )

    def create_drivers(self, print_traceback, **kwargs) -> Mapping[str, GRPCDriver]:
        ret = {}

        if self.address_pm != '':
            from drunc.process_manager.process_manager_driver import ProcessManagerDriver
            ret['process_manager'] = ProcessManagerDriver(
                self.address_pm,
                self._token,
                aio_channel = True,
                rethrow_by_default = print_traceback
            )

        if self.address_controller != '':
            from drunc.controller.controller_driver import ControllerDriver
            ret['controller'] = ControllerDriver(
                self.address,
                self._token,
                aio_channel = False,
                rethrow_by_default = print_traceback
            )

        return ret

    def set_controller_driver(self, address_controller, print_traceback, **kwargs) -> None:
        self.address_controller = address_controller

        from drunc.controller.controller_driver import ControllerDriver

        self._drivers['controller'] = ControllerDriver(
            self.address_controller,
            self._token,
            aio_channel = False,
            rethrow_by_default = print_traceback
        )


    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening_pm(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler
        from drunc.utils.configuration import ConfTypes
        bcch = BroadcastClientConfHandler(
            type = ConfTypes.ProtobufAny,
            data = broadcaster_conf,
        )

        self.status_receiver_pm = BroadcastHandler(
            broadcast_configuration = bcch
        )

    def start_listening_controller(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler
        from drunc.utils.configuration import ConfTypes
        bcch = BroadcastClientConfHandler(
            type = ConfTypes.ProtobufAny,
            data = broadcaster_conf,
        )
        self.status_receiver_controller = BroadcastHandler(
            broadcast_configuration = bcch
        )

    def terminate(self):
        if self.status_receiver_pm:
            self.status_receiver_pm.stop()

        if self.status_receiver_controller:
            self.status_receiver_controller.stop()

