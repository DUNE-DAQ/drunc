from drunc.utils.shell_utils import ShellContext, GRPCDriver
from druncschema.token_pb2 import Token
from typing import Mapping

class ControllerContext(ShellContext): # boilerplatefest
    status_receiver = None
    took_control = False

    def reset(self, address:str=None, print_traceback:bool=False):
        self.address = address
        super(ControllerContext, self)._reset(
            print_traceback = print_traceback,
            name = 'controller',
            token_args = {},
            driver_args = {},
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}

        from drunc.controller.controller_driver import ControllerDriver
        return {
            'controller': ControllerDriver(
                self.address,
                self._token
            )
        }

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening_controller(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.configuration_utils import ConfData, ConfTypes

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = ConfData(
                data = broadcaster_conf,
                type = ConfTypes.ProtobufAny
            )
        )

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()

