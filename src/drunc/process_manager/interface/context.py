from typing import Mapping

from druncschema.token_pb2 import Token
from drunc.utils.shell_utils import ShellContext, GRPCDriver

class ProcessManagerContext(ShellContext): # boilerplatefest
    status_receiver = None

    def reset(self, address:str=None, print_traceback:bool=False):
        self.address = address
        super(ProcessManagerContext, self)._reset(
            print_traceback = print_traceback,
            name = 'process_manager',
            token_args = {},
            driver_args = {
                'print_traceback': print_traceback
            },
        )

    def create_drivers(self, print_traceback, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}

        from drunc.process_manager.process_manager_driver import ProcessManagerDriver

        return {
            'process_manager': ProcessManagerDriver(
                self.address,
                self._token,
                aio_channel = True,
                rethrow_by_default = print_traceback
            )
        }

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.utils.configuration import ConfTypes

        self.status_receiver = BroadcastHandler(
            broadcast_configuration = broadcaster_conf,
            conf_type = ConfTypes.Protobuf
        )
        from rich import print as rprint
        rprint(f':ear: Listening to the Process Manager at {self.address}')

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
