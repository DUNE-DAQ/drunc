from typing import Mapping

from druncschema.token_pb2 import Token
from drunc.utils.shell_utils import ShellContext, GRPCDriver

class ProcessManagerContext(ShellContext): # boilerplatefest
    status_receiver = None

    def reset(self, address:str=None):
        self.address = address
        super(ProcessManagerContext, self)._reset(
            name = 'process_manager',
            token_args = {},
            driver_args = {},
        )

    def create_drivers(self, **kwargs) -> Mapping[str, GRPCDriver]:
        if not self.address:
            return {}

        from drunc.process_manager.process_manager_driver import ProcessManagerDriver

        return {
            'process_manager': ProcessManagerDriver(
                self.address,
                self._token,
                aio_channel = True,
            )
        }

    def create_token(self, **kwargs) -> Token:
        from drunc.utils.shell_utils import create_dummy_token_from_uname
        return create_dummy_token_from_uname()


    def start_listening(self, broadcaster_conf):
        from drunc.broadcast.client.broadcast_handler import BroadcastHandler
        from drunc.broadcast.client.configuration import BroadcastClientConfHandler
        from drunc.utils.configuration import ConfTypes
        bcch = BroadcastClientConfHandler(
            data = broadcaster_conf,
            type = ConfTypes.ProtobufAny,
        )

        self._log.debug(f'Broadcaster configuration:\n{broadcaster_conf}')

        self.status_receiver = BroadcastHandler(bcch)

        from rich import print as rprint
        rprint(f':ear: Listening to the Process Manager at {self.address}')

    def terminate(self):
        if self.status_receiver:
            self.status_receiver.stop()
