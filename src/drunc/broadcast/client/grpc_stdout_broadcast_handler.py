from druncschema.broadcast_pb2_grpc import BroadcastReceiverServicer
from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
from druncschema.generic_pb2 import Empty
import grpc
from drunc.utils.configuration_utils import ConfTypes, ConfData


class gRPCStdoutBroadcastHandler(BroadcastReceiverServicer):
    def __init__(self, conf:ConfData, token, **kwargs) -> None:
        super(gRPCStdoutBroadcastHandler, self).__init__(
            **kwargs
        )
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('gRPCStdoutBroadcastHandler is not handled it needs to be reworked!')
        self.ready = False
        self.stub = None
        self.token = token
        from logging import getLogger
        self._log = getLogger("BroadcastReceiver")
        self._address = f'[::]:{port}'
        self._log.debug('Broadcast receiver initialised')

    def stop_receiving(self)->None:
        self._server.stop(0)
        self._log.debug('Broadcast receiver stopped')

    def connect(self) -> None:
        from drunc.utils.grpc_utils import send_command
        from druncschema.broadcast_pb2 import BroadcastRequest
        self._log.info(f'Connecting to {self.stub}')
        try:
            send_command(
                controller = self.stub,
                token = self.token,
                command = 'add_to_broadcast_list',
                data = BroadcastRequest(broadcast_receiver_address = self._address),
                rethrow = True
            )

        except Exception as e:
            self._log.error('Could not connect to service to receive broadcast')
            self._log.error(str(e))
            raise e

    def disconnect(self) -> None:
        from drunc.utils.grpc_utils import send_command
        from druncschema.broadcast_receiver_pb2 import BroadcastRequest

        try:
            send_command(
                controller = self.stub,
                token = self.token,
                command = 'remove_from_broadcast_list',
                data = BroadcastRequest(broadcast_receiver_address = self._address),
                rethrow = True
            )

        except Exception as e:
            self._log.error('Could not disconnect from broadcaster (maybe it\' dead and you won\'t receive any broadcast anyway)')
            self._log.error(str(e))

    def terminate(self) -> None:
        self.disconnect()
        self.stop_receiving()

    def serve(self) -> None:
        from concurrent import futures
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        from druncschema.broadcast_pb2_grpc import add_BroadcastReceiverServicer_to_server
        add_BroadcastReceiverServicer_to_server(self, self._server)

        self._server.add_insecure_port(self._address)

        self._server.start()
        self.ready = True
        self._log.debug('Broadcast receiver server started')
        self._server.wait_for_termination()
        self.ready = False

    def handle_broadcast(self, bm: BroadcastMessage, context: grpc.aio.ServicerContext=None) -> Empty:
        from druncschema.generic_pb2 import PlainText
        from drunc.utils.grpc_utils import unpack_any

        type = bm.type
        if type == BroadcastType.TEXT_MESSAGE:
            pt = unpack_any(bm.data, PlainText)
            self._log.info(f'{bm.emitter}: {pt}')
        elif type == BroadcastType.ACK:
            self._log.info(f'{bm.emitter}: Ack')
        elif type == BroadcastType.SERVER_SHUTDOWN:
            self._log.info(f'{bm.emitter} is shutting down')

        return Empty()