from drunc.communication.controller_pb2_grpc import BroadcastServicer
from drunc.communication.controller_pb2 import BroadcastMessage, GenericResponse, ResponseCode, Level
import grpc


class BroadcastReceiver(BroadcastServicer):
    def __init__(self, port) -> None:
        BroadcastServicer.__init__(self)
        self.ready = False

        from drunc.utils.utils import setup_fancy_logging
        self._log = setup_fancy_logging("Controller Status Receiver")
        self._address = f'[::]:{port}'
        self._log.debug('Broadcast receiver initialised')

    def stop(self)->None:
        self._server.stop(0)
        self._log.debug('Broadcast receiver stopped')

    def serve(self) -> None:
        from concurrent import futures
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        from drunc.communication.controller_pb2_grpc import add_BroadcastServicer_to_server
        add_BroadcastServicer_to_server(self, self._server)
        self._server.add_insecure_port(self._address)
        self._server.start()
        self.ready = True
        self._log.debug('Broadcast receiver server started')
        self._server.wait_for_termination()
        self.ready = False

    def handle_broadcast(self, bm: BroadcastMessage, context: grpc.aio.ServicerContext=None) -> GenericResponse:
        message = f'{bm.emitter}: {bm.payload}' if bm.emitter else bm.payload
        level = bm.level
        if   level == Level.DEBUG   : self._log.debug   (message)
        elif level == Level.INFO    : self._log.info    (message)
        elif level == Level.WARNING : self._log.warning (message)
        elif level == Level.ERROR   : self._log.error   (message)
        elif level == Level.CRITICAL: self._log.critical(message)
        else                        : self._log.info    (message)
        return GenericResponse(
            response_code = ResponseCode.DONE,
            response_text = 'OK'
        )