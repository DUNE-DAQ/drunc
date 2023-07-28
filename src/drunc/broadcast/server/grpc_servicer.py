import grpc
from threading import Thread, Lock
from queue import Queue
from druncschema.broadcast_pb2_grpc import BroadcastSenderServicer
from druncschema.broadcast_pb2 import BroadcastRequest
from druncschema.request_response_pb2 import Request, Response
from druncschema.generic_pb2 import PlainText
from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
from druncschema.authoriser_pb2 import ActionType

class ListenerRepresentation:

    def __init__(self, configuration):
        self.address = configuration['address']
        self.channel = grpc.insecure_channel(address)
        from druncschema.broadcast_pb2_grpc import BroadcastReceiverStub
        self.stub = BroadcastReceiverStub(self.channel)

    def handle_broadcast(self, message):
        return self.stub.handle_broadcast(message)


class GRCPBroadcastSender(BroadcastSenderServicer):

    def __init__(self, configuration):
        from logging import getLogger
        self.name = 'broadcast_sender'
        self._log = getLogger("Broadcast Sender")
        self._listeners = {}
        self._listener_lock = Lock()
        self._message_queue = Queue()
        self._consumer_thread = Thread(target=self._consumer, name='broadcast_consumer')
        self._consumer_thread.start()
        self._log.info('Broadcaster started')

    def _send(self, bm:BroadcastMessage):
        pass


    def get_listeners(self):
        import copy as cp
        with self._listener_lock:
            ret = cp.deepcopy(list(self._listeners.keys()))
            return ret

    def broadcast(self, txt, type=BroadcastType.TEXT_MESSAGE):
        from druncschema.broadcast_pb2 import BroadcastMessage
        from google.protobuf import any_pb2
        message = PlainText(text=txt)
        data_detail = any_pb2.Any()
        data_detail.Pack(message)

        bm = BroadcastMessage(
            emitter = self.name,
            type = BroadcastType.TEXT_MESSAGE,
            data = data_detail
        )

        return self._message_queue.put(bm)

    def broadcast_exception(self, exception):
        from druncschema.broadcast_pb2 import BroadcastMessage
        from google.protobuf import any_pb2
        message = PlainText(text=str(exception))
        data_detail = any_pb2.Any()
        data_detail.Pack(message)

        bm = BroadcastMessage(
            emitter = self.name,
            type = BroadcastType.EXCEPTION_RAISED,
            data = data_detail
        )


    def add_to_bl_logic(self, request:BroadcastRequest):
        self.add_listener(request.broadcast_receiver_address)

    def execute_command(self, request, format, logic, action):
        uname = request.token.user_name
        if self.authoriser:
            if not self.authoriser.is_authorised(request.token, action):
                from drunc.authoriser.exceptions import Unauthorised
                raise Unauthorised(uname, action)
        from drunc.utils.grpc_utils import unpack_any

        data = unpack_any(request.data, format)

        self.broadcast(f'Executing {action} (user: {uname})', BroadcastType.COMMAND_EXECUTION_START)
        ret = logic(data)
        self.broadcast(f'Finshed executing {action} (user: {uname})', BroadcastType.COMMAND_EXECUTION_SUCCESS)

        response = Response()
        response.token.CopyFrom(request.token)
        data = Any()
        if ret:
            data.Pack(ret)
            response.data.CopyFrom(data)

        return response

    def add_to_broadcast_list(self, request:Request, context) -> Response:
        from drunc.utils.command import execute_command
        try:
            return self.execute_command(
                request = request,
                format = BroadcastRequest,
                logic = self.add_to_bl_logic,
                action = ActionType.CREATE
            )
        except Exception as e:
            self.broadcast_exception(e)
            from drunc.utils.command import parse_exception
            return parse_exception(e)

    def remove_from_broadcast_list(self, request:Request, context) -> Response:
        r = unpack_any(data, BroadcastRequest)
        if not self.broadcaster.rm_listener(r.broadcast_receiver_address):
            raise ctler_excpt.ControllerException(f'Failed to remove {r.broadcast_receiver_address} from broadcast list')
        return PlainText(text = f'Removed {r.broadcast_receiver_address} to broadcast list')



    def get_broadcast_list(self, request:Request, context) -> Response:
        #return self._generic_user_command(request, '_get_broadcast_list', context)
        ret = StringStringMap()
        listeners = self.broadcaster.get_listeners()
        for k, v in listeners.items():
            ret[k] = v
        return


    def ack(self, address):
        if address not in self._listeners:
            raise RuntimeError(f'Cannot send ack to {address}')

        stub = self._listeners[address]
        self._log.debug(f'Ack to {address}')

        from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
        message = BroadcastMessage(
            emitter = self.name,
            type = BroadcastType.ACK
        )

        try:
            stub.handle_broadcast(message)
        except Exception as e:
            self._log.error(f'Could not Ack to {address}: {str(e)}')

    def shutdown(self):
        from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
        bm = BroadcastMessage(
            emitter = self.name,
            type = BroadcastType.SERVER_SHUTDOWN
        )

        return self._message_queue.put(bm)

    def add_listener(self, address):
        with self._listener_lock:
            if address in self._listeners.keys():
                self._log.error(f'Listener {address} already exists')
                self._listener_lock.release()
                return False
            self._log.info(f'Adding listener {address}')
            self._listeners[address] = ListenerRepresentation(address)
            self.ack(address)

        return True


    def rm_listener(self, address):
        with self._listener_lock:
            if address not in self._listeners.keys():
                self._log.error(f'Listener {address} does not exist')
                self._listener_lock.release()
                return False
            self._log.info(f'Removing listener {address}')
            del self._listeners[address]
        return True

    def join(self):
        return self._consumer_thread.join()

    def _consumer(self):
        from druncschema.broadcast_pb2 import BroadcastType

        while True:
            message = self._message_queue.get() # Wait for a message from the controller
            self._log.debug('Received broadcast message: ' + str(message))
            self._listener_lock.acquire()
            for address, listener in self._listeners.items():
                self._log.debug(f'Broadcasting {message} to {address}')
                try:
                    response = listener.handle_broadcast(message)
                except Exception as e:
                    self._log.error(f'Could not broadcast to {address}: {e}')
                self._log.debug(f'Received response from {address}: {response}')

            self._listener_lock.release()

            if message.type == BroadcastType.SERVER_SHUTDOWN:
                break


def main():
    from druncschema.broadcast_pb2_grpc import BroadcastReceiver
    from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
    from druncschema.generic_pb2 import Empty

    class StatusReceiver(BroadcastReceiver):
        def __init__(self, port):
            super(StatusReceiver, self).__init__()
            self.port = port

        def handle_broadcast(self, bm: BroadcastMessage, context: grpc.aio.ServicerContext=None):
            from drunc.utils.grpc_utils import unpack_any
            if bm.type == BroadcastType.SERVER_SHUTDOWN:
                print('End of broadcast')

            elif bm.type == BroadcastType.TEXT_MESSAGE:
                pt = unpack_any(bm.data, PlainText)
                print(f'Got broadcasted message: "{pt.text}" on port {self.port}')

            return Empty()

    def serve(port:int) -> None:
        from concurrent import futures
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        from druncschema.broadcast_pb2_grpc import add_BroadcastReceiverServicer_to_server
        add_BroadcastReceiverServicer_to_server(StatusReceiver(port), server)
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        print(f'Status receiver started on {port}')
        server.wait_for_termination()

    receiver_threads = []
    port_list = range(15000, 15010)
    for port in port_list:
        try:
            server_thread = Thread(target=serve, kwargs={'port':port}, name=f'serve_thread_{port}')
            server_thread.start()
            receiver_threads.append(serve_thread)
        except:
            pass

    broadcaster = BroadcastSender()
    for port in port_list:
        broadcaster.add_listener(f'[::]:{port}')
    broadcaster.broadcast('Test message')
    broadcaster.broadcast('How do you do?')
    broadcaster.broadcast('Let\'s all go for coffee!')
    broadcaster.broadcast('End of broadcast')
    broadcaster.shutdown()
    broadcaster.join()
    print('Broadcasting done')
    print('\n\nYou can now ctrl-c\n\n')

    for thread in receiver_threads:
        thread.join()

if __name__ == '__main__':
    main()