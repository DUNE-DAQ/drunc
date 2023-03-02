import grpc
from threading import Thread, Lock
from queue import Queue

class ListenerRepresentation:

    def __init__(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        from drunc.communication.controller_pb2_grpc import BroadcastStub
        self.stub = BroadcastStub(self.channel)

    def handle_broadcast(self, message):
        return self.stub.handle_broadcast(message)

class Broadcaster:

    def __init__(self):
        from drunc.utils.utils import setup_fancy_logging
        self._log = setup_fancy_logging("Broadcaster")
        self._listeners = {}
        self._listener_lock = Lock()
        self._message_queue = Queue()
        self._consumer_thread = Thread(target=self._consumer, name='broadcast_consumer')
        self._consumer_thread.start()
        self._log.info('Broadcaster started')

    def new_broadcast(self, message):
        return self._message_queue.put(message)

    def add_listener(self, address):
        self._listener_lock.acquire()
        if address in self._listeners.keys():
            self._log.error(f'Listener {address} already exists')
            self._listener_lock.release()
            return False
        self._log.info(f'Adding listener {address}')
        self._listeners[address] = ListenerRepresentation(address)
        self._listener_lock.release()
        return True


    def rm_listener(self, address):
        self._listener_lock.acquire()
        if address not in self._listeners.keys():
            self._log.error(f'Listener {address} does not exist')
            self._listener_lock.release()
            return False
        self._log.info(f'Removing listener {address}')
        del self._listeners[address]
        self._listener_lock.release()
        return True

    def join(self):
        return self._consumer_thread.join()

    def _consumer(self):
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

            if message.payload == "over_and_out":
                break


def main():
    from drunc.communication.controller_pb2_grpc import BroadcastServicer
    from drunc.communication.controller_pb2 import BroadcastMessage, GenericResponse, ResponseCode

    class StatusReceiver(BroadcastServicer):
        def __init__(self, port):
            super(StatusReceiver, self).__init__()
            self.port = port

        def handle_broadcast(self, bm: BroadcastMessage, context: grpc.aio.ServicerContext=None) -> GenericResponse:
            print(f'Got broadcasted message: "{str(bm.payload)}" on port {self.port}')
            return GenericResponse(
                response_code = ResponseCode.DONE,
                response_text = 'OK'
            )

    def serve(port:int) -> None:
        from concurrent import futures
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        from drunc.communication.controller_pb2_grpc import add_BroadcastServicer_to_server
        add_BroadcastServicer_to_server(StatusReceiver(port), server)
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        print(f'Status receiver started on {port}')
        server.wait_for_termination()

    receiver_threads = []
    for port in range(500, 510):
        try:
            server_thread = Thread(target=serve, kwargs={'port':port}, name=f'serve_thread_{port}')
            server_thread.start()
            receiver_threads.append(serve_thread)
            break
        except:
            pass

    broadcaster = Broadcaster()
    for port in range(500, 510):
        broadcaster.add_listener(f'[::]:{port}')
    broadcaster.new_broadcast(BroadcastMessage(payload='Test message', emitter='broadcaster-test', level=0))
    broadcaster.new_broadcast(BroadcastMessage(payload='How do you do?', emitter='broadcaster-test', level=0))
    broadcaster.new_broadcast(BroadcastMessage(payload='Let\'s all go for coffee!', emitter='broadcaster-test', level=0))
    broadcaster.new_broadcast(BroadcastMessage(payload='end_of_broadcast', emitter='broadcaster-test', level=0))
    broadcaster.join()
    print('Broadcasting done')
    print('\n\nYou can now ctrl-c\n\n')

    for thread in receiver_threads:
        thread.join()

if __name__ == '__main__':
    main()