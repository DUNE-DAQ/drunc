
from druncschema.broadcast_pb2 import BroadcastMessage
from drunc.broadcast.server.broadcast_sender_implementation import BroadcastSenderImplementation

class KafkaSender(BroadcastSenderImplementation):
    def __init__(self, conf):
        self.kafka_address = conf['kafka_address']

    def _send(self, bm:BroadcastMessage):
        pass


