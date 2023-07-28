import abc
from druncschema.broadcast_pb2 import BroadcastMessage

class BroadcastSenderImplementation(abc.ABC):
    @abc.abstractmethod
    def _send(self, bm:BroadcastMessage):
        pass


