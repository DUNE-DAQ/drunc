import abc
from druncschema.broadcast_pb2 import BroadcastMessage

class BroadcastSenderImplementation(abc.ABC):
    @abc.abstractmethod
    def _send(self, bm:BroadcastMessage):
        pass

    @abc.abstractmethod
    def describe_broadcast(self):
        pass

    @abc.abstractmethod
    def can_broadcast(self):
        pass

