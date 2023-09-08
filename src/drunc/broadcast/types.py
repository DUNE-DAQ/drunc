from enum import Enum


class BroadcastTypes(Enum):
    gRPC = 1
    Kafka = 2
    ERS = 2


class BroadcastTypeNotHandled(Exception):
    def __init__(self, btype):
        message = f'{btype} not handled'
        super(BroadcastTypeNotHandled, self).__init__(
            message
        )