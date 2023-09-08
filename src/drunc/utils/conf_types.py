from enum import Enum

class ConfTypes(Enum):
    Json = 0
    Protobuf = 1
    OKS = 2


class ConfTypeNotSupported(Exception):
    def __init__(self, conf_type, class_name):
        message = f'{conf_type.value} is not supported by {class_name}'
        super(ConfTypeNotSupported, self).__init__(message)
