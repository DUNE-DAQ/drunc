from enum import Enum
from drunc.utils.utils import setup_fancy_logging

class ChildNodeType(Enum):
    kUnknown = 0
    kController = 1
    kDAQApplication = 2


class ChildNode:
    def __init__(self, node_type:ChildNodeType=ChildNodeType.kUnknown, name:str='') -> None:
        self.node_type = node_type
        self.log = setup_fancy_logging(name)
        self.name = name

    def close(self):
        raise NotImplementedError('This method must be implemented by the child node.')

    def propagate_command(self, command, data, token, location):
        raise NotImplementedError('This method must be implemented by the child node.')