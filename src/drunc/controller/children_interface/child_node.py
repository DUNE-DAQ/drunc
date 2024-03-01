import abc
from enum import Enum
from drunc.exceptions import DruncSetupException

class ChildNodeType(Enum):
    gRPC = 1
    REST_API = 2


class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f'The type {t} is not supported for the ChildNode {name}')


class ChildNode(abc.ABC):
    def __init__(self, name:str, node_type:ChildNodeType, **kwargs) -> None:
        super(ChildNode, self).__init__(
            **kwargs
        )

        self.node_type = node_type
        import logging
        self.log = logging.getLogger(f"{name}-child-node")
        self.name = name


    @abc.abstractmethod
    def __str__(self):
        pass


    @abc.abstractmethod
    def terminate(self):
        pass


    @abc.abstractmethod
    def propagate_command(self, command, data, token):
        pass


    @abc.abstractmethod
    def get_status(self, token):
        pass


    @staticmethod
    def get_child(name:str, type:ChildNodeType, configuration, token=None, **kwargs):

        match type:
            case ChildNodeType.gRPC:
                from drunc.controller.children_interface.grpc_child import gRPCChildNode
                return gRPCChildNode(
                    configuration = configuration,
                    token = token,
                    name = name,
                    **kwargs,
                )
            case ChildNodeType.REST_API:
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode
                return RESTAPIChildNode(
                    configuration = configuration,
                    token = token,
                    name = name,
                    **kwargs,
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(type, name)

