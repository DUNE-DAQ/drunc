import abc
from enum import Enum
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration_utils import ConfTypes, ConfData, ConfTypeNotSupported

class ChildNodeType(Enum):
    gRPC = 1
    REST_API = 2


class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f'The type {t} is not supported for the ChildNode {name}')


class ChildNode(abc.ABC):
    def __init__(self, name:str, node_type:ChildNodeType, token=None, **kwargs) -> None:
        super(ChildNode, self).__init__(
            **kwargs
        )

        self.node_type = node_type
        import logging
        self.log = logging.getLogger(f"{name}-child-node")
        self.name = name
        self.token = token


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
    def get_child(name, conf:ConfData, token=None, **kwargs):
        child_type = ''

        match conf.type:
            case ConfTypes.RawDict:
                child_type = conf['type']
            case ConfTypes.OKSObject:
                child_type = 'rest-api'
            case _:
                raise ConfTypeNotSupported(conf.type, "ChildNode.get_child")

        match child_type:
            case 'grpc':
                from drunc.controller.children_interface.grpc_child import gRPCChildNode
                return gRPCChildNode(
                    child_conf = conf,
                    token = token,
                    name = name,
                    node_type = ChildNodeType.gRPC,
                    **kwargs,
                )
            case 'rest-api':
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode
                return RESTAPIChildNode(
                    child_conf = conf,
                    token = token,
                    name = name,
                    node_type = ChildNodeType.REST_API,
                    **kwargs,
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(child_type, name)

