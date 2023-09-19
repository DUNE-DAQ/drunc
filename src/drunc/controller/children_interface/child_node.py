import abc
from enum import Enum

class ChildNodeType(Enum):
    gRPC = 1
    REST_API = 2


class ChildInterfaceTechnologyUnknown(Exception):
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
    def close(self):
        pass


    @abc.abstractmethod
    def propagate_command(self, command, data, token):
        pass


    @abc.abstractmethod
    def get_status(self, token):
        pass

    @staticmethod
    def get_from_file(name, conf:dict, token=None):
        from drunc.utils.conf_types import ConfTypes

        match conf['type'].lower():
            case 'grpc':
                from drunc.controller.children_interface.grpc_child import gRPCChildNode
                return gRPCChildNode(
                    child_conf = conf,
                    conf_type = ConfTypes.Json,
                    token = token,
                    name = name,
                    node_type = ChildNodeType.gRPC
                )
            case 'rest-api':
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode
                return RESTAPIChildNode(
                    child_conf = conf,
                    conf_type = ConfTypes.Json,
                    token = token,
                    name = name,
                    node_type = ChildNodeType.REST_API
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(conf['type'], name)


    @staticmethod
    def get_from_oks(name, conf, token=None):
        raise RuntimeError('OKS Not supported')
