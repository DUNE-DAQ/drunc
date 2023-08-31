import abc

class ChildInterfaceTechnologyUnknown(Exception):
    def __init__(self, t):
        super().__init__(f'The type {t} is not supported for the ChildNode')

class ChildNode(abc.ABC):
    def __init__(self, name:str) -> None:
        self.node_type = node_type
        import logging
        self.log = logging.getLogger(f"{name}-child-node")
        self.name = name

    @abc.abstractmethod
    def _close(self):
        pass

    @abc.abstractmethod
    def _propagate_command(self, command, data, token):
        pass


    @staticmethod
    def get(conf:dict):
        match conf['type']:
            case 'grpc':
                from drunc.controller.children_interface.grpc_child import gRPCChildNode
                return gRPCChildNode(conf)
            case 'rest':
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode
                return RESTAPIChildNode(conf)
            case _:
                raise ChildInterfaceTechnologyUnknown(conf['type'])