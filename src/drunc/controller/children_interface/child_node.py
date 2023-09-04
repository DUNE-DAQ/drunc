import abc

class ChildInterfaceTechnologyUnknown(Exception):
    def __init__(self, t, name):
        super().__init__(f'The type {t} is not supported for the ChildNode {name}')


class ChildNode(abc.ABC):
    def __init__(self, name:str, node_type:str, token=None) -> None:
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


    # def take_control(self, token):
    #     self.token = token


    @staticmethod
    def get_from_file(name, conf:dict, token=None):

        match conf['type']:
            case 'grpc':
                from drunc.controller.children_interface.grpc_child import gRPCChildNode
                return gRPCChildNode(
                    conf = conf,
                    conf_type = 'file',
                    token = token,
                    name = name,
                    node_type = conf['type']
                )
            case 'rest-api':
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode
                return RESTAPIChildNode(
                    conf = conf,
                    conf_type = 'file',
                    token = token,
                    name = name,
                    node_type = conf['type']
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(conf['type'], name)


    @staticmethod
    def get_from_oks(name, conf, token=None):
        raise RuntimeError('OKS Not supported')
