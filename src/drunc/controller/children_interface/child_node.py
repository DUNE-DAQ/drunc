import abc
from drunc.exceptions import DruncSetupException
from drunc.utils.utils import ControlType, get_control_type_and_uri_from_connectivity_service, get_control_type_and_uri_from_cli

class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f'The type {t} is not supported for the ChildNode {name}')


class ChildNode(abc.ABC):
    def __init__(self, name:str, node_type:ControlType, **kwargs) -> None:
        super().__init__(**kwargs)

        self.node_type = node_type
        import logging
        self.log = logging.getLogger(f"{name}-child-node")
        self.name = name

    @abc.abstractmethod
    def __str__(self):
        pass
        return f'\'{self.name}@{self.uri}\' (type {self.node_type})'


    @abc.abstractmethod
    def terminate(self):
        pass


    @abc.abstractmethod
    def propagate_command(self, command, data, token):
        pass

    @abc.abstractmethod
    def get_status(self, token):
        pass

    @abc.abstractmethod
    def get_endpoint(self):
        pass


    @staticmethod
    def get_child(name:str, cli, configuration, init_token=None, connectivity_service=None, **kwargs):

        from drunc.utils.configuration import ConfTypes

        ctype = ControlType.Unknown
        uri = None
        if connectivity_service:
            ctype, uri = get_control_type_and_uri_from_connectivity_service(connectivity_service, name, timeout=60)
        import logging
        log = logging.getLogger("ChildNode.get_child")

        if ctype == ControlType.Unknown:
            ctype, uri = get_control_type_and_uri_from_cli(cli)

        if uri is None or ctype == ControlType.Unknown:
            log.error(f"Could not understand how to talk to \'{name}\'")
            raise DruncSetupException(f"Could not understand how to talk to \'{name}\'")

        log.info(f"Child {name} is of type {ctype} and has the URI {uri}")

        match ctype:
            case ControlType.gRPC:
                from drunc.controller.children_interface.grpc_child import gRPCChildNode, gRCPChildConfHandler

                return gRPCChildNode(
                    configuration = gRCPChildConfHandler(configuration, ConfTypes.PyObject),
                    init_token = init_token,
                    name = name,
                    uri = uri,
                    **kwargs,
                )


            case ControlType.REST_API:
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode,RESTAPIChildNodeConfHandler

                return RESTAPIChildNode(
                    configuration =  RESTAPIChildNodeConfHandler(configuration, ConfTypes.PyObject),
                    name = name,
                    uri = uri,
                    # init_token = init_token, # No authentication for RESTAPI
                    **kwargs,
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(ctype, name)

