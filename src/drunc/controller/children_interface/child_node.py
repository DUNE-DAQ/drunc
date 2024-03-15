import abc
from enum import Enum
from drunc.exceptions import DruncSetupException

class ChildNodeType(Enum):
    Unknown = 0
    gRPC = 1
    REST_API = 2


class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f'The type {t} is not supported for the ChildNode {name}')


class ChildNode(abc.ABC):
    def __init__(self, name:str, node_type:ChildNodeType, **kwargs) -> None:
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


    @staticmethod
    def _get_children_type_from_cli(CLAs:list[str]) -> ChildNodeType:
        for CLA in CLAs:
            if   CLA.startswith("rest://"): return ChildNodeType.REST_API
            elif CLA.startswith("grpc://"): return ChildNodeType.gRPC

        from drunc.exceptions import DruncSetupException
        raise DruncSetupException("Could not find if the child was controlled by gRPC or a REST API")

    @staticmethod
    def _get_children_type_and_uri_from_registry(application_registry, name) -> tuple[ChildNodeType, str]:
        uri = application_registry.lookup(name)
        return ChildNode._get_children_type_from_cli([uri]), uri


    @staticmethod
    def get_child(name:str, cli, configuration, init_token=None, application_registry=None, **kwargs):

        from drunc.utils.configuration import ConfTypes

        type = ChildNodeType.Unknown
        uri = None
        if application_registry:
            type, uri = ChildNode._get_children_type_from_registry(application_registry, name)

        if type == ChildNodeType.Unknown:
            type = ChildNode._get_children_type_from_cli(cli)

        match type:
            case ChildNodeType.gRPC:
                from drunc.controller.children_interface.grpc_child import gRPCChildNode, gRCPChildConfHandler

                return gRPCChildNode(
                    configuration = gRCPChildConfHandler(configuration, ConfTypes.PyObject),
                    init_token = init_token,
                    name = name,
                    uri = uri,
                    **kwargs,
                )


            case ChildNodeType.REST_API:
                from drunc.controller.children_interface.rest_api_child import RESTAPIChildNode,RESTAPIChildNodeConfHandler

                return RESTAPIChildNode(
                    configuration =  RESTAPIChildNodeConfHandler(configuration, ConfTypes.PyObject),
                    name = name,
                    uri = uri,
                    # init_token = init_token, # No authentication for RESTAPI
                    **kwargs,
                )
            case _:
                raise ChildInterfaceTechnologyUnknown(type, name)

