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

    @abc.abstractmethod
    def get_endpoint(self):
        pass

    @staticmethod
    def _get_children_type_from_cli(CLAs:list[str]) -> ChildNodeType:
        for CLA in CLAs:
            if   CLA.startswith("rest://"): return ChildNodeType.REST_API
            elif CLA.startswith("grpc://"): return ChildNodeType.gRPC

        from drunc.exceptions import DruncSetupException
        raise DruncSetupException("Could not find if the child was controlled by gRPC or a REST API")

    @staticmethod
    def _get_children_type_and_uri_from_connectivity_service(connectivity_service, name) -> tuple[ChildNodeType, str]:
        uris = []
        from drunc.connectivity_service.client import ApplicationLookupUnsuccessful

        for _ in range(5):
            try:

                uris = connectivity_service.resolve(name+'_control.*')
                if len(uris) == 0:
                    raise ApplicationLookupUnsuccessful

            except ApplicationLookupUnsuccessful as e:
                import time
                time.sleep(1)
                continue

        if len(uris) != 1:
            raise DruncSetupException(f"Could not resolve the URI for \'{name}_control\' in the connectivity service, got response {uris}")

        uri = uris[0]['uri']

        return ChildNode._get_children_type_from_cli([uri]), uri


    @staticmethod
    def get_child(name:str, cli, configuration, init_token=None, connectivity_service=None, **kwargs):

        from drunc.utils.configuration import ConfTypes

        ctype = ChildNodeType.Unknown
        uri = None
        if connectivity_service:
            ctype, uri = ChildNode._get_children_type_and_uri_from_connectivity_service(connectivity_service, name)
        import logging
        log = logging.getLogger("ChildNode.get_child")

        if ctype == ChildNodeType.Unknown:
            ctype = ChildNode._get_children_type_from_cli(cli)

        log.info(f"Child {name} is of type {ctype} and has the URI {uri}")

        match ctype:
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
                raise ChildInterfaceTechnologyUnknown(ctype, name)

