from abc import ABC, abstractmethod

from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    ExecuteExpertCommandResponse,
    ExecuteFSMCommandResponse,
    FSMCommand,
    StatusResponse,
)
from druncschema.request_response_pb2 import Response
from druncschema.token_pb2 import Token

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfTypes
from drunc.utils.utils import (
    ControlType,
    get_control_type_and_uri_from_cli,
    get_control_type_and_uri_from_connectivity_service,
    get_logger,
)


class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f"The type {t} is not supported for the ChildNode {name}")


class ChildNode(ABC):
    def __init__(
        self,
        name: str,
        configuration,
        node_type: ControlType,
        **kwargs,
    ) -> None:
        self.node_type = node_type
        self.log = get_logger(f"controller.{name}-child-node")
        self.name = name
        self.configuration = configuration
        self.included = True

    # TODO: terminate abstraction
    def terminate(self):
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def get_endpoint(self) -> str:
        pass

    @abstractmethod
    def propagate_command(
        self,
        command: str,
        request: AddressedCommand,
        token: Token | None,
    ) -> Response:
        pass

    @abstractmethod
    def status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        pass

    @abstractmethod
    def describe(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> DescribeResponse:
        pass

    @abstractmethod
    def describe_fsm(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        key: str = "",
    ) -> DescribeFSMResponse:
        pass

    @abstractmethod
    def execute_fsm_command(
        self,
        command: FSMCommand,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExecuteFSMCommandResponse:
        pass

    @abstractmethod
    def execute_expert_command(
        self,
        json_string: str,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExecuteExpertCommandResponse:
        pass

    @abstractmethod
    def recompute_status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        pass

    # TODO: needs reimplementation
    @staticmethod
    def get_child(
        name: str,
        cli,
        configuration,
        init_token=None,
        connectivity_service=None,
        timeout=60,
        **kwargs,
    ):
        log = get_logger("controller.child_node")
        ctype = ControlType.Unknown
        uri = None
        node_in_error = False

        if connectivity_service:
            try:
                ctype, uri = get_control_type_and_uri_from_connectivity_service(
                    connectivity_service, name, timeout=timeout
                )
            except ApplicationLookupUnsuccessful as alu:
                log.error(
                    f"Could not find the application '{name}' in the connectivity service: {alu}"
                )

        if ctype == ControlType.Unknown:
            try:
                ctype, uri = get_control_type_and_uri_from_cli(cli)
            except DruncSetupException as e:
                log.error(
                    f"Could not understand how to talk to the application '{name}' from its CLI: {e}"
                )

        address = None
        port = 0
        if uri is not None:
            try:
                address, port = uri.split(":")
                port = int(port)
            except ValueError as e:
                log.debug(f"Could not split the URI {uri} into address and port: {e}")

        if ctype == ControlType.Unknown or address is None or port == 0:
            log.error(f"Could not understand how to talk to '{name}'")
            node_in_error = True
            ctype = ControlType.Direct

        log.info(f"Child {name} is of type {ctype} and has the URI {uri}")

        match ctype:
            case ControlType.gRPC:
                from drunc.controller.children_interface.grpc_child import (
                    gRCPChildConfHandler,
                    gRPCChildNode,
                )

                return gRPCChildNode(
                    configuration=gRCPChildConfHandler(
                        configuration, ConfTypes.PyObject
                    ),
                    init_token=init_token,
                    name=name,
                    uri=uri,
                    connectivity_service=connectivity_service,
                    **kwargs,
                )

            case ControlType.REST_API:
                from drunc.controller.children_interface.rest_api_child import (
                    RESTAPIChildNode,
                    RESTAPIChildNodeConfHandler,
                )

                return RESTAPIChildNode(
                    configuration=RESTAPIChildNodeConfHandler(
                        configuration, ConfTypes.PyObject
                    ),
                    name=name,
                    uri=uri,
                    # init_token = init_token, # No authentication for RESTAPI
                    **kwargs,
                )

            case ControlType.Direct:
                from drunc.controller.children_interface.client_side_child import (
                    ClientSideChild,
                )

                node = ClientSideChild(
                    name=name,
                    **kwargs,
                )
                if node_in_error:
                    node.state.to_error()
                return node

            case _:
                raise ChildInterfaceTechnologyUnknown(ctype, name)
