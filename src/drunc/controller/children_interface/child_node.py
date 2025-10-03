import os

from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    FSMCommandsDescription,
    Status,
    StatusResponse,
)
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token

from drunc.connectivity_service.exceptions import ApplicationLookupUnsuccessful
from drunc.controller.utils import get_detector_name
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


class ChildNode:  # abc.ABC):
    def __init__(
        self, name: str, configuration, node_type: ControlType, **kwargs
    ) -> None:
        self.node_type = node_type
        self.log = get_logger(f"controller.{name}-child-node")
        self.name = name
        self.configuration = configuration
        self.included = True

    def __str__(self):
        pass
        return f"'{self.name}@{self.uri}' (type {self.node_type})"

    # @abc.abstractmethod
    def terminate(self):
        pass

    # @abc.abstractmethod
    def get_endpoint(self) -> str | None:
        return None

    # @abc.abstractmethod
    def propagate_command(
        self,
        command: str,
        request: AddressedCommand,
        token: Token | None,
    ) -> Response:
        return Response(
            name=self.name,
            token=token,
            data=None,
            flag=ResponseFlag.NOT_EXECUTED_NOT_READY,
            children=[],
        )

    # @abc.abstractmethod
    def status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        status = Status(
            state="unknown",
            sub_state="unknown",
            in_error=False,
            included=True,
        )

        response = StatusResponse(
            token=None,
            name=self.name,
            status=status,
            children=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        return response

    def describe(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> DescribeResponse:
        descriptionType = None
        descriptionName = None

        if self.configuration is not None:
            if hasattr(
                self.configuration.data, "application_name"
            ):  # Get the application name and type
                descriptionType = self.configuration.data.application_name
                descriptionName = self.configuration.data.id
            elif hasattr(self.configuration.data, "controller") and hasattr(
                self.configuration.data.controller, "application_name"
            ):  # Get the controller name and type
                descriptionType = self.configuration.data.controller.application_name
                descriptionName = self.configuration.data.controller.id

        description = Description(
            type=descriptionType,
            name=descriptionName,
            endpoint=self.get_endpoint(),
            info=(
                get_detector_name(self.configuration)
                if self.configuration is not None
                else None
            ),
            session=os.getenv("DUNEDAQ_SESSION"),
            commands=None,
            broadcast=None,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        response = DescribeResponse(
            token=None,
            name=self.name,
            description=description,
            children=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        return response

    def describe_fsm(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
        key: str = "",
    ) -> DescribeFSMResponse:
        description = FSMCommandsDescription()

        response = DescribeFSMResponse(
            token=None,
            name=self.name,
            description=description,
            children=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        return response

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
