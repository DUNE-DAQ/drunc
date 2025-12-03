from abc import ABC, abstractmethod

from druncschema.controller_pb2 import (
    AddressedCommand,
    DescribeFSMResponse,
    DescribeResponse,
    ExcludeResponse,
    ExecuteExpertCommandResponse,
    ExecuteFSMCommandResponse,
    FSMCommand,
    IncludeResponse,
    RecomputeStatusResponse,
    StatusResponse,
)
from druncschema.request_response_pb2 import Response
from druncschema.token_pb2 import Token

from drunc.exceptions import DruncSetupException
from drunc.utils.utils import (
    ControlType,
    get_logger,
)


class ChildInterfaceTechnologyUnknown(DruncSetupException):
    def __init__(self, t, name):
        super().__init__(f"The type {t} is not supported for the ChildNode {name}")


class ChildNode(ABC):
    def __init__(self, name: str, node_type: ControlType):
        self.log = get_logger(f"controller.children_interface.{name}-child-node")
        self.name = name
        self.node_type = node_type
        self.included = True

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def get_endpoint(self) -> str:
        pass

    @abstractmethod
    def terminate(self) -> None:
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
    def include(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> IncludeResponse:
        pass

    @abstractmethod
    def exclude(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExcludeResponse:
        pass

    @abstractmethod
    def recompute_status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> RecomputeStatusResponse:
        pass
