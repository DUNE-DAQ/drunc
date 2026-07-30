from druncschema.generic_pb2 import OutcomeStatus
from druncschema.run_control_pb2 import (
    EndSessionRequest,
    EndSessionResponse,
    LogOnServerRequest,
    LogOnServerResponse,
    StartSessionRequest,
    StartSessionResponse,
    ValidateCommunicationRequest,
    ValidateCommunicationResponse,
    ValidateSessionRequest,
    ValidateSessionResponse,
)
from druncschema.run_control_pb2_grpc import RunControlServicer

from drunc.run_control.interface.context import RunControlContext
from drunc.utils.utils import get_logger


class RunControl(RunControlServicer):
    def __init__(self, config: dict[str, str | int | float | bool]):
        self.config: dict[str, str | int | float | bool] = config
        self.log = get_logger(
            "run_control",
            file_handler_path=getattr(self.config, "log_path", None),
            rich_handler=True,
        )
        self.log.debug(
            "Initialized the run control service with config: %s", self.config
        )

    def start_session(
        self, request: StartSessionRequest, context: RunControlContext
    ) -> StartSessionResponse:
        self.log.info(f"Received StartSession request: {request}")
        return StartSessionResponse()

    def end_session(
        self, request: EndSessionRequest, context: RunControlContext
    ) -> EndSessionResponse:
        self.log.info(f"Received EndSession request: {request}")
        return EndSessionResponse()

    def validate_session(
        self, request: ValidateSessionRequest, context: RunControlContext
    ) -> ValidateSessionResponse:
        self.log.info(f"Received ValidateSession request: {request}")
        return ValidateSessionResponse()

    def log_on_server(
        self, request: LogOnServerRequest, context: RunControlContext
    ) -> LogOnServerResponse:
        self.log.info(f"Received SendMsg request: {request}")
        return OutcomeStatus()

    def validate_communication(
        self, request: ValidateCommunicationRequest, context: RunControlContext
    ) -> ValidateCommunicationResponse:
        self.log.info(f"Received ValidateCommunication request: {request}")
        return ValidateCommunicationResponse(
            token=self.token, status=OutcomeStatus.SUCCESS
        )
