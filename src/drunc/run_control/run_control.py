from druncschema.generic_pb2 import OutcomeFlag
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.run_control_pb2 import (
    DeploySessionResponseFlag,
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
        return StartSessionResponse(
            token=request.token,
            result=DeploySessionResponseFlag(status=DeploySessionResponseFlag.SUCCESS),
        )

    def end_session(
        self, request: EndSessionRequest, context: RunControlContext
    ) -> EndSessionResponse:
        self.log.info(f"Received EndSession request: {request}")
        return EndSessionResponse(token=request.token, result=OutcomeFlag.SUCCESS)

    def validate_session(
        self, request: ValidateSessionRequest, context: RunControlContext
    ) -> ValidateSessionResponse:
        self.log.info(
            "This will require a check with the session manager server to validate that "
            "there is nothing wrong with the session name"
        )
        self.log.info(
            "Once the session name is validated, resource manager checks will be ran, "
            "but the resource manager currently does not exist"
        )
        return ValidateSessionResponse(
            token=request.token, result=DeploySessionResponseFlag.FAILURE_OTHER
        )

    def log_on_server(
        self, request: LogOnServerRequest, context: RunControlContext
    ) -> LogOnServerResponse:
        """
        Log the message on the server with the specified severity level.

        Args:
            request (LogOnServerRequest): The request containing the log message and
                severity level.
            context (RunControlContext): The gRPC context.

        Returns:
            LogOnServerResponse: The response indicating the outcome of the logging
                operation.

        Raises:
            TODO: ValueError: If the severity level is not recognized.
        """

        # Map the severity level to the corresponding logging method and get the method
        level = request.severity.lower()
        log_method = getattr(self.log, level, None)

        # Log the message using the appropriate logging method
        if log_method:
            log_method(request.text)
            return LogOnServerResponse(
                token=request.token, flag=ResponseFlag.EXECUTED_SUCCESSFULLY
            )

        return LogOnServerResponse(
            token=request.token, flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED
        )

        # Return a response indicating the outcome of the logging operation

    def validate_communication(
        self, request: ValidateCommunicationRequest, context: RunControlContext
    ) -> ValidateCommunicationResponse:
        self.log.info(
            f"Received ValidateCommunication request from user [green]{request.token.user_name}[/]"
        )
        return ValidateCommunicationResponse(
            token=request.token, status=OutcomeFlag.SUCCESS
        )
