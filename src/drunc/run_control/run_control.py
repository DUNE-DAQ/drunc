from druncschema.run_control_pb2 import (
    EndSessionRequest,
    EndSessionResponse,
    StartSessionRequest,
    StartSessionResponse,
    ValidateSessionRequest,
    ValidateSessionResponse,
)
from druncschema.run_control_pb2_grpc import RunControlServicer

from drunc.utils.utils import get_logger


class RunControl(RunControlServicer):
    def __init__(self, config: dict[str, str | int | float | bool]):
        self.log = get_logger(__name__)
        self.config = config

    def start_session(
        self, request: StartSessionRequest, context
    ) -> StartSessionResponse:
        self.log.info(f"Received StartSession request: {request}")
        return StartSessionResponse()

    def end_session(self, request: EndSessionRequest, context) -> EndSessionResponse:
        self.log.info(f"Received EndSession request: {request}")
        return EndSessionResponse()

    def validate_session(
        self, request: ValidateSessionRequest, context
    ) -> ValidateSessionResponse:
        self.log.info(f"Received ValidateSession request: {request}")
        return ValidateSessionResponse()
