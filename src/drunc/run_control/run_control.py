import abc
import getpass
from collections.abc import MutableMapping
from dataclasses import asdict, dataclass, fields
from urllib.parse import ParseResult, urlparse

import conffwk
from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.common_pb2 import LoggerTarget, SendLogRequest, SendLogResponse
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.process_manager_pb2 import (
    ProcessInstanceList,
    ProcessQuery,
)
from druncschema.request_response_pb2 import Request, Response, ResponseFlag
from druncschema.run_control_pb2 import ConnectToPMRequest, SessionConfiguration
from druncschema.run_control_pb2_grpc import RunControlServicer
from grpc import ServicerContext, StatusCode

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.utils.utils import get_logger, log_echo, resolve_localhost_to_hostname


@dataclass
class SessionConfig:
    configuration_file: str = ""
    configuration_id: str = ""
    session_name: str = ""
    valid: bool = False


class RunControl(abc.ABC, RunControlServicer):
    rc_type = "run_control"

    def __init__(self):
        # TODO: This needs to contain the configuration name
        self.log = get_logger("run_control.CONFIGURATION_NAME_run_control")
        self.name = "run_control"  # TODO Skeleton: needs to update
        self.address_pm = None
        self.session_config = SessionConfig()
        self.override_logs = True
        self.drivers: MutableMapping[str, object] = {}

        dach = DummyAuthoriserConfHandler.from_pyobject(
            data=None  # TODO Skeleton: CONFIGURATION NEEDS UPDATING HERE
        )

        self.authoriser = DummyAuthoriser(dach, SystemType.RUN_CONTROL)

        self.commands = [
            CommandDescription(
                name="describe",
                data_type=["request_response_pb2.Request"],
                help="Describe the server and its supported commands.",
                return_type="description_pb2.Description",
            ),
            CommandDescription(
                name="send_log",
                data_type=["common_pb2.SendLogRequest"],
                help="Log a message on the server.",
                return_type="common_pb2.SendLogResponse",
            ),
            CommandDescription(
                name="connect_to_pm",
                data_type=["run_control_pb2.ConnectToPMRequest"],
                help="Connect Run Control to a process manager.",
                return_type="request_response_pb2.Response",
            ),
            CommandDescription(
                name="ps",
                data_type=["process_manager_pb2.ProcessQuery"],
                help="List processes matching the query within the configured session.",
                return_type="process_manager_pb2.ProcessInstanceList",
            ),
            CommandDescription(
                name="set_session_config",
                data_type=["run_control_pb2.SessionConfiguration"],
                help="Update non-empty session settings and reset the valid flag.",
                return_type="request_response_pb2.Response",
            ),
            CommandDescription(
                name="print_session_config",
                data_type=["request_response_pb2.Request"],
                help="Return the current session configuration and valid flag.",
                return_type="run_control_pb2.SessionConfiguration",
            ),
            CommandDescription(
                name="valid",
                data_type=["request_response_pb2.Request"],
                help="Set the session configuration valid flag to true without validation.",
                return_type="request_response_pb2.Response",
            ),
        ]

    def set_session_config(
        self,
        config: SessionConfiguration,
        context: ServicerContext,
    ) -> Response:
        self.session_config.valid = False

        for field in fields(self.session_config):
            if field.name == "valid":
                continue
            value = getattr(config, field.name)
            if value:
                setattr(self.session_config, field.name, value)

            # Cant believe we have to do this
            if field.name == "configuration_file":
                self.session_config.configuration_file = (
                    lambda path: (
                        path
                        if path.startswith("oksconflibs:")
                        else f"oksconflibs:{path}"
                    )
                )(self.session_config.configuration_file)

        return Response(
            name=self.name,
            token=config.token,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    @authentified_and_authorised(
        action=ActionType.READ,
        system=SystemType.RUN_CONTROL,
    )
    def print_session_config(
        self,
        request: Request,
        context: ServicerContext,
    ) -> SessionConfiguration:
        """Return the current session configuration."""
        return SessionConfiguration(
            token=request.token,
            **asdict(self.session_config),
        )

    @authentified_and_authorised(
        action=ActionType.UPDATE,
        system=SystemType.RUN_CONTROL,
    )
    def validate(self, request: Request, context: ServicerContext) -> Response:
        """Set the session configuration valid flag to true."""

        self.session_config.valid = False

        required_fields = ("configuration_file", "configuration_id", "session_name")
        missing = [
            name
            for name in required_fields
            if not getattr(self.session_config, name).strip()
        ]

        if missing:
            message = f"Missing session configuration fields: {', '.join(missing)}"
            self.log.error(message)
            context.abort(StatusCode.INVALID_ARGUMENT, message)

        try:
            db = conffwk.Configuration(self.session_config.configuration_file)
        except Exception as e:
            message = f"Invalid file: {e}"
            self.log.error(message)
            context.abort(StatusCode.INVALID_ARGUMENT, message)

        try:
            db.get_dal(class_name="Session", uid=self.session_config.configuration_id)
        except Exception as e:
            message = f"Some invaild id: {e}"
            self.log.error(message)
            context.abort(StatusCode.INVALID_ARGUMENT, message)

        self.session_config.valid = True
        return Response(
            name=self.name,
            token=request.token,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

    def send_log(
        self,
        request: SendLogRequest,
        context: ServicerContext,
    ) -> SendLogResponse:
        """
        Log a message on the server with the specified severity.

        Args:
            request: SendLogRequest containing the log message and severity.
            context: gRPC ServicerContext (not used).

        Returns:
            SendLogResponse indicating the result of the logging operation.

        Raises:
            None
        """

        response = SendLogResponse(token=None, flag=ResponseFlag.EXECUTED_SUCCESSFULLY)

        # LoggerTarget.MAIN -> the app's real logger, LoggerTarget.ECHO -> drunc.echo
        target_log = self.log if request.logger == LoggerTarget.MAIN else log_echo
        level = request.severity.lower()
        getattr(target_log, level, target_log.info)(request.text)
        return response

    @authentified_and_authorised(action=ActionType.READ, system=SystemType.RUN_CONTROL)
    def describe(self, request: Request, context: ServicerContext) -> Description:
        self.log.info(f"{self.name} running describe")

        # TODO Skeleton: Will obviously need filling up as features pop in
        response = Description(
            type="some name for now",
            name="unnamed",
            info="no info for now",
            session="no session",
            commands=self.commands,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )

        return response

    def boot():
        # workflow:
        """
        1. add a 'valid' in the dataclass
        2. in boot, check if valid is valid. if not, exit and say 'make sure u validate!'
        3. if valid, boot as usual
        """

        pass

    @authentified_and_authorised(action=ActionType.READ, system=SystemType.RUN_CONTROL)
    def connect_to_pm(
        self, request: ConnectToPMRequest, context: ServicerContext
    ) -> Response:
        address_raw = request.address
        self.log.info("PM address received: %s", address_raw)

        address_raw: ParseResult = urlparse(address_raw)
        address_raw = str(address_raw.netloc) if address_raw.netloc is not None else ""
        address_raw = resolve_localhost_to_hostname(address_raw)

        self.address_pm = address_raw

        from drunc.process_manager.process_manager_driver import (
            ProcessManagerDriver,
        )

        self.drivers["process_manager"] = ProcessManagerDriver(
            self.address_pm,
            request.token,
        )

        self.log.debug("made process manager driver")

        # check if describe works
        pm_type = self.drivers["process_manager"].describe().type
        self.log.info(f"PM connected, type: {pm_type}")

        self.drivers["process_manager"].send_log(
            f"{getpass.getuser()} connected from run control (!!)"
        )

        # TODO Skeleton: Will obviously need filling up as features pop in
        response = Response(
            name="unnamed",
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )
        return response

    @authentified_and_authorised(action=ActionType.READ, system=SystemType.RUN_CONTROL)
    def ps(
        self, request: ProcessQuery, context: ServicerContext
    ) -> ProcessInstanceList:
        self.log.info(f"{self.name} running ps")

        request.session = self.session_config.session_name

        if "process_manager" not in self.drivers:
            self.log.error("Cannot list processes: process manager is not connected")
            return ProcessInstanceList(
                name=self.name,
                token=request.token,
                flag=ResponseFlag.NOT_EXECUTED_NOT_READY,
            )
        pm_driver = self.drivers["process_manager"]

        # Will need cleaning later
        args = f" with request {request}" if request else ""
        msg = f"{getpass.getuser()} sent ps{args} via {self.rc_type}"
        pm_driver.send_log(msg)

        results = pm_driver.ps(request)
        return results
