import abc
import getpass

from urllib.parse import ParseResult, urlparse

from druncschema.authoriser_pb2 import ActionType, SystemType
from druncschema.common_pb2 import LoggerTarget, SendLogRequest, SendLogResponse
from collections.abc import MutableMapping
from druncschema.description_pb2 import CommandDescription, Description
from druncschema.request_response_pb2 import Request, ResponseFlag, Response
from druncschema.run_control_pb2_grpc import RunControlServicer
from druncschema.run_control_pb2 import ConnectToPMRequest
from grpc import ServicerContext

from drunc.authoriser.configuration import DummyAuthoriserConfHandler
from drunc.authoriser.decorators import authentified_and_authorised
from drunc.authoriser.dummy_authoriser import DummyAuthoriser
from drunc.utils.utils import get_logger, log_echo, resolve_localhost_to_hostname


from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
)

class RunControl(abc.ABC, RunControlServicer):
    rc_type = "Hehehehe"

    def __init__(self):
        # TODO: This needs to contain the configuration name
        self.log = get_logger("run_control.CONFIGURATION_NAME_run_control")
        self.name = "run_control"  # TODO Skeleton: needs to update

        # A whole lot of configuration happpening here! We should dataclass this awayy later but for now.. fine
        self.address_pm = None
        self.configuration_file = None
        self.configuration_id = None
        self.session_name = "emir-test"
        self.override_logs = True
        self.drivers: MutableMapping[str, object] = {}

        dach = DummyAuthoriserConfHandler.from_pyobject(
            data=None  # TODO Skeleton: CONFIGURATION NEEDS UPDATING HERE
        )

        self.authoriser = DummyAuthoriser(dach, SystemType.RUN_CONTROL)

        self.commands = [
            CommandDescription(
                name="describe",
                data_type=["None"],
                help="Describe self (return a list of commands, the type of endpoint, the name and session).",
                return_type="description_pb2.Description",
            ),
        ]

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

        self.log.info(f"{address_raw=}")

        from drunc.process_manager.process_manager_driver import (
            ProcessManagerDriver,
        )

        self.drivers["process_manager"] = ProcessManagerDriver(
            self.address_pm,
            request.token,
        )

        self.log.info("made process manager driver")

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
    def ps(self, request: ProcessQuery, context: ServicerContext) -> ProcessInstanceList:

        self.log.info(f"{self.name} running ps")


        request.session = self.session_name

        # need checks to see if the driver exists

        self.log.info(f"getting driver")
        pm_driver = self.drivers["process_manager"]

        self.log.info(f"Sending request")
        results = pm_driver.ps(request)

        self.log.info(f"returning")
        
        return results


        # NOTE THAT SESSION SHOULD BE FILLED FORM THE RUN CONTROL

        # NOTE THAT SESSION SHOULD BE FILLED FORM THE RUN CONTROL

        # call in the pmd 
        # use the pmd to send a message to the pms 
        # get the pms output 

        # that should be returned as a respoinse thing


        # log_pm_cmd(obj)
        # ps_impl(obj, query, long_format, width, fit)


