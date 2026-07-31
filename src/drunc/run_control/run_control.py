import getpass
import logging
import multiprocessing as mp
import os

import conffwk
from druncschema.generic_pb2 import OutcomeFlag
from druncschema.process_manager_pb2 import (
    LogLines,
    LogRequest,
    ProcessQuery,
    ProcessUUID,
)
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.run_control_pb2 import (
    DeploySessionResponseFlag,
    EndSessionRequest,
    EndSessionResponse,
    EndSessionResponseFlag,
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
from drunschema.token_pb2 import Token

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.exceptions import ServerUnreachable
from drunc.process_manager.configuration import (
    get_process_manager_configuration,
    validate_pm_config,
)
from drunc.process_manager.interface.process_manager import run_pm
from drunc.process_manager.process_manager_driver import ProcessManagerDriver
from drunc.run_control.configuration import import_config_json_to_dict
from drunc.run_control.interface.context import RunControlContext
from drunc.run_control.utils import (
    ProcessManagerDeploymentType,
    determine_process_manager_type,
)
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

        # The run control is only responsible for a single instance of the session
        # running at any given time. Keep track of whether this is the case by
        # storing the session name in this variable. If it is None, then there is no
        # session running. If it is not None, then there is a session running and the
        # value of this variable is the name of the session.
        self.session_name: str | None = None
        self.session_dal: conffwk.dal | None = None
        self.connectivity_server_client: ConnectivityServiceClient | None = None
        self.process_manager_type: ProcessManagerDeploymentType | None = None
        self.pm_process: mp.Process | None = None
        self.drivers: dict[str, object] = {}
        self.token: Token | None = None

    def start_session(
        self, request: StartSessionRequest, context: RunControlContext
    ) -> StartSessionResponse:
        self.log.info(f"Received StartSession request: {request}")
        self.log.critical("Still requires implementation!")

        # Check there is no running session
        if self.session_name:
            error_msg = (
                f"Cannot start a new session while session [{self.session_name}] is "
                f"running. Please end the current session before starting a new one."
            )
            self.log.error(error_msg)
            return StartSessionResponse(
                token=request.token,
                result=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_RUN_CONTROL_ALREADY_OPERATING_SESSION
                ),
            )

        # Validate whether the process manager is a configuration file or a URI
        # If it is a configuration file, attempt to deploy it
        # In both cases, you need to attempt to communicate with it.
        self.process_manager_type = determine_process_manager_type(
            request.process_manager
        )
        if self.process_manager_type == ProcessManagerDeploymentType.UNKNOWN:
            self.log.error(
                "The process manager is neither a configuration file nor a URI. "
                "Please provide a valid process manager."
            )
            return StartSessionResponse(
                token=request.token,
                result=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_OTHER
                ),
            )

        # If the process manager technology is internal, deploy your process manager
        if self.process_manager_type == ProcessManagerDeploymentType.INTERNAL:
            self.log.info(
                f"Process manager received is {request.process_manager}, deploying"
            )

            process_manager_conf_file: str = get_process_manager_configuration(
                request.process_manager
            )

            # Validate the process manager configuration before starting it
            if not validate_pm_config(process_manager_conf_file):
                self.log.error(
                    "Process manager configuration [red]{process_manager_conf_file[/] validation failed. Exiting."
                )
                return StartSessionResponse(
                    token=request.token,
                    result=DeploySessionResponseFlag(
                        status=DeploySessionResponseFlag.FAILURE_OTHER
                    ),
                )

            # Start the process manager as a separate process
            self.log.info(f"Starting process manager {request.process_manager}")

            pm_conf_dict = import_config_json_to_dict(
                "process_manager", request.process_manager
            )
            pm_host = pm_conf_dict.get("host", "localhost")
            pm_port = pm_conf_dict.get("port", 0)
            pm_address = f"{pm_host}:{pm_port}"
            port = mp.Value("i", 0)

            self.log.debug(
                "Startign [green]process manager[/] with configuration file: [green]%s[/]",
                process_manager_conf_file,
            )
            self.pm_process = mp.Process(
                target=run_pm,
                kwargs={
                    "pm_conf": process_manager_conf_file,
                    "pm_address": pm_address,
                    "override_logs": request.override_logs,
                    "log_level": "INFO",  # PLACEHOLDER
                    "log_path": ".",  # PLACEHOLDER
                    "generated_port": port,
                },
            )
            self.pm_process.start()
            self.log.debug("[green]Process manager[/green] started")
        else:
            pm_address = request.process_manager
            self.log.info(
                f"External process manager address received: {pm_address}, using it directly"
            )

        # Add the process manager driver
        self.token = request.token
        self.drivers["process_manager"] = ProcessManagerDriver(pm_address, self.token)

        # Establish communication with the process manager, check it is running and ready to accept requests
        self.log.debug(
            f"Attempting to connect to the process manager at the address: [green]{pm_address}[/]"
        )
        try:
            self.drivers("process_manager").describe()
        except Exception as e:
            self.log.error(
                f"[red]Could not connect to the process manager at the address: [/red]"
                f"[green]{pm_address}[/green]"
            )
            self.log.critical(f"Reason: {e}")

            if type(e) == ServerUnreachable:
                self.log.error(
                    "[red]This can happen if you have the webproxy enabled at CERN. Ensure "
                    "http_proxy, https_proxy, no_proxy, and equivalent aren't set. [/red]"
                )

            if (
                self.process_manager_type == ProcessManagerDeploymentType.INTERNAL
                and not self.pm_process.is_alive()
            ):
                self.log.error(
                    f"[red]The process_manager is dead[/red], exit code "
                    f"{self.pm_process.exitcode}"
                )

            if self.pm_process.is_alive():
                self.pm_process.terminate()
                self.pm_process.join()

            return StartSessionResponse(
                token=request.token,
                result=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_PROCESS_MANAGER_NOT_REACHABLE
                ),
            )

        # Get the dal to get the connectivity service client
        db = conffwk.Configuration(request.configuration_file)
        self.session_dal = db.get_dal(class_name="Session", uid=request.session_id)
        connectivity_service_address: str = (
            f"{self.session_dal.connectivity_service.host}:"
            f"{self.session_dal.connectivity_service.service.port}"
        )
        self.connectivity_server_client = ConnectivityServiceClient(
            self.session_name, connectivity_service_address
        )

        # Print the process manager endpoint addresses
        self.log.info(
            f"Process manager is running and reachable at the address: [green]{pm_address}[/]"
        )
        self.log.info("Ready to start the data taking")

        # Include the endpoint addresses in the response
        return StartSessionResponse(
            token=request.token,
            result=DeploySessionResponseFlag(status=DeploySessionResponseFlag.SUCCESS),
        )

    def end_session(
        self, request: EndSessionRequest, context: RunControlContext
    ) -> EndSessionResponse:
        self.log.info(f"Received EndSession request: {request}")
        self.log.critical("Still requires implementation!")

        # Check there is a running session
        if not self.session:
            error_msg = (
                "Cannot end a session when no session is running. Please start a "
                "session before attempting to end it."
            )
            self.log.error(error_msg)
            return EndSessionResponse(
                token=request.token,
                result=EndSessionResponseFlag(
                    status=EndSessionResponseFlag.FAILURE_RUN_CONTROL_NO_OPERATING_SESSION
                ),
            )

        # Bring the session's FSM back to an intiial state if running in safe mode
        self.log.info(
            "Bringing the session's FSM back to an initial state if running in safe mode"
        )
        self.log.critical("LEFT AS A TODO")
        # if hasattr(self.drivers, "controller"):
        #     try:
        #         self.log.info("Attempting graceful shutdown of the controller")
        #         stop_run_cmd = ctx.command.commands.get("stop-run")
        #         scrap_cmd = ctx.command.commands.get("scrap")
        #         if stop_run_cmd is not None:
        #             ctx.invoke(stop_run_cmd)
        #         else:
        #             ctx.obj.log.warning(
        #                 "Command 'stop-run' not found; skipping graceful "
        #                 "shutdown step."
        #             )
        #         if scrap_cmd is not None:
        #             ctx.invoke(scrap_cmd)
        #         else:
        #             ctx.obj.log.warning(
        #                 "Command 'scrap' not found; skipping graceful "
        #                 "shutdown step."
        #             )
        #         ctx.obj.log.info("Controller shutdown gracefully")
        #     except Exception as e:
        #         ctx.obj.log.error(
        #             f"Could not shutdown the controller gracefully, reason: {e}"

        # Remove the controller driver
        if hasattr(self.drivers, "controller"):
            self.drivers.remove("controller")

        # Check all processes have been terminated. If not, terminate them
        self.log.info("Checking all processes have been terminated")
        if hasattr(self.drivers, "process_manager"):
            query = ProcessQuery(session=self.session_name)
            running_processes = self.drivers["process_manager"].ps(query)
            if running_processes:
                self.log.info("Attempting to terminate residual processes")
                self.drivers["process_manager"].kill(query)
                self.log.info("All processes terminated successfully")

        # Retract the session from the connectivity server
        try:
            self.connectivity_server_client.retract_partition(
                fail_quickly=True, fail_quietly=True
            )
            self.log.debug("Session retracted from the connectivity service")
        except Exception as e:
            self.log.error(
                f"Could not retract the session from the connectivity service: {e}"
            )

        # Remove the process manager driver
        self.drivers("process_manager").send_msg(
            f"{getpass.getuser()} disconnected from the run control"
        )
        self.drivers("process_manager").close()
        self.drivers.remove("process_manager")

        # If the process manager was deployed by the run control, terminate it
        if self.process_manager_type == ProcessManagerDeploymentType.INTERNAL:
            self.pm_process.terminate()  # Send a SIGTERM to the pm_process
            self.pm_process.join(timeout=2)  # Block continuing execution for 2s
            if self.pm_process.is_alive():
                self.log.warning(
                    "Process manager did not exit in time, terminating forcefully."
                )
                self.pm_process.kill()  # Send a SIGKILL
                self.pm_process.join()  # Block until the process is dead
            self.log.debug("Process manager terminated")

            self.log.info("[green]unified_shell exited successfully[/green]")
            logging.shutdown()
            self.terminate()
            self.pm_process = None

        # Clear the local variables
        self.session_name = None
        self.process_manager_type = None
        self.token = None
        self.session_dal = None
        self.connectivity_server_client = None

        return EndSessionResponse(
            token=request.token,
            result=EndSessionResponseFlag(status=EndSessionResponseFlag.SUCCESS),
        )

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

    def logs(self, request: LogRequest, context: RunControlContext) -> LogLines:
        """
        Get the logs of the run control service.

        Args:
            request (LogRequest): The request containing the log retrieval parameters.
            context (RunControlContext): The gRPC context.

        Returns:
            LogLines: The response containing the retrieved log lines.

        Raises:
            TBC
        """
        self.log.info(f"Received Logs request: {request}")
        self.log.critical("Still requires implementation!")
        return LogLines(
            name="run_control",
            token=request.token,
            uuid=ProcessUUID(uuid=str(os.getpid())),
            lines=["Not yet, you've gotta wait a bit ;)"],
            flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
        )
