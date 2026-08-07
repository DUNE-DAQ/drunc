import getpass
import logging
import multiprocessing as mp
import os
import sys
import time

import conffwk
from druncschema.description_pb2 import Description
from druncschema.generic_pb2 import OutcomeFlag
from druncschema.process_manager_pb2 import (
    LogLines,
    LogRequest,
    ProcessInstance,
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
    RunControlBootRequest,
    RunControlBootResponse,
    RunControlTerminateRequest,
    RunControlTerminateResponse,
    StartSessionRequest,
    StartSessionResponse,
    ValidateCommunicationRequest,
    ValidateCommunicationResponse,
    ValidateSessionRequest,
    ValidateSessionResponse,
)
from druncschema.run_control_pb2_grpc import RunControlServicer
from druncschema.token_pb2 import Token
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.controller.controller_driver import ControllerDriver
from drunc.exceptions import DruncSetupException
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
from drunc.utils.grpc_utils import ServerUnreachable
from drunc.utils.shell_utils import InterruptedCommand
from drunc.utils.utils import (
    get_logger,
    ignore_sigint_sighandler,
    resolve_localhost_and_127_ip_to_network_ip,
)


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
        self.configuration_file: str | None = None
        self.session_id: str | None = None
        self.override_logs: bool | None = None
        self.controller_log_level: str | None = None

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
            self.log.info(f"{process_manager_conf_file=}")

            # Validate the process manager configuration before starting it
            if not validate_pm_config(process_manager_conf_file):
                self.log.error(
                    f"Process manager configuration [red]{process_manager_conf_file}[/] validation failed. Exiting."
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
            self.log.info(f"Got the {pm_conf_dict=}")
            pm_host = resolve_localhost_and_127_ip_to_network_ip(
                pm_conf_dict.get("host", "localhost")
            )
            pm_port = pm_conf_dict.get("port", 0)
            pm_address = f"{pm_host}:{pm_port}"

            ctx_mp = mp.get_context("spawn")
            ready_event = ctx_mp.Event()
            port = ctx_mp.Value("i", 0)

            # Get the session DAL
            db = conffwk.Configuration(
                "oksconflibs:" + request.path_to_configuration_file
            )
            session_dal = db.get_dal(class_name="Session", uid=request.session_id)
            app_log_path = session_dal.log_path

            self.log.info(
                "Starting [green]process manager[/] with configuration file: [green]%s[/]",
                process_manager_conf_file,
            )
            self.log.info(f"Target address: {pm_address=}")
            self.pm_process = ctx_mp.Process(
                target=run_pm,
                kwargs={
                    "pm_conf": process_manager_conf_file,
                    "pm_address": pm_address,
                    "override_logs": request.override_logs,
                    "log_level": "DEBUG",  # PLACEHOLDER
                    "log_path": app_log_path,
                    "ready_event": ready_event,
                    "signal_handler": ignore_sigint_sighandler,
                    "generated_port": port,
                },
            )
            self.log.info("Starting the pm process")
            self.pm_process.start()

            # Check if the process manager started correctly
            process_started = False
            for _ in range(100):  # 10s timeout
                if ready_event.is_set():
                    process_started = True
                    break

                if not self.pm_process.is_alive():
                    exit_code = self.pm_process.exitcode
                    self.log.error(
                        f"[red]Process manager process died unexpectedly with exit code {exit_code}."
                    )
                    self.log.error(
                        "[red]This is likely a configuration error (e.g., bad kube-config)."
                    )
                    self.log.error(
                        "[red]Please check the full traceback in the terminal above this message.[/red]"
                    )
                    sys.exit(exit_code if exit_code else 1)
                time.sleep(0.1)

            if not process_started:
                # This message will only show if the process is *alive* but never sent the "ready" signal
                raise DruncSetupException(
                    "[red]Process manager timed out starting. Check logs for details.[/red]"
                )

            # Setup the process manager address
            process_manager_address = resolve_localhost_and_127_ip_to_network_ip(
                f"{pm_host}:{port.value}"
            )
            # ctx.obj.reset(address_pm=process_manager_address)
            self.log.debug(
                f"[green]process_manager[/green] started at address [green]"
                f"{process_manager_address}[/green]"
            )

            self.log.debug("[green]Process manager[/green] started")
            # Update the communication port number, since it may have been set to 0 in the configuration file
            pm_address = f"{pm_host}:{port.value}"
            self.log.info(
                f"Process manager started at address: [green]{pm_address}[/green]"
            )
        else:
            pm_address = request.process_manager
            self.log.info(
                f"External process manager address received: {pm_address}, using it directly"
            )

        # Add the process manager driver
        self.token = request.token
        self.log.warning(f"Adding process manager driver with address: {pm_address}")
        self.drivers["process_manager"] = ProcessManagerDriver(pm_address, self.token)

        # Establish communication with the process manager, check it is running and ready to accept requests
        self.log.info(
            f"Attempting to connect to the process manager at the address: [green]{pm_address}[/]"
        )
        try:
            describe_result = self.drivers["process_manager"].describe()
            self.log.critical(f"{describe_result=}")
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

            if self.pm_process and self.pm_process.is_alive():
                self.pm_process.terminate()
                self.pm_process.join()

            return StartSessionResponse(
                token=request.token,
                result=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_PROCESS_MANAGER_NOT_REACHABLE
                ),
            )

        self.log.critical("PROCESS MANAGER EXISTS WOOHOO!")
        self.configuration_file = request.path_to_configuration_file
        self.session_id = request.session_id
        self.override_logs = request.override_logs
        self.controller_log_level = request.controller_log_level
        self.session_name = request.session_name

        # Get the dal to get the connectivity service client
        connectivity_service_address: str = (
            f"{session_dal.connectivity_service.host}:"
            f"{session_dal.connectivity_service.service.port}"
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
        if not self.session_name:  # Fixed check to match self.session_name
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

        # Remove the controller driver
        if "controller" in self.drivers:
            self.drivers.pop("controller")

        # Check all processes have been terminated. If not, terminate them
        self.log.info("Checking all processes have been terminated")
        if "process_manager" in self.drivers:
            query = ProcessQuery(session=self.session_name)
            running_processes = self.drivers["process_manager"].ps(query)
            if running_processes:
                self.log.info("Attempting to terminate residual processes")
                self.drivers["process_manager"].kill(query)
                self.log.info("All processes terminated successfully")

        # Retract the session from the connectivity server
        if self.connectivity_server_client:
            try:
                self.connectivity_server_client.retract_partition(
                    fail_quickly=True, fail_quietly=True
                )
                self.log.debug("Session retracted from the connectivity service")
            except Exception as e:
                self.log.error(
                    f"Could not retract the session from the connectivity service: {e}"
                )

        # Remove the process manager driver using proper dict access
        if "process_manager" in self.drivers:
            self.drivers["process_manager"].send_msg(
                f"{getpass.getuser()} disconnected from the run control"
            )
            self.drivers["process_manager"].close()
            del self.drivers["process_manager"]

        # If the process manager was deployed by the run control, terminate it
        if (
            self.process_manager_type == ProcessManagerDeploymentType.INTERNAL
            and self.pm_process
        ):
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
            if hasattr(self, "terminate"):
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

    def boot(
        self, request: RunControlBootRequest, context: RunControlContext
    ) -> RunControlBootResponse:
        """
        Boot the run control service.

        Args:
            request (RunControlBootRequest): The request containing the boot parameters.
            context (RunControlContext): The gRPC context.

        Returns:
            RunControlBootResponse: The response indicating the outcome of the boot
                operation.
        """
        if not self.session_name:
            self.log.error(
                "Cannot boot: run control does not have a session. Please start a session first."
            )
            return RunControlBootResponse(
                token=request.token,
                flag=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_OTHER
                ),
            )
        self.log.info(f"Received Boot request: {request}")
        processes = self.drivers["process_manager"].ps(
            ProcessQuery(session=self.session_name)
        )

        # Check that the run control has a process manager driver
        if "process_manager" not in self.drivers:
            self.log.error(
                "Cannot boot: run control does not have a process manager driver. "
                "Please start a session first."
            )
            return RunControlBootResponse(
                token=request.token,
                flag=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_OTHER
                ),
            )

        # Store the number of processes that are expected to be booted with this command, to check later if any processes died immediately after booting.
        expected_booted_processes = 0

        # PLACEHOLDER
        user = getpass.getuser()  # PLACEHOLDER

        # The run control will validate this in the session manager in the future
        if len(processes.values) > 0:
            self.log.error(
                f"Cannot boot: session {self.session_name} already has {len(processes.values)} processes running. "
                "Please terminate the existing session first."
            )
            # Note this will be overridden with an exception handled through gRPC
            # interceptors, but that will only happen once we have the base set of
            # comamnds running
            return RunControlBootResponse(
                token=request.token,
                flag=DeploySessionResponseFlag(
                    status=DeploySessionResponseFlag.FAILURE_SESSION_APPS_ALREADY_RUNNING
                ),
            )

        try:
            results = self.drivers["process_manager"].boot(
                conf_file=self.configuration_file,
                conf_id=self.session_id,
                user=user,
                session_name=self.session_name,
                log_level=self.controller_log_level,
                override_logs=self.override_logs,
                sleep_between_app_boot=0,
            )
            expected_booted_processes = sum(1 for _ in results)
            for result in results:
                self.log.critical(
                    f"Booting process: {result.values[0].process_description.metadata.name}"
                )
                if not result:
                    break
                self.log.debug(
                    f"'{result.values[0].process_description.metadata.name}' ({result.values[0].uuid.uuid}) started"
                )
        except InterruptedCommand:
            self.log.warning("Booting interrupted")
            return
        except DruncSetupException as e:
            self.log.error(e)
            return

        controller_address = self.drivers["process_manager"].controller_address
        if controller_address:
            self.log.info(f"Controller endpoint is '{controller_address}'")
            self.log.info("Connecting the unified_shell to the controller endpoint")
            self.drivers["controller"] = ControllerDriver(
                controller_address, self.token
            )

        else:
            self.log.error("Could not understand where the controller is!")
            return

        # If any processes died immediately, place the controller in error.
        alive_process_count = len(
            [p for p in processes.values if p.status_code == ProcessInstance.RUNNING]
        )

        dead_process_count = expected_booted_processes - alive_process_count

        if (
            not self.drivers["controller"].status().status.in_error
            and dead_process_count == 0
        ):
            self.log.info("Booted successfully")
        elif dead_process_count != 0:
            self.log.error(
                f"Booted, but {dead_process_count} processes died after booting."
            )
            # The following line has been commented out as there are issues with the k8s PM
            # booting process, which terminates processes and immediately reboots them. The
            # current cause of this issue is unknown, and has been listed in the issue list.
            # obj.get_driver("controller").to_error()
        elif self.drivers["controller"].status().status.in_error:
            self.log.error("Booted, but the top controller is in error")
            # if obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]:
            #     log.error(
            #         "Unified shell: Running in batch mode, and because error state is detected, exiting."
            #     )
            sys.exit(1)

        return RunControlBootResponse(
            token=request.token, flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED
        )

    def terminate(
        self, request: RunControlTerminateRequest, context: RunControlContext
    ) -> RunControlTerminateResponse:
        """
        Execute the process manager terminate command, but only do this for the current
        session
        """
        session_query = ProcessQuery(session=self.session_name)
        self.log.info(f"Terminating session [green]{self.session_name}[/]")
        self.drivers["process_manager"].kill(session_query)

        # As the session is now terminated, we can delete the controller driver, as it is no
        # longer needed.
        self.drivers.pop("controller")

    def _controller_setup(self, controller_address):
        desc = Description()

        timeout = 60

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            # console=ctx._console,
        ) as progress:
            waiting = progress.add_task(
                "[yellow]Trying to talk to the root controller...", total=timeout
            )

            stored_exception = None

            start_time = time.time()
            while time.time() - start_time < timeout:
                progress.update(waiting, completed=time.time() - start_time)

                try:
                    desc = self.drivers["controller"].describe().description
                    stored_exception = None
                    break
                except ServerUnreachable as e:
                    stored_exception = e
                    time.sleep(1)
                except Exception as e:
                    self.log.critical("Could not get the controller's status")
                    self.log.critical(e)
                    self.log.critical("Exiting.")
                    self.drivers["process_manager"].terminate()
                    raise e

        if stored_exception is not None:
            raise stored_exception

        self.log.info(
            f"{controller_address} is '{desc.name}.{desc.session}' (name.session), starting listening..."
        )
        self.drivers["controller"].name = f"{desc.name}.{desc.session}"

        self.log.warning("Connected to the controller")

        # 60s for everyone to show up on the connectivity service, and 10s to come out of initialising state
        timeout = 60 + 10

        time_start = time.time()
        state = self.drivers["controller"].status().status.state.lower()
        # with StatusTableUpdater(ctx) as updater:
        #     task = updater.add_task("Waiting on tree initialisation...", total=timeout)
        while time.time() - time_start < timeout and state == "initialising":
            state = self.drivers["controller"].status().status.state.lower()
            # updater.update(task, completed=time.time() - time_start)
            # updater.update_table()
            time.sleep(0.5)

            # updater.update_table()

        if state == "initialising":
            self.log.error("Controller did not initialise in time")
            return

        self.log.debug(f"Taking control of the controller as {self.token()}")
        try:
            ret = self.drivers["controller"].take_control()

            if ret.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
                self.log.info("You are in control.")
                # ctx.took_control = True
            else:
                self.log.info("You are NOT in control.")
                # ctx.took_control = False

        except Exception as e:
            self.log.error("You are NOT in control.")
            raise e

        return desc
