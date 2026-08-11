"""
This is a fake DAQ application that doesn't do anything, but should talk in the same way
to the run control.

It is primarily used for the testing of the Run Control.
"""

import copy as cp
import os
import random
import signal
import threading
import time
from typing import NotRequired, TypedDict, cast
from urllib.parse import urlparse
from wsgiref.simple_server import WSGIServer, make_server

import click
import conffwk
import requests
from flask import Flask, Response, request
from flask_restful import Api, Resource  # type: ignore[import-untyped]

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.utils.utils import (
    get_logger,
    get_new_port,
    get_root_logger,
    resolve_localhost_and_127_ip_to_network_ip,
)

# TypedDicts for execute_command request structure - execution-time is not a valid
# python identifier, so we use a TypedDict to define the structure of the data
# dictionary in the command request.
_CmdData = TypedDict(
    "_CmdData",
    {
        "execution-time": int,
        "seg_fault": int,
        "throw": bool,
    },
    total=False,
)


class CommandRequest(TypedDict):
    """Expected structure for incoming DAQ application command requests."""

    entry_state: str
    exit_state: str
    id: str
    data: NotRequired[_CmdData]


_CommandResult = tuple[str, int]


__version__ = "1.0.0"

# Set up logging
get_root_logger("info")
log = get_logger("fake_daqapp_rest", stream_handlers=True, log_level="INFO")


class AppState:
    """
    Tracks state of the apps, and simulates the behaviour of daq_applicaitons with
    stateful commands.
    """

    def __init__(self, app_name: str):
        """
        Initialize the app state.

        Args:
            app_name (str): The name of the app, used for logging and in the responses

        Returns:
            None

        Raises:
            None
        """
        self.appname = app_name
        self.state = "INITIAL"
        self.executing_command = False
        self.log = get_logger("fake_daqapp_rest.AppState", log_level="INFO")

    def send_response_to_response_listener(
        self,
        address: str,
        txt: str,
        success: bool = True,
        data: dict[str, object] | None = None,
    ) -> None:
        """
        Send a response to the response listener.

        Args:
            address (str): The address of the response listener
            txt (str): The text to send in the response
            success (bool): Whether the command was executed successfully or not
            data (dict): Additional data to send in the response

        Returns:
            None

        Raises:
            None
        """

        # The response is sent as a POST request to the response listener, with the
        # following contents in the body:
        data_to_send = {
            "success": success,
            "result": txt,
            "appname": self.appname,
            "data": data or {},
        }

        self.log.info(f"Sending RESPONSE to {address}, data: {data_to_send}")
        try:
            response = requests.post(
                address,
                json=data_to_send,
                headers={
                    "Content-Type": "application/json",
                },
            )
            response.raise_for_status()
        except Exception as e:
            self.log.error("Couldn't send response to response listener")
            self.log.exception(e)

    def execute_command(
        self,
        req_data: CommandRequest,
        answer_port: str,
        answer_host: str | None,
        remote_host: str | None,
    ) -> Response:
        """
        Execute a command received from the command facility.

        Args:
            req_data (dict): The data received in the command, should contain at least
                the following keys:
                - id: The id of the command, used for logging and in the responses
                - entry_state: The state the app should be in to execute the command, or
                    "*" to ignore the state
                - exit_state: The state the app will be in after executing the command
                - data: A dictionary with additional data for the command, can contain:
                    - execution-time: An integer with the time the command should take
                        to execute, in seconds
                    - seg_fault: An integer that if present will cause the app to exit
                        with that code
                    - throw: If present, the app will throw an exception instead of
                        executing the command
            answer_port (str): The port to send the response to
            answer_host (str | None): The host to send the response to, if None, the
                remote_host will be used
            remote_host (str): The host that sent the command, used for logging and as a
                fallback for the answer_host

        Returns:
            Response: A Flask response object with the result of the command execution

        Raises:
            None
        """
        self.log.debug("Received command with the following data:")
        self.log.debug(f"{req_data=}")
        self.log.debug(f"{answer_port=}")
        self.log.debug(f"{answer_host=}")
        self.log.debug(f"{remote_host=}")

        # Construct the address to send the response to
        reply_address = (
            f"http://{answer_host}:{answer_port}/response"
            if answer_host
            else f"{remote_host or 'localhost'}:{answer_port}/response"
        )

        # Extract the relevant information from the command data
        entry_state = req_data["entry_state"]
        exit_state = req_data["exit_state"]
        command_id = req_data["id"]
        raw_data = req_data.get("data")
        data: _CmdData = raw_data if raw_data is not None else {}

        # If the app is already executing a command, it should not execute another one.
        # Send a response to the response listener indicating that it is busy
        if self.executing_command:
            response_txt = "Already executing a command!"
            self.log.info(
                "Application is already executing a command, cannot execute another "
                "one simultaneously."
            )
            self.send_response_to_response_listener(
                address=reply_address,
                txt=response_txt,
                success=False,
            )
            return Response("Already executing a command!\n", status=202)

        # Determine the time the command should take to execute. If not specified in the
        # data, it will be a random time between 1 and 5 seconds. We also determine a
        # random time for the worries, which is the time the app will wait before
        # failing the command in case of a seg_fault or throw, to simulate the time it
        # takes for the app to fail after starting the execution of the command.
        cmd_exec_time = data.get("execution-time", random.randint(1, 5))
        worries = random.randint(0, cmd_exec_time)

        # Validate that the app is in the correct state to execute the command. If not,
        # send a response to the response listener indicating that the command cannot
        # be executed due to the state of the app. The wildcard "*" can be used to
        # indicate that the command can be executed in any state.
        if entry_state != "*" and self.state != entry_state.upper():
            info = (
                f"DAQ Application is in state {self.state} and command {command_id} "
                f"requires to be in state {entry_state.upper()} to execute. Not "
                "executing."
            )
            self.log.info(info)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=info,
            )
            return Response(f"{info}\n", status=202)

        # Execute the command, and mark the app as busy executing a command to prevent
        # concurrent executions.
        self.log.info(f"Executing {command_id}")
        self.executing_command = True

        # Failure testing through payload
        if data.get("seg_fault"):
            time.sleep(worries)
            app_execution_info = "<seeeeeeeeeg fauuuuuuuult message>"
            self.log.info(app_execution_info)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=app_execution_info,
            )
            self.executing_command = False
            exit(data["seg_fault"])

        if data.get("throw"):
            time.sleep(worries)
            app_execution_info = (
                "This is an eRrOr, YoU hAvE bEeN vErY nAuGhTy (aka task failed "
                "successfully)"
            )
            self.log.info(app_execution_info)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=app_execution_info,
            )
            self.executing_command = False
            return Response(f"{app_execution_info}\n", status=202)

        # FAILURE TESTING - CMD TIMEOUT
        # For testing purposes, we can delay the execution of the command to simulate a
        # long running command and test timeouts in the run control
        ft_fsm_timeout_raw = os.getenv("DRUNC_FT_FSM_CMD_TIMEOUT")
        ft_fsm_timeout = int(ft_fsm_timeout_raw) if ft_fsm_timeout_raw else None
        ft_fsm_timeout_cmd = os.getenv("DRUNC_FT_FSM_CMD_TIMEOUT_CMD")
        ft_fsm_timeout_app_name = os.getenv("DRUNC_FT_FSM_CMD_TIMEOUT_APP_NAME")
        if (
            ft_fsm_timeout
            and ft_fsm_timeout_cmd == command_id
            and ft_fsm_timeout_app_name == self.appname
        ):
            self.log.warning(
                f"Delaying execution of {command_id} in {ft_fsm_timeout_app_name} by "
                f"{ft_fsm_timeout} seconds"
            )
            time.sleep(ft_fsm_timeout)

        # FAILURE TESTING - CMD PROCESS DEATH
        # The following block simulates a failure of the app while executing a stateful
        # command. Thisserves uniquely to test the robustness of the Run Control when an
        # app exits upon running an applciation, and should not be used for any other
        # purpose.
        ft_fsm_death_cmd_raw = os.getenv("DRUNC_FT_FSM_CMD_DEATH_CMD", "")
        ft_fsm_death_cmd = False
        if ft_fsm_death_cmd_raw:
            ft_fsm_death_cmd = (
                ft_fsm_death_cmd_raw.strip('"').strip("'") == req_data["id"]
            )
        self.log.debug(f"{ft_fsm_death_cmd=}")

        ft_fsm_death_app_name_raw = os.getenv("DRUNC_FT_FSM_CMD_DEATH_APP_NAME", "")
        ft_fsm_death_app_name = False
        if ft_fsm_death_app_name_raw:
            ft_fsm_death_app_name = (
                ft_fsm_death_app_name_raw.strip('"').strip("'") == self.appname
            )
        self.log.debug(f"{ft_fsm_death_app_name=}")

        if ft_fsm_death_cmd and ft_fsm_death_app_name:
            self.log.debug("'Worries' sleeping prior to simulating process death")
            time.sleep(worries)
            self.log.warning(
                f"Simulating death of {self.appname} during FSM cmd execution"
            )
            # This requires a more agressive exit than sys.exit(), as this process is
            # running in a separate thread.
            os._exit(1)

        # "Execute" the command by sleeping for the determined time
        self.log.info(f"Sleeping for {cmd_exec_time} seconds")
        time.sleep(cmd_exec_time)

        # Notify command success
        app_execution_info = (
            f"Executed {command_id} successfully, after {cmd_exec_time} seconds"
        )
        self.log.info(app_execution_info)
        self.send_response_to_response_listener(
            success=True,
            address=reply_address,
            txt=app_execution_info,
        )

        # Update app state, and mark as not busy
        self.state = exit_state.upper()
        self.executing_command = False
        return Response(f"{app_execution_info}\n", status=202)


"""
Resources for Flask app
"""


class AppCommand(Resource):  # type: ignore[misc, no-any-unimported]
    """
    Flask interface for the fake daq app.

    Receives the commands from the command facility and passes them to the AppState to
    be executed, and sends the response back to the response listener.
    """

    @classmethod
    def pass_daq_app(cls, daq_app: AppState) -> type["AppCommand"]:
        """
        Interface to pass the daq_app instance to the Flask resource, since Flask
        doesn't allow to pass arguments to the resource constructor.
        """
        cls.daq_app = daq_app
        return cls

    def post(self) -> _CommandResult:
        """
        Endpoint to receive commands from the command facility. The command data should
        be sent in a JSON format, with the following structure:
        {
            "id": "command_id",
            "entry_state": "state the app should be in to execute the command, or *",
            "exit_state": "state the app will be in after executing the command",
            "data": { optional parameters:
                "execution-time": "time the command should take to execute",
                "seg_fault": "app will exit with this code to simulate a failure",
                "throw": "app will throw an exception to simulate a failure"
            }
        }
        """
        global app_state

        # Validate that the request contains JSON data
        try:
            data = request.get_json(force=True)
        except:
            return "Not a JSON command!\n", 406

        if not isinstance(data, dict):
            return "JSON command must be an object!\n", 406

        command_data = cast(CommandRequest, data)

        log = get_logger("fake_daqapp_rest.AppCommand")
        log.info(f"GET request with args: {data}")

        # Execute the command in a separate thread to not block the Flask app and to
        # allow concurrent command executions, since the app can receive multiple
        # commands while executing one command, and to allow the simulation of long
        # running commands without blocking the Flask app.
        thread = threading.Thread(
            target=self.daq_app.execute_command,
            kwargs={
                "req_data": cp.deepcopy(command_data),
                "answer_port": request.headers["X-Answer-Port"],
                "answer_host": request.headers.get("X-Answer-Host"),
                "remote_host": request.remote_addr,
            },
        )
        thread.start()

        return "Command received\n", 202


# Helper functions
def update_connectivity_service(
    name: str, connectivity_service: ConnectivityServiceClient, interval: int, url: str
) -> None:
    """
    Function to continuously update the connectivity service with the address of the
    app, to simulate the behaviour of a real DAQ application that is continuously
    publishing its address to the connectivity service. This is necessary for the Run
    Control to be able to send commands to the app, since the Run Control gets the
    address of the app from the connectivity service. The function runs in a separate
    thread to not block the main thread of the app, which is running the Flask app to
    receive commands from the command facility.

    Args:
        name: The name of the publishing app
        connectivity_service: the client to publish to the connectivity service
        interval: Interval in seconds to update the connectivity service
        url: The app address to publish to the connectivity service

    Returns:
        None

    Raises:
        None
    """
    while True:
        connectivity_service.publish(
            name + "_control",
            url,
            "RunControlMessage",
        )
        time.sleep(interval)


def index() -> str:
    """
    Endpoint to check if the app is running, can be used in the tests to wait for the
    app to be ready before sending commands to it.

    Args:
        None

    Returns:
        str: A string indicating the app is running.

    Raises:
        None
    """
    return f"Fake DAQ app v{__version__}"


def get_address(hostname: str) -> str:
    """
    Gets a new address for the application, by finding an available port.

    Args:
        hostname: The hostname to use in the address

    Returns:
        str: URI with the given hostname and a new available port

    Raises:
        None
    """
    return f"rest://{hostname}:{get_new_port()}"


@click.command()
@click.option("-n", "--name", required=True, help="The name of the app in the response")
@click.option(
    "-d",
    "--configurationservice",
    required=True,
    help="This is a dummy argument in this case",
)
@click.option(
    "-c",
    "--commandfacility",
    required=False,
    help="Where the fake app should get its command from",
)
@click.option(
    "-i",
    "--informationservice",
    default="stdout://flat",
    help="This is a dummy argument in this case",
)
@click.option(
    "-l", "--log_level", default="info", help="Logging level minimum threshold"
)
@click.option(
    "-p",
    "--partition",
    default="global",
    help="This is a dummy argument in this case",
)
@click.option("-s", "--session", default="test", help="name of session")
@click.option("-k", "--configurationid", default="test-config", help="ID of session")
def main(
    name: str,
    configurationservice: str,
    commandfacility: str | None,
    informationservice: str,
    log_level: str,
    partition: str,
    session: str,
    configurationid: str,
) -> None:
    # The following block simulates a failure during the initialization of the app. This
    # serves uniquely to test the robustness of the Run Control when an app fails to
    # initialize, and should not be used for any other purpose. The environment variable
    # is set in the configuration file that tests this behaviour.
    if os.getenv("DRUNC_PROCESS_DEATH_ON_BOOT"):
        log.info("Sleeping to allow intiialization timeout")
        time.sleep(20)
        log.warning("Simulating failure during initialization")
        exit(1)
    log.info(f"Starting application {name}")
    app_state = AppState(name)

    # Set up and parse configuration
    conf = conffwk.Configuration(configurationservice)
    session_dal = conf.get_dal(
        class_name="Session",
        uid=configurationid,
    )
    connectivity_service_address = (
        session_dal.connectivity_service.host
        + ":"
        + str(session_dal.connectivity_service.service.port)
    )

    # Validate command facility argument
    if not commandfacility:
        log.critical("No command facility passed, exiting")
        exit(1)

    # Resolve the command facility URL and validate the scheme
    url = urlparse(resolve_localhost_and_127_ip_to_network_ip(commandfacility))
    if url.scheme != "rest":
        log.exception("DAQApplication communication scheme must be rest")
        exit(1)

    log.debug(f"Initializing fake_daq_application with address {url}")
    if url.port == 0:
        if not url.hostname:
            log.exception("No hostname in command facility URL")
            exit(1)
        url = urlparse(get_address(url.hostname))
    log.info(f"Communication address is {url}")

    interval = 2

    # FAILURE TESTING - DEATH ON BOOT
    # The following block simulates a failure on initialization of the app. This
    # serves uniquely to test the robustness of the Run Control when an app fails to
    # complete initialization, and should not be used for any other purpose. The
    # environment variable is set in the configuration file that tests this behaviour.
    ft_die_on_boot: bool = (
        os.getenv("DRUNC_FT_PROCESS_DEATH_ON_BOOT", "false").lower() == "true"
    )
    ft_app_to_die_boot = os.getenv("DRUNC_FT_PROCESS_DEATH_BOOT_APP_NAME", "")
    if ft_die_on_boot and ft_app_to_die_boot == name:
        log.warning(f"Simulating death of {name} on boot")
        exit(1)

    connectivity_service = ConnectivityServiceClient(
        session=session,
        address=connectivity_service_address,
    )

    connectivity_service_thread = threading.Thread(
        target=update_connectivity_service,
        args=(name, connectivity_service, interval, url.geturl()),
        name="connectivity_service_updating_thread",
    )

    def terminate(*args: object) -> None:  # Accept args for signal handlers
        for s in [signal.SIGTERM, signal.SIGQUIT]:
            if signal.getsignal(s) in args:
                log.warning(f"Received termination signal {s}, shutting down {name}...")
        log.info(f"Terminating application {name}...")
        shutdown_event.set()

        # 2. Close connections explicitly
        if "server" in server_container:
            try:
                server = server_container["server"]
                server.server_close()
            except:
                pass

        # 3. Give threads a tiny buffer to stop gracefully
        time.sleep(0.1)

        # 4. Final hard exit
        log.info("Shutdown complete. Exiting.")
        os._exit(1)

    def terminate_signal_process(signum: int, sigframe: object) -> None:
        log.warning(f"Received signal {signum}, terminating process")
        terminate()

    for sig in [signal.SIGTERM, signal.SIGQUIT]:
        signal.signal(sig, terminate)
    app = Flask(__name__)
    api = Api(app)
    DAQAppCMD = AppCommand.pass_daq_app(app_state)
    api.add_resource(DAQAppCMD, "/command", methods=["POST"])
    app.add_url_rule("/", "index", index)
    server_ready = threading.Event()
    shutdown_event = threading.Event()

    def run_flask_app(
        app: Flask,
        host: str,
        port: int,
        event: threading.Event,
        server_container: dict[str, WSGIServer],
    ) -> None:
        server = make_server(host, port, app)
        server_container["server"] = server
        server.timeout = 0.5
        event.set()

        # Don't use serve_forever() directly if you need external control
        # Use a loop that checks the shutdown event
        while not shutdown_event.is_set():
            server.handle_request()  # Handles one request at a time

        server.shutdown()
        server.server_close()

    url = urlparse(url) if isinstance(url, str) else url
    flask_url = url.geturl().replace("rest://", "http://")

    if not url.hostname or url.port is None:
        log.error("Invalid command facility URL: missing hostname or port")
        exit(1)

    server_container: dict[str, WSGIServer] = {}
    flask_thread = threading.Thread(
        target=run_flask_app,
        kwargs={
            "app": app,
            "host": url.hostname,
            "port": url.port,
            "event": server_ready,
            "server_container": server_container,
        },
        name="flask_thread",
        daemon=True,  # Ensure the thread exits when the main program exits
    )
    flask_thread.start()

    if not server_ready.wait(timeout=10):
        log.error("Timed out waiting for FakeDAQ app to start")
        exit(1)

    time.sleep(1)
    for i in range(10):
        log.debug(f"Trying to connect to Flask app, attempt {i + 1}/10")
        response = requests.get(flask_url + "/")
        log.debug(f"Response: {response.status_code}")
        if response.status_code == 200:
            log.info("Fake DAQ app started successfully and is responding to requests")
            break
        if i == 9:
            log.error("Failed to start fake DAQ app")
            exit(1)
        time.sleep(1)

    connectivity_service_thread.start()

    # FAILURE TESTING LOGIC BLOCK - DEATH POST BOOT
    # The following block simulates a failure after the initialization of the app. This
    # serves uniquely to test the robustness of the Run Control when an app fails to
    # complete initialization, and should not be used for any other purpose. The
    # environment variable is set in the configuration file that tests this behaviour.
    ft_die_post_boot: bool = (
        os.getenv("DRUNC_FT_PROCESS_DEATH_POST_BOOT", "false").lower() == "true"
    )
    if ft_die_post_boot and ft_app_to_die_boot == name:
        log.warning(f"Simulating death of {name} post boot")
        terminate()

    log.info(
        "Fake DAQ application is running and publishing to connectivity service. Press Ctrl+C to exit."
    )


if __name__ == "__main__":
    main()
