import json
import multiprocessing
import queue
import socket
import threading
import time
from json import JSONDecodeError
from typing import NoReturn

import requests
import socks
from druncschema.controller_pb2 import (
    ExecuteFSMCommandResponse,
    FSMCommand,
    FSMResponseFlag,
    Status,
    StatusResponse,
)
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from flask import Flask, request
from flask_restful import Api

from drunc.controller.children_interface.client_side_child import ClientSideChild
from drunc.controller.exceptions import ChildError, ExpertCommandException
from drunc.exceptions import DruncException, DruncSetupException
from drunc.fsm.configuration import FSMConfHandler
from drunc.fsm.core import FSM
from drunc.utils.configuration import ConfHandler
from drunc.utils.flask_manager import FlaskManager
from drunc.utils.grpc_utils import pack_to_any
from drunc.utils.utils import ControlType, get_logger, get_new_port


class ResponseTimeout(ChildError):
    pass


class NoResponse(ChildError):
    pass


class CouldnotSendCommand(ChildError):
    pass


class ResponseDispatcher(threading.Thread):
    STOP = "RESPONSE_QUEUE_STOP"

    def __init__(self, listener):
        threading.Thread.__init__(self)
        self.listener = listener
        self.log = get_logger("controller.ResponseDispatcher")

    def run(self) -> NoReturn:
        self.log.debug("ResponseDispatcher starting to run")

        while True:
            # self.log.debug(f'starting to iterating: {self.listener.queue.qsize()}')
            # self.log.debug(f'Queue pointer {self.listener.queue}')
            # try:
            r = self.listener.queue.get()
            self.log.debug(f"ResponseDispatcher got the following answer: {r}")
            # except:
            #     self.log.debug(f'ResponseDispatcher nothing')
            #     continue

            if r == self.STOP:
                self.log.debug("ResponseDispatcher STOP")
                break
            self.listener.notify(r)

    def stop(self) -> NoReturn:
        self.listener.queue.put_nowait(self.STOP)
        self.join()

    def __str__(self):
        return f"'{self.name}@{self.uri}' (type {self.node_type})"


class ResponseListener:
    _instance = None
    manager = None
    import threading

    _lock = threading.Lock()

    def __init__(self):
        raise DruncSetupException("Call get() instead")

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls.__new__(cls)
                cls.port = get_new_port()
                cls.app = Flask("response-listener")
                cls.api = Api(cls.app)
                cls.queue = multiprocessing.Queue()
                cls.handlers = {}

                cls.dispatcher = ResponseDispatcher(cls)
                cls.dispatcher.start()

                def index():
                    log = get_logger("controller.ResponseListener")
                    json = request.get_json(force=True)
                    log.debug(f"Received {json}")
                    # enqueue command reply
                    cls.queue.put(json)
                    log.debug(f"Queue size {cls.queue.qsize()}")
                    log.debug(f"Queue pointer {cls.queue}")
                    return "Response received"

                def get():
                    return "ready"

                cls.app.add_url_rule("/response", "index", index, methods=["POST"])
                cls.app.add_url_rule("/", "get", get, methods=["GET"])
                cls.manager = FlaskManager(
                    port=cls.port, app=cls.app, name="response-listener-flaskmanager"
                )

                cls.manager.start()
                while not cls.manager.is_ready():
                    time.sleep(0.1)

        return cls._instance

    @classmethod
    def exists(cls):
        return cls._instance is not None

    @classmethod
    def get_port(cls):
        return cls.port

    @classmethod
    def __del__(cls):
        cls.terminate()

    @classmethod
    def terminate(cls):
        cls.queue.close()
        cls.queue.join_thread()
        if cls.manager:
            cls.manager.stop()

    @classmethod
    def register(cls, app: str, handler):
        """Register a new notification handler

        :param      app:           The application
        :type       app:           str
        :param      handler:       The handler
        :type       handler:       { type_description }

        :rtype:     None

        :raises     RuntimeError:  { exception_description }
        """
        if app in cls.handlers:
            raise DruncSetupException(
                f"Handler already registered with notification listerner for app {app}"
            )

        cls.handlers[app] = handler

    @classmethod
    def unregister(cls, app: str):
        """De-register a notification handler

        Args:
            app (str): application name

        """
        if app not in cls.handlers:
            raise DruncException(f"No handler registered for app {app}")
        del cls.handlers[app]

    @classmethod
    def notify(cls, reply: dict):
        if "appname" not in reply:
            raise DruncException(f"No 'appname' field in reply {reply}")

        app = reply["appname"]

        if app not in cls.handlers:
            cls.log.warning(f"Received notification for unregistered app '{app}'")
            return

        cls.handlers[app].notify(reply)


class AppCommander:
    def __init__(
        self,
        app_name: str,
        app_host: str,
        app_port: int,
        response_host: str,
        response_port: int,
        proxy_host: str = None,
        proxy_port: int = None,
    ):
        self.app_host = app_host
        self.app_port = app_port
        self.response_host = response_host
        self.response_port = response_port
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

        self.app = app_name
        self.log = get_logger(f"controller.{self.app}-commander")
        self.app_url = f"http://{self.app_host}:{self.app_port}/command"

        self.response_queue = queue.Queue()
        self.sent_cmd = None

    def notify(self, response):
        self.response_queue.put(response)

    def ping(self):
        self.log.debug(f"Pinging '{self.app}'")
        if self.proxy_host and self.proxy_port:
            self.log.debug(f"Proxy: '{self.proxy_host}:{self.proxy_port}'")
        self.log.debug(f"App: '{self.app_host}:{self.app_port}'")

        if not self.proxy_host and not self.proxy_port:
            self.log.debug("NO proxy setup")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
        else:
            self.log.debug("Proxy setup")
            s = socks.socksocket(socket.AF_INET, socket.SOCK_STREAM)
            s.set_proxy(socks.SOCKS5, self.proxy_host, self.proxy_port)
            s.settimeout(1)
        n_tries = 2

        for i_try in range(n_tries):
            try:
                s.connect((self.app_host, self.app_port))
                s.shutdown(2)
                self.log.debug(f"'{self.app}' pings")
                return True

            except Exception as e:
                self.log.error(f"'{self.app}' does not ping, reason: '{e!s}'")
                if i_try == n_tries - 1:
                    return False

    def send_app_command(
        self, cmd_id: str, module_data: dict, entry_state="ANY", exit_state="ANY"
    ):
        # here we go again...
        # module_data = {"modules": [{"data": cmd_data, "match": ""}]}

        cmd = {
            "id": cmd_id,
            "data": module_data,
            "entry_state": entry_state,
            "exit_state": exit_state,
        }
        self.log.debug(json.dumps(cmd, sort_keys=True, indent=2))

        headers = {
            "content-type": "application/json",
            "X-Answer-Port": str(self.response_port),
        }
        if self.response_host is not None:
            headers["X-Answer-Host"] = self.response_host

        self.log.debug(headers)

        n_tries = 2
        for i_try in range(n_tries):
            try:
                ack = requests.post(
                    self.app_url,
                    data=json.dumps(cmd),
                    headers=headers,
                    timeout=1.0,
                    proxies=(
                        {
                            "http": f"socks5h://{self.response_host}:{self.response_port}",
                            "https": f"socks5h://{self.response_host}:{self.response_port}",
                        }
                        if self.proxy_host
                        else None
                    ),
                )
                break
            except requests.ConnectionError as e:
                self.log.error(f"Connection error to {self.app_url}: {e}")
                if i_try == n_tries - 1:
                    raise CouldnotSendCommand(
                        f"Connection error to {self.app_url}"
                    ) from e
                else:
                    self.log.error("Trying again...")

        self.log.debug(f"Ack to {self.app}: {ack.status_code}")
        self.sent_cmd = cmd_id

    def check_response(self, timeout: int = 0) -> dict:
        """Check if a response is present in the queue

        Args:
            timeout (int, optional): Timeout in seconds

        Returns:
            dict: Command response is json

        Raises:
            NoResponse: Description
            ResponseTimeout: Description

        """
        try:
            # self.log.info(f"Checking for answers from {self.app} {self.sent_cmd}")
            r = self.response_queue.get(block=(timeout > 0), timeout=timeout)
            self.log.info(f"Received reply from {self.app} to {self.sent_cmd}")
            self.sent_cmd = None

        except queue.Empty:
            self.log.info(f"Queue empty! {self.app} to {self.sent_cmd}")
            if not timeout:
                raise NoResponse(
                    f"No response available from {self.app} for command {self.sent_cmd}"
                )
            else:
                self.log.error(
                    f"Timeout while waiting for a reply from {self.app} for command {self.sent_cmd}"
                )
                raise ResponseTimeout(
                    f"Timeout while waiting for a reply from {self.app} for command {self.sent_cmd}"
                )
        return r


"""
This is a very simple FSM, because it doesn't exist on the server side (appfwk),
and hence cannot be figured from there
"""


class RESTAPIChildNodeConfHandler(ConfHandler):
    def get_host_port(self):
        for service in self.data.exposes_service:
            if self.data.id + "_control" in service.id:
                return self.data.runs_on.runs_on.id, service.port
        raise DruncSetupException(
            f"REST API child node {self.data.id} does not expose a control service"
        )


class RESTAPIChildNode(ClientSideChild):
    def __init__(
        self,
        name,
        configuration: RESTAPIChildNodeConfHandler,
        fsm_configuration: FSMConfHandler,
        uri,
    ):
        super().__init__(
            name=name,
            node_type=ControlType.REST_API,
            configuration=configuration,
            fsm_configuration=fsm_configuration,
        )

        self.log = get_logger(f"controller.{name}_rest_api_child")

        self.response_listener = ResponseListener.get()

        if fsm_configuration:
            fsmch = FSMConfHandler(fsm_configuration)
            self.fsm = FSM(conf=fsmch)

        response_listener_host = socket.gethostname()

        self.app_host, app_port = uri.split(":")
        self.app_port = int(app_port)

        if self.app_port == 0:
            raise DruncSetupException(
                f"Application {name} does not expose a control service in the configuration, or has not advertised itself to the application registry service, or the application registry service is not reachable."
            )

        proxy_host, proxy_port = getattr(self.configuration.data, "proxy", [None, None])
        proxy_port = int(proxy_port) if proxy_port is not None else None

        self.commander = AppCommander(
            app_name=self.name,
            app_host=self.app_host,
            app_port=self.app_port,
            response_host=response_listener_host,
            response_port=self.response_listener.get_port(),
            proxy_host=proxy_host,
            proxy_port=proxy_port,
        )

        self.response_listener.register(self.name, self.commander)

    def __str__(self):
        return f"'{self.name}@{self.app_host}:{self.app_port}' (type {self.node_type})"

    def get_endpoint(self):
        return f"rest://{self.app_host}:{self.app_port}"

    def status(
        self,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> StatusResponse:
        status = Status(
            state=self.state.get_operational_state(),
            sub_state=(
                "idle" if not self.state.get_executing_command() else "executing_cmd"
            ),
            in_error=self.state.in_error() or not self.commander.ping(),
            included=self.state.included(),
        )

        response = StatusResponse(
            name=self.name,
            status=status,
            children=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        return response

    def execute_fsm_command(
        self,
        command: FSMCommand,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> ExecuteFSMCommandResponse:
        command_name = command.command_name

        response = ExecuteFSMCommandResponse(
            token=None,
            name=self.name,
            command_name=command_name,
            fsm_flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        # Don't execute command if we are excluded.
        if self.state.excluded():
            response.fsm_flag = FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED
            response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY
            return response

        try:
            module_data = json.loads(command.data if command.data else "{}")
        except JSONDecodeError as e:
            self.log.error(f"Error parsing JSON command data: {e}")
            response.fsm_flag = FSMResponseFlag.FSM_FAILED
            response.flag = ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT
            return response

        entry_state = self.state.get_operational_state()
        transition = self.fsm.get_transition(command_name)
        exit_state = self.fsm.get_destination_state(entry_state, transition)
        self.state.executing_command_mark()
        self.log.info(f"Sending '{command_name}' to '{self.name}'")

        try:
            self.commander.send_app_command(
                cmd_id=command_name,
                module_data={"modules": [{"data": module_data, "match": ""}]},
                entry_state=entry_state.upper(),
                exit_state=exit_state.upper(),
            )
            self.log.debug(f"Sent '{command_name}' to '{self.name}'")

            r = self.commander.check_response(150)
            self.log.debug(f"Got response from '{command_name}' to '{self.name}'")

            response.data = json.dumps(r)

            if not r["success"]:
                # The RPC was successful, but the FSM command was not.
                self.log.error(r["result"])
                self.state.to_error()
                response.fsm_flag = FSMResponseFlag.FSM_FAILED
                return response

        except Exception as e:
            self.log.error(f"Got error from '{command_name}' to '{self.name}': {e!s}")
            self.state.to_error()
            response.fsm_flag = FSMResponseFlag.FSM_FAILED
            response.flag = ResponseFlag.UNHANDLED_EXCEPTION_THROWN
            return response

        self.state.end_command_execution_mark()
        self.state.new_operational_state(exit_state)

        return response

    def execute_expert_command(
        self,
        json_string: str,
        target: str = "",
        execute_along_path: bool = True,
        execute_on_all_subsequent_children_in_path: bool = True,
    ) -> Response:
        data_dict = json.loads(json_string)
        example = json.dumps(
            {
                "data": {"modules": [{"data": {"duration": 100}, "match": ""}]},
                "entry_state": "RUNNING",
                "exit_state": "RUNNING",
                "id": "record",
            },
            indent=4,
        )

        if (
            "id" not in data_dict
            or "data" not in data_dict
            or "entry_state" not in data_dict
            or "exit_state" not in data_dict
        ):
            raise ExpertCommandException(
                f"Invalid format for expert command: format should be: {example}, you provided {json_string}"
            )

        command_name = data_dict["id"]
        cmd_data = data_dict["data"]
        entry_state = data_dict["entry_state"].upper()
        exit_state = data_dict["exit_state"].upper()

        if entry_state != exit_state:
            raise ExpertCommandException(
                f"'entry_state' and 'exit_state' must be the same, provided entry_state='{data_dict['entry_state']}' and exit_state='{data_dict['exit_state']}'"
            )

        current_state = self.state.get_operational_state().upper()

        if entry_state not in [current_state, "ANY", "ALL", "", ".*"]:
            raise ExpertCommandException(
                f"Invalid 'entry_state', according to the command the system should be '{data_dict['entry_state']}', application is in state '{current_state}'"
            )

        self.log.info(f"Sending '{command_name}' to '{self.name}'")

        try:
            self.commander.send_app_command(
                cmd_id=command_name,
                module_data=cmd_data,
                entry_state=entry_state,
                exit_state=exit_state,
            )
            self.log.debug(f"Sent '{command_name}' to '{self.name}'")
            r = self.commander.check_response(150)

            self.log.debug(f"Got response from '{command_name}' to '{self.name}'")

            success = r["success"]

            response_data = PlainText(text=json.dumps(r))

            response = Response(
                name=self.name,
                token=None,
                data=pack_to_any(response_data),
                flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
                children={},
            )

            if not success:
                self.log.error(r["result"])
                self.state.to_error()
                response.flag = ResponseFlag.EXECUTED_SUCCESSFULLY  # /!\ The command executed successfully, but the FSM command was not successful
                return response

        except Exception as e:  # OK, we catch all exceptions here, but that's because REST-API are stateless, and we so we need to put the application in error.
            self.log.error(
                f"Got error from '{command_name}' to '{self.name}': {str(e)}"
            )
            self.state.to_error()

            self.log.exception(e)
            raise e

        return response
