import json
import multiprocessing
import queue
import socket
import threading
import time
from typing import NoReturn

import requests
import socks
from druncschema.controller_pb2 import FSMCommandResponse, FSMResponseFlag
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from flask import Flask, request
from flask_restful import Api

from drunc.controller.children_interface.client_side_child import ClientSideChild
from drunc.controller.exceptions import ChildError
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
        try:
            s.connect((self.app_host, self.app_port))
            s.shutdown(2)
            self.log.debug(f"'{self.app}' pings")
            return True
        except Exception as e:
            self.log.error(f"'{self.app}' does not ping, reason: '{e!s}'")
            return False

    def send_command(
        self, cmd_id: str, cmd_data: dict, entry_state="ANY", exit_state="ANY"
    ):
        # here we go again...
        module_data = {"modules": [{"data": cmd_data, "match": ""}]}

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
        try:
            ack = requests.post(
                self.app_url,
                data=json.dumps(cmd),
                headers=headers,
                timeout=1.0,
                proxies={
                    "http": f"socks5h://{self.response_host}:{self.response_port}",
                    "https": f"socks5h://{self.response_host}:{self.response_port}",
                }
                if self.proxy_host
                else None,
            )
        except requests.ConnectionError:
            self.log.error(f"Connection error to {self.app_url}")
            raise CouldnotSendCommand(f"Connection error to {self.app_url}")

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

    # def terminate(self):
    #     pass

    def get_endpoint(self):
        return f"rest://{self.app_host}:{self.app_port}"

    # def get_status(self, token):

    #     status = Status(
    #         state = self.state.get_operational_state(),
    #         sub_state = 'idle' if not self.state.get_executing_command() else 'executing_cmd',
    #         in_error = self.state.in_error() or not self.commander.ping(), # meh
    #         included = self.state.included(),
    #     )
    #     return Response(
    #         name = self.name,
    #         token = None,
    #         data = pack_to_any(status),
    #         flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
    #         children = [],
    #     )

    # def propagate_command(self, command:str, data, token:Token) -> Response:
    #     if command == 'exclude':
    #         self.state.exclude()
    #         return Response(
    #             name = self.name,
    #             token = token,
    #             data = pack_to_any(
    #                 PlainText(
    #                     text=f"\'{self.name}\' excluded"
    #                 )
    #             ),
    #             flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
    #             children = []
    #         )
    #     elif command == 'include':
    #         self.state.include()
    #         return Response(
    #             name = self.name,
    #             token = token,
    #             data = pack_to_any(
    #                 PlainText(
    #                     text=f"\'{self.name}\' included"
    #                 )
    #             ),
    #             flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
    #             children = []
    #         )

    #     if self.state.excluded():
    #         return Response(
    #             name = self.name,
    #             token = token,
    #             data = pack_to_any(
    #                 FSMCommandResponse(
    #                     flag = FSMResponseFlag.FSM_NOT_EXECUTED_EXCLUDED,
    #                     command_name = data.command_name,
    #                     data = None
    #                 )
    #             ),
    #             flag = ResponseFlag.EXECUTED_SUCCESSFULLY,
    #             children = []
    #         )

    #     # here lies the mother of all the problems
    #     if command == 'execute_fsm_command':
    #         return self.propagate_fsm_command(command, data, token)
    #     elif command == 'describe':
    #         return self.describe(token)
    #     else:
    #         self.log.info(f'Ignoring command \'{command}\' sent to \'{self.name}\'')
    #         return Response(
    #             name = self.name,
    #             token = token,
    #             data = None,
    #             flag = ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    #             children = []
    #         )

    def propagate_fsm_command(self, command: str, data, token: Token) -> Response:
        entry_state = self.state.get_operational_state()
        transition = self.fsm.get_transition(data.command_name)
        exit_state = self.fsm.get_destination_state(entry_state, transition)
        self.state.executing_command_mark()
        self.log.info(f"Sending '{data.command_name}' to '{self.name}'")

        try:
            self.commander.send_command(
                cmd_id=data.command_name,
                cmd_data=json.loads(data.data),
                entry_state=entry_state.upper(),
                exit_state=exit_state.upper(),
            )
            self.log.debug(f"Sent '{data.command_name}' to '{self.name}'")
            r = self.commander.check_response(150)

            self.log.debug(f"Got response from '{data.command_name}' to '{self.name}'")

            success = r["success"]

            response_data = pack_to_any(PlainText(text=json.dumps(r)))

            fsm_data = FSMCommandResponse(
                flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY
                if success
                else FSMResponseFlag.FSM_FAILED,
                command_name=data.command_name,
                data=response_data,
            )
            response = Response(
                name=self.name,
                token=token,
                data=pack_to_any(fsm_data),
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
                f"Got error from '{data.command_name}' to '{self.name}': {e!s}"
            )
            self.state.to_error()
            self.log.exception(e)
            raise e

        self.state.end_command_execution_mark()
        self.state.new_operational_state(exit_state)
        return response
