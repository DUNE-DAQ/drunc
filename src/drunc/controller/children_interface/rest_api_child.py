from drunc.controller.children_interface.child_node import ChildNode, ChildNodeType

import threading
from typing import NoReturn

class ResponseDispatcher(threading.Thread):

    STOP="RESPONSE_QUEUE_STOP"

    def __init__(self,listener):
        threading.Thread.__init__(self)
        self.listener = listener
        from logging import getLogger
        self.log = getLogger('ResponseDispatcher')

    def run(self) -> NoReturn:
        self.log.debug(f'ResponseDispatcher starting to run')

        while True:
            # self.log.debug(f'starting to iterating: {self.listener.queue.qsize()}')
            # self.log.debug(f'Queue pointer {self.listener.queue}')
            # try:
            r = self.listener.queue.get()
            #     self.log.debug(f'ResponseDispatcher got the following answer: {r}')
            # except:
            #     self.log.debug(f'ResponseDispatcher nothing')
            #     continue

            if r == self.STOP:
                self.log.debug(f'ResponseDispatcher STOP')
                break
            self.listener.notify(r)

    def stop(self) -> NoReturn:
        self.listener.queue.put_nowait(self.STOP)
        self.join()

    def __str__(self):
        return f'\'{self.name}@{self.uri}\' (type {self.node_type})'

class ResponseListener:
    _instance = None
    manager = None
    def __init__(self):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('Call get() instead')

    @classmethod
    def get(cls):
        from logging import getLogger
        log = getLogger('ResponseListener.get')
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            from drunc.utils.utils import get_new_port
            cls.port = get_new_port()
            from flask import Flask
            from flask_restful import Api
            cls.app = Flask('response-listener')
            cls.api = Api(cls.app)
            from multiprocessing import Queue
            cls.queue = Queue()
            cls.handlers = {}

            cls.dispatcher = ResponseDispatcher(cls)
            cls.dispatcher.start()

            from flask import request
            def index():
                from logging import getLogger
                log = getLogger('ResponseListener.index')
                json = request.get_json(force=True)
                log.debug(f'Received {json}')
                # enqueue command reply
                cls.queue.put(json)
                log.debug(f'Queue size {cls.queue.qsize()}')
                log.debug(f'Queue pointer {cls.queue}')
                return "Response received"

            def get():
                return "ready"

            cls.app.add_url_rule("/response", "index", index, methods=["POST"])
            cls.app.add_url_rule("/", "get", get, methods=["GET"])
            from drunc.utils.flask_manager import FlaskManager
            cls.manager = FlaskManager(
                port = cls.port,
                app = cls.app,
                name = "response-listener-flaskmanager"
            )

            cls.manager.start()
            while not cls.manager.is_ready():
                from time import sleep
                sleep(0.1)

        return cls._instance

    @classmethod
    def exists(cls):
        return (cls._instance is not None)

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
        """
        Register a new notification handler

        :param      app:           The application
        :type       app:           str
        :param      handler:       The handler
        :type       handler:       { type_description }

        :rtype:     None

        :raises     RuntimeError:  { exception_description }
        """
        if app in cls.handlers:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(f"Handler already registered with notification listerner for app {app}")

        cls.handlers[app] = handler

    @classmethod
    def unregister(cls, app: str):
        """
        De-register a notification handler

        Args:
            app (str): application name

        """
        if not app in cls.handlers:
            from drunc.exceptions import DruncException
            raise DruncException(f"No handler registered for app {app}")
        del cls.handlers[app]

    @classmethod
    def notify(cls, reply: dict):
        if 'appname' not in reply:
            from drunc.exceptions import DruncException
            raise DruncException(f"No 'appname' field in reply {reply}")

        app = reply["appname"]

        if not app in cls.handlers:
            cls.log.warning(f"Received notification for unregistered app '{app}'")
            return

        cls.handlers[app].notify(reply)

class ResponseTimeout(Exception):
    pass
class NoResponse(Exception):
    pass

class AppCommander:

    def __init__(
            self,
            app_name: str,
            app_host: str,
            app_port: int,
            response_host: str,
            response_port: int,
            proxy_host:str=None,
            proxy_port:int=None,
            ):

        self.app_host = app_host
        self.app_port = app_port
        self.response_host = response_host
        self.response_port = response_port
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

        from logging import getLogger
        self.app = app_name
        self.log = getLogger(f'{self.app}-commander')
        self.app_url = f"http://{self.app_host}:{self.app_port}/command"

        from queue import Queue
        self.response_queue = Queue()
        self.sent_cmd = None

    def notify(self, response):
        self.response_queue.put(response)

    def ping(self):
        self.log.debug(f'Pinging \'{self.app}\'')
        if self.proxy_host and self.proxy_port:
            self.log.debug(f'Proxy: \'{self.proxy_host}:{self.proxy_port}\'')
        self.log.debug(f'App: \'{self.app_host}:{self.app_port}\'')

        import socket, socks

        if not self.proxy_host and not self.proxy_port:
            self.log.debug(f'NO proxy setup')
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
        else:
            self.log.debug(f'Proxy setup')
            s = socks.socksocket(socket.AF_INET, socket.SOCK_STREAM)
            s.set_proxy(socks.SOCKS5, self.proxy_host, self.proxy_port)
            s.settimeout(1)
        try:
            s.connect((self.app_host, self.app_port))
            s.shutdown(2)
            self.log.debug(f'\'{self.app}\' pings')
            return True
        except Exception as e:
            self.log.debug(f'\'{self.app}\' does not ping, reason: \'{str(e)}\'')
            return False

    def send_command(
            self,
            cmd_id: str,
            cmd_data: dict,
            entry_state="ANY",
            exit_state="ANY"):
        # here we go again...
        module_data = {
            "modules": [
                {
                    "data": cmd_data,
                    "match": ""
                }
            ]
        }

        cmd = {
            "id": cmd_id,
            "data": module_data,
            "entry_state": entry_state,
            "exit_state": exit_state,
        }
        import json
        self.log.debug(json.dumps(cmd, sort_keys=True, indent=2))

        headers = {
            "content-type": "application/json",
            "X-Answer-Port": str(self.response_port),
        }
        if not self.response_host is None:
            headers['X-Answer-Host'] = self.response_host

        self.log.debug(headers)
        import requests
        ack = requests.post(
            self.app_url,
            data=json.dumps(cmd),
            headers=headers,
            timeout=1.,
            proxies={
                'http': f'socks5h://{self.response_host}:{self.response_port}',
                'https': f'socks5h://{self.response_host}:{self.response_port}'
            } if self.proxy_host else None
        )

        self.log.debug(f"Ack to {self.app}: {ack.status_code}")
        self.sent_cmd = cmd_id


    def check_response(self, timeout:int=0) -> dict:
        """Check if a response is present in the queue

        Args:
            timeout (int, optional): Timeout in seconds

        Returns:
            dict: Command response is json

        Raises:
            NoResponse: Description
            ResponseTimeout: Description
        """
        import queue
        try:
            # self.log.info(f"Checking for answers from {self.app} {self.sent_cmd}")
            r = self.response_queue.get(block=(timeout>0), timeout=timeout)
            self.log.debug(f"Received reply from {self.app} to {self.sent_cmd}")
            self.sent_cmd = None

        except queue.Empty:
            if not timeout:
                raise NoResponse(f"No response available from {self.app} for command {self.sent_cmd}")
            else:
                self.log.error(f"Timeout while waiting for a reply from {self.app} for command {self.sent_cmd}")
                raise ResponseTimeout(
                    f"Timeout while waiting for a reply from {self.app} for command {self.sent_cmd}"
                )

        return r


'''
This is a very simple FSM, because it doesn't exist on the server side (appfwk),
and hence cannot be figured from there
'''
class StateRESTAPI:

    def __init__(self, initial_state='initial'):
        # We'll wrap all these in a mutex for good measure
        from threading import Lock
        self._state_lock = Lock()
        self._executing_command = False
        self._assumed_operational_state = initial_state
        self._included = True
        self._errored = False


    def executing_command_mark(self):
        with self._state_lock:
            self._executing_command = True

    def end_command_execution_mark(self):
        with self._state_lock:
            self._executing_command = False

    def new_operational_state(self, new_state):
        with self._state_lock:
            self._assumed_operational_state = new_state

    def get_operational_state(self):
        with self._state_lock:
            return self._assumed_operational_state

    def get_executing_command(self):
        with self._state_lock:
            return self._executing_command

    def include(self):
        with self._state_lock:
            self._included = True

    def exclude(self):
        with self._state_lock:
            self._included = False

    def included(self):
        with self._state_lock:
            return self._included

    def excluded(self):
        with self._state_lock:
            return not self._included

    def to_error(self):
        with self._state_lock:
            self._errored = True

    def fix_error(self):
        with self._state_lock:
            self._errored = False

    def in_error(self):
        with self._state_lock:
            return self._errored

from drunc.utils.configuration import ConfHandler
from drunc.exceptions import DruncSetupException

class BadArgumentInConf(DruncSetupException):
    pass

class RESTAPIChildNodeConfHandler(ConfHandler):

    def get_uri(self):
        ### https://github.com/DUNE-DAQ/appfwk/blob/production/v4/src/CommandLineInterpreter.hpp#L55
        # bpo::options_description desc(descstr.str());
        # desc.add_options()
        #    ("name,n", bpo::value<std::string>()->required(), "Application name")
        #    ("partition,p", bpo::value<std::string>()->default_value("global"), "Partition name")
        #    ("commandFacility,c", bpo::value<std::string>()->required(), "CommandFacility URI")
        #    ("informationService,i", bpo::value<std::string>()->default_value("stdout://flat"), "Information Service URI")
        #    ("configurationService,d", bpo::value<std::string>()->required(), "Configuration Service URI")
        #    ("help,h", "produce help message");

        # <data val="--name"/>
        # <data val="ru-02"/>
        # <data val="-c"/>
        # <data val="rest://localhost:3335"/>
        # <data val="-i"/>
        # <data val="kafka://monkafka.cern.ch:30092/opmon"/>
        # <data val="--configurationService"/>
        # <data val="oksconfig:///nfs/home/plasorak/NAFD24-02-08-OKS/swdir/sourcecode/appdal/test/config/test-session.data.xml"/>


        ### FAILED ATTEMPT
        # import click
        # def store():
        #     commandFacility = None

        # # main = fake_main_app

        # @click.command()
        # @click.option("--name", "-n")
        # @click.option("--partition", "-p")
        # @click.option("--commandFacility", "-c")
        # @click.option("--informationService", "-i")
        # @click.option("--configurationService", "-d")
        # @click.pass_context
        # def fake_main_app(ctx, name, partition, commandfacility, informationservice, configurationservice):
        #     ctx.obj.commandFacility = commandfacility

        # cmd = fake_main_app

        # obj = store()
        CLAs=self.data.commandline_parameters

        # context = cmd.make_context(info_name='dmmy', args=CLAs, obj=obj)
        # fake_main_app.parse_args(args=CLAs, ctx=context)
        # self.log.info(context.__dict__)
        # self.log.info(obj.commandFacility)
        cmd_arg_index = None
        for i, CLA in enumerate(CLAs):
            if CLA in ["--commandFacility", "-c"]:
                cmd_arg_index = i+1
                break

        if len(CLAs) <= cmd_arg_index:
            BadArgumentInConf(f'--commandFacility (or -c) was not specified for this app in its command line parameters!')
        # fake_main_app.main(args=CLAs, obj=obj)#.invoke(ctx)
        # # self.log.info(obj.commandFacility)

        return CLAs[cmd_arg_index]
        # except Exception as e:
        #     raise BadArgumentInConf(f'Error in the configuration, the 2nd CLA seems to be incorrect: {e.message}. CLA:\'{self.data.controller.commandline_parameters[1]}\'')

from drunc.fsm.configuration import FSMConfHandler

class RESTAPIChildNode(ChildNode):
    def __init__(self, name, configuration:RESTAPIChildNodeConfHandler, fsm_configuration:FSMConfHandler):
        super(RESTAPIChildNode, self).__init__(
            name = name,
            node_type = ChildNodeType.REST_API
        )

        from logging import getLogger
        self.log = getLogger(f'{name}-rest-api-child')

        self.response_listener = ResponseListener.get()

        import socket
        response_listener_host = socket.gethostname()
        uri = configuration.get_uri()
        from urllib.parse import urlparse
        uri = urlparse(uri)
        self.app_host, app_port = uri.netloc.split(":")
        self.app_port = int(app_port)

        proxy_host, proxy_port = getattr(configuration.data, "proxy", [None, None])
        proxy_port = int(proxy_port) if proxy_port is not None else None

        self.commander = AppCommander(
            app_name = self.name,
            app_host = self.app_host,
            app_port = self.app_port,
            response_host = response_listener_host,
            response_port = self.response_listener.get_port(),
            proxy_host = proxy_host,
            proxy_port = proxy_port,
        )

        from drunc.fsm.core import FSM
        from drunc.fsm.configuration import FSMConfHandler
        fsmch = FSMConfHandler(fsm_configuration)

        self.fsm = FSM(conf=fsmch)

        self.response_listener.register(self.name, self.commander)

        self.state = StateRESTAPI()

    def __str__(self):
        return f'\'{self.name}@{self.app_host}:{self.app_port}\' (type {self.node_type})'

    def terminate(self):
        pass

    def get_status(self, token):
        from druncschema.controller_pb2 import Status

        return Status(
            name = self.name,
            state = self.state.get_operational_state(),
            sub_state = 'idle' if not self.state.get_executing_command() else 'executing_cmd',
            in_error = self.state.in_error() or not self.commander.ping(), # meh
            included = self.state.included(),
        )

    def propagate_command(self, command, data, token):

        if command == 'exclude':
            self.state.exclude()
        elif command == 'include':
            self.state.include()

        if self.state.excluded():
            return

        # here lies the mother of all the problems
        if command != 'execute_fsm_command':
            self.log.info(f'Ignoring command \'{command}\' sent to \'{self.name}\'')
            return None

        self.log.info(f'Sending \'{command}\' to \'{self.name}\'')

        # from drunc.utils.grpc_utils import unpack_any
        # from druncschema.controller_pb2 import FSMCommand
        # fsm_command = unpack_any(data, FSMCommand)
        from druncschema.controller_pb2 import FSMCommandResponseCode

        entry_state = self.state.get_operational_state()
        transition = self.fsm.get_transition(data.command_name)
        exit_state = self.fsm.get_destination_state(entry_state, transition)
        self.state.executing_command_mark()
        import json
        try:
            self.commander.send_command(
                cmd_id = data.command_name,
                cmd_data = json.loads(data.data),
                entry_state = entry_state.upper(),
                exit_state = exit_state.upper(),
            )
            r = self.commander.check_response(10)
            if not r['success']:
                self.log.error(r['result'])
                self.state.to_error()
                return FSMCommandResponseCode.UNSUCCESSFUL
        except Exception as e:
            self.log.error(str(e))
            self.state.to_error()
            return FSMCommandResponseCode.UNSUCCESSFUL
        self.state.end_command_execution_mark()
        self.state.new_operational_state(exit_state)
        return FSMCommandResponseCode.SUCCESSFUL
