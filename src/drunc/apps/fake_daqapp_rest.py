"""This is a fake DAQ application that doesn't do anything, but should talk in the same way to the run control"""

import argparse
import copy as cp
import random
import threading
import time
from urllib.parse import urlparse

import conffwk
import requests
from flask import Flask, Response, request
from flask_restful import Api, Resource

from drunc.connectivity_service.client import ConnectivityServiceClient
from drunc.utils.utils import (
    get_logger,
    get_new_port,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
    setup_standard_loggers,
)

__version__ = "1.0.0"
setup_root_logger(log_level="info")
setup_standard_loggers()


class AppState:
    def __init__(self, app_name: str):
        self.appname = app_name
        self.state = "INITIAL"
        self.executing_command = False
        self.log = get_logger("fake_daqapp_rest.AppState")

    def send_response_to_response_listener(
        self, address: str, txt: str, success: bool = True, data: dict = {}
    ):
        data_to_send = {
            "success": success,
            "result": txt,
            "appname": self.appname,
            "data": data,
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
        self, req_data, answer_port, answer_host, remote_host
    ) -> Response:
        reply_address = (
            f"http://{answer_host}:{answer_port}/response"
            if answer_host
            else f"{remote_host}:{answer_port}/response"
        )

        entry_state = req_data["entry_state"]
        exit_state = req_data["exit_state"]
        command_id = req_data["id"]
        data = req_data.get("data", {})

        if self.executing_command:
            response_txt = "Already executing a command!!"
            self.log.info(response_txt)
            self.send_response_to_response_listener(
                address=reply_address,
                txt=response_txt,
                success=False,
            )
            return

        time_spent = data.get("execution-time", random.randint(1, 5))

        worries = random.randint(0, time_spent)

        if entry_state != "*" and self.state != entry_state.upper():
            info = f"DAQ Application is in state {self.state} and command {command_id} requires to be in state {entry_state.upper()} to execute. Not executing"
            self.log.info(info)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=info,
            )
            return

        self.log.info(f"Executing {command_id}")

        self.executing_command = True

        if data.get("seg_fault"):
            time.sleep(worries)
            info = "<seeeeeeeeeg fauuuuuuuult message>"
            self.log.info(info)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=info,
            )
            self.executing_command = False
            exit(data["seg_fault"])

        if data.get("throw"):
            time.sleep(worries)
            what = (
                "This is an eRrOr, YoU hAvE bEeN vErY nAuGhTy (aka task failed successfully)",
            )
            self.log.info(what)
            self.send_response_to_response_listener(
                success=False,
                address=reply_address,
                txt=what,
            )
            self.executing_command = False
            return

        print(f"Sleeping for {time_spent} seconds")

        time.sleep(time_spent)

        info = f"Executed {command_id} successfully, after {time_spent} seconds"
        self.log.info(info)

        self.send_response_to_response_listener(
            success=True,
            address=reply_address,
            txt=info,
        )
        self.state = exit_state.upper()
        self.executing_command = False
        return


"""
Resources for Flask app
"""


class AppCommand(Resource):
    @classmethod
    def pass_daq_app(cls, daq_app):
        cls.daq_app = daq_app
        return cls

    def post(self):
        global app_state

        try:
            data = request.get_json(force=True)
        except:
            return "Not a JSON command!\n", 406

        log = get_logger("fake_daqapp_rest.AppCommand")
        log.info(f"GET request with args: {data}")
        thread = threading.Thread(
            target=self.daq_app.execute_command,
            kwargs={
                "req_data": cp.deepcopy(data),
                "answer_port": request.headers["X-Answer-Port"],
                "answer_host": request.headers.get("X-Answer-Host"),
                "remote_host": request.remote_addr,
            },
        )
        thread.start()

        return "Command received\n", 202


def update_connectivity_service(name, connectivity_service, interval, url):
    while True:
        connectivity_service.publish(
            name + "_control",
            url,
            "RunControlMessage",
        )
        time.sleep(interval)


def index():
    return f"Fake DAQ app v{__version__}"


def get_address_for_conn_srv(hostname):
    return f"rest://{hostname}:{get_new_port()}"


def main():
    parser = argparse.ArgumentParser(
        prog="FakeApplication",
        description="This is a fake application that communicate in the same way with the RunControl as the DAQApplication (thru REST)",
    )
    parser.add_argument(
        "-n", "--name", required=True, help="The name of the app in the response"
    )
    parser.add_argument(
        "-d",
        "--configurationService",
        required=True,
        help="This is a dummy argument in this case",
    )
    parser.add_argument(
        "-c",
        "--commandFacility",
        required=False,
        help="Where the fake app should get its command from",
    )
    parser.add_argument(
        "-i",
        "--informationService",
        default="stdout://flat",
        help="This is a dummy argument in this case",
    )
    parser.add_argument(
        "-l", "--log_level", default="info", help="Logging level minimum threshold"
    )
    parser.add_argument(
        "-p",
        "--partition",
        default="global",
        help="This is a dummy argument in this case",
    )
    parser.add_argument("-s", "--session", default="test", help="name of session")
    args = parser.parse_args()

    name = args.name
    print(f"Name: {name}")
    app_state = AppState(name)

    log = get_logger("fake_daqapp_rest", rich_handler=True)
    conf = conffwk.Configuration(args.configurationService)
    session = conf.get_dal(
        class_name="Session",
        uid=args.session,
    )
    connectivity_service_address = (
        session.connectivity_service.host
        + ":"
        + str(session.connectivity_service.service.port)
    )
    if not args.commandFacility:
        log.error("No command facility passed, exiting")
        exit(1)

    url = urlparse(resolve_localhost_and_127_ip_to_network_ip(args.commandFacility))
    if url.scheme != "rest":
        log.exception("DAQApplication communication scheme must be rest")
        exit(1)

    log.debug(f"Initializing fake_daq_application with address {url}")
    if url.port == 0:
        url = get_address_for_conn_srv(url.hostname)
    log.info(f"Communication address is {url}")

    interval = 2

    connectivity_service = ConnectivityServiceClient(
        session=args.session,
        address=connectivity_service_address,
    )

    connectivity_service_thread = threading.Thread(
        target=update_connectivity_service,
        args=(name, connectivity_service, interval, url),
        name="connectivity_service_updating_thread",
    )

    # Doesn't do what is expected, probably flask
    # def terminate(signum, sigframe):
    #     connectivity_service_thread.join()
    #     log.info("Connectivity service terminated")
    #     exit(1)
    # for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM, signal.SIGQUIT]:
    #     signal.signal(sig, terminate)
    app = Flask(__name__)
    api = Api(app)
    DAQAppCMD = AppCommand.pass_daq_app(app_state)
    api.add_resource(DAQAppCMD, "/command", methods=["POST"])
    app.add_url_rule("/", "index", index)

    url = urlparse(url)
    flask_url = url.geturl().replace("rest://", "http://")

    log.info(f"Starting FakeDAQ app on {flask_url}")
    flask_thread = threading.Thread(
        target=app.run,
        kwargs={"host": url.hostname, "port": url.port, "debug": False},
        name="flask_thread",
    )

    flask_thread.start()

    for i in range(10):
        response = requests.get(flask_url + "/")
        log.info(f"Response: {response.status_code}")
        if response.status_code == 200:
            break
        if i == 9:
            log.error("Failed to start fake DAQ app")
            exit(1)
        time.sleep(1)

    connectivity_service_thread.start()


if __name__ == "__main__":
    main()
