import concurrent
import os
import signal

import click
import grpc
from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
from druncschema.token_pb2 import Token

from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.controller import Controller
from drunc.utils.configuration import ConfTypes, OKSKey
from drunc.utils.utils import (
    create_logger_handler,
    get_logger,
    log_levels,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
    validate_command_facility,
)


@click.command()
@click.option(
    "-s",
    "--sessionName",
    type=str,
    required=True,
    help="Name of session e.g. 'local-2x3-config-username'",
)
@click.option(
    "-k",
    "--configurationId",
    type=str,
    required=True,
    help="Id of session in configuration, e.g. 'local-2x3-config'",
)
@click.option(
    "-n",
    "--name",
    type=str,
    required=True,
    help="Name of application, e.g. 'root-controller'",
)
@click.option(
    "-c",
    "--commandFacility",
    type=str,
    callback=validate_command_facility,
    required=True,
    help="Facility through which commands should be sent, e.g. grpc://localhost:12345",
)
@click.option(
    "-d",
    "--configurationService",
    type=str,
    required=True,
    help="Service to retrieve configuration, e.g. file://config/daqsystemtest/example-configs.data.xml",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def controller_cli(
    sessionname: str,
    configurationservice: str,
    commandfacility: str,
    name: str,
    configurationid: str,
    log_level: str,
):
    """Spawns a single controller defined in the boot-configuration file, in a given session identified by its name, with communications defined through the command-facility.\n"""
    setup_root_logger(log_level)
    log = get_logger("controller.controller_cli")
    create_logger_handler(
        log_file_path=None,
        rich_handler=False,
    )

    token = Token(
        user_name="controller_init_token",
        token="",
    )

    controller_configuration = ControllerConfHandler(
        type=ConfTypes.OKSFileName,
        data=configurationservice,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=name,
            session=configurationid,  # some of the function for enable/disable require the full dal of the session
        ),
    )

    commandfacility = resolve_localhost_and_127_ip_to_network_ip(commandfacility)

    ctrlr = Controller(
        name=name,
        session=sessionname,
        configuration=controller_configuration,
        token=token,
    )

    def serve(listen_addr: str) -> None:
        server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=1))
        add_ControllerServicer_to_server(ctrlr, server)
        port = server.add_insecure_port(listen_addr)

        server.start()
        log.debug(f"'{ctrlr.name}' was started on '{port}'")
        return server, port

    def controller_shutdown():
        log.warning("Requested termination")
        ctrlr.terminate()

    def kill_me(sig, frame):
        l = get_logger("controller.kill_me")
        l.info("Sending SIGKILL")
        pgrp = os.getpgid(os.getpid())
        os.killpg(pgrp, signal.SIGKILL)

    def shutdown(sig, frame):
        log.info("Shutting down gracefully")
        try:
            controller_shutdown()
        except Exception as e:
            log.exception(e)
            kill_me(sig, frame)

    signal.signal(signal.SIGHUP, kill_me)
    signal.signal(signal.SIGINT, shutdown)

    try:
        server, port = serve(commandfacility)
        server_name = commandfacility.split(":")[0]
        ctrlr.advertise_control_address(f"grpc://{server_name}:{port}")
        server.wait_for_termination(timeout=None)

    except Exception as e:
        log.exception(e)
