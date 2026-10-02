import concurrent
import os
import signal

import click
import grpc
from daqpytools.logging import logging_log_levels
from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
from druncschema.token_pb2 import Token

from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.controller import Controller
from drunc.grpc_settings import (
    CONTROLLER_SERVER_GRPC_CONFIG,
    CONTROLLER_SERVER_GRPC_MAX_WORKERS,
)
from drunc.utils.configuration import OKSKey
from drunc.utils.utils import (
    get_logger,
    get_root_logger,
    resolve_localhost_and_127_ip_to_network_ip,
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
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
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
    get_root_logger(log_level)
    log = get_logger(
        "controller.core.ctrl_cli", file_handler_path=None, rich_handler=False
    )

    token = Token(
        user_name="controller_init_token",
        token="",
    )

    controller_configuration = ControllerConfHandler.from_oks(
        url=configurationservice,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=name,
            session=configurationid,  # some of the function for include/exclude require the full dal of the session
        ),
        session_name=sessionname,
    )

    commandfacility = resolve_localhost_and_127_ip_to_network_ip(commandfacility)

    ctrlr = Controller(
        name=name,
        session=sessionname,
        configuration=controller_configuration,
        token=token,
    )

    def serve(listen_addr: str) -> None:
        server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=CONTROLLER_SERVER_GRPC_MAX_WORKERS
            ),
            options=CONTROLLER_SERVER_GRPC_CONFIG,
        )
        add_ControllerServicer_to_server(ctrlr, server)
        port = server.add_insecure_port(listen_addr)

        server.start()
        log.debug(f"'{ctrlr.name}' was started on '{port}'")
        return server, port

    def controller_shutdown():
        log.info("Requested termination")
        log.info("Calling ctrlr.terminate()")
        ctrlr.terminate()
        log.info("ctrlr.terminate() completed")

    def kill_me(sig, frame):
        log_km = get_logger("controller.iface.kill_me")
        log_km.info("Sending SIGKILL")
        if ctrlr.top_segment_controller:
            ctrlr.connectivity_service.retract_partition(fail_quickly=True)
        pgrp = os.getpgid(os.getpid())
        os.killpg(pgrp, signal.SIGKILL)

    def shutdown(sig, frame):
        log.info(f"Shutting down gracefully (received signal: {sig})")
        try:
            controller_shutdown()
        except Exception as e:
            log.exception(e)
            kill_me(sig, frame)

    try:
        server, port = serve(commandfacility)
        server_name = commandfacility.split(":")[0]
        ctrlr.advertise_control_address(f"grpc://{server_name}:{port}")
        ctrlr.init_controller()

        # Add signal handling for gRPC server
        def signal_handler(signum, frame):
            log.info(f"Received signal {signum}, shutting down gRPC server")
            server.stop(grace=2.0)  # Give 2 seconds for graceful shutdown
            log.info("gRPC server shutdown completed")

            try:
                shutdown(signum, frame)
                log.info("shutdown() completed")
            except Exception as e:
                log.exception(e)
            finally:
                log.info("Exiting...")
                os._exit(0)

        # Register signal handlers for the server
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)

        server.wait_for_termination(timeout=None)

    except Exception as e:
        log.exception(e)
