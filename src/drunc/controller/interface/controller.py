import concurrent
import os
import signal

import click
import grpc
from druncschema.controller_pb2_grpc import add_ControllerServicer_to_server
from druncschema.token_pb2 import Token

from drunc.controller.configuration import ControllerConfHandler
from drunc.controller.controller import Controller
from drunc.utils.configuration import OKSKey, find_configuration, parse_conf_url
from drunc.utils.utils import (
    get_logger,
    log_levels,
    resolve_localhost_and_127_ip_to_network_ip,
    setup_root_logger,
    validate_command_facility,
)


@click.command()
@click.argument("boot-configuration", type=str)
@click.argument("command-facility", type=str, callback=validate_command_facility)
@click.argument("application_name", type=str)
@click.argument("session", type=str)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def controller_cli(
    boot_configuration: str,
    command_facility: str,
    application_name: str,
    session: str,
    log_level: str,
):
    """
    Spawns a single controller defined in the boot-configuration file, in a given session identified by its name, with communications defined through the command-facility.\n
    Arguments\n
    boot-configuration - Path to boot configuration, searches DUNEDAQ_DB_PATH if not absolute, e.g. 'config/daqsystemtest/example-configs.data.xml'\n
    command-facility - Facility through which commands should be sent, e.g. grpc://localhost:12345\n
    application_name - Name of application, e.g. 'root-controller'\n
    session - Name of session in boot-configuration, e.g. 'local-2x3-config'
    """
    setup_root_logger(log_level)
    log = get_logger("controller.controller_cli")
    token = Token(
        user_name="controller_init_token",
        token="",
    )

    boot_configuration = find_configuration(boot_configuration)
    conf_path, conf_type = parse_conf_url(boot_configuration)
    controller_configuration = ControllerConfHandler(
        type=conf_type,
        data=conf_path,
        oks_key=OKSKey(
            schema_file="schema/confmodel/dunedaq.schema.xml",
            class_name="RCApplication",
            obj_uid=application_name,
            session=session,  # some of the function for enable/disable require the full dal of the session
        ),
    )

    ctrlr = Controller(
        name=application_name,
        session=session,
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
        command_facility = resolve_localhost_and_127_ip_to_network_ip(command_facility)
        server_name = command_facility.split(":")[0]
        server, port = serve(command_facility)

        ctrlr.advertise_control_address(f"grpc://{server_name}:{port}")
        server.wait_for_termination(timeout=None)

    except Exception as e:
        log.exception(e)
