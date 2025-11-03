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
        session_name=sessionname,
    )

    commandfacility = resolve_localhost_and_127_ip_to_network_ip(commandfacility)

    ctrlr = Controller(
        name=name,
        session=sessionname,
        configuration=controller_configuration,
        token=token,
    )

# In src/drunc/controller/interface/controller.py

    def serve(listen_addr: str) -> tuple:
        server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
        add_ControllerServicer_to_server(ctrlr, server)

        # --- MODIFIED BINDING LOGIC ---
        bind_addr = listen_addr # Default fallback
        actual_port = 0
        try:
            # Extract port, removing potential grpc:// prefix and host part
            port_str = listen_addr.split(':')[-1]
            port = int(port_str)
            
            # We MUST bind to '[::]' (all interfaces) for HostPort to work
            bind_addr = f'[::]:{port}' 
            
            log.info(f"Original listen address '{listen_addr}', binding server to '{bind_addr}'.")
            actual_port = server.add_insecure_port(bind_addr) # Bind here
            
            if actual_port == 0 and port != 0: # Check if binding failed (port=0 is OK)
                 raise RuntimeError(f"Failed to bind server to {bind_addr}, port came back as 0.")
            elif port == 0 and actual_port != 0:
                 log.info(f"OS assigned port {actual_port} for binding '[::]:0'")
                 port = actual_port # Update port to the one assigned
            elif actual_port != port:
                 log.warning(f"Requested port {port} but bound to {actual_port}. Check for conflicts.")

        except (ValueError, IndexError, RuntimeError) as e:
            log.critical(f"CRITICAL: Failed to parse port or bind server: {e}. Attempting fallback...")
            # Fallback: Try binding to the original address directly
            try:
                actual_port = server.add_insecure_port(listen_addr)
                if actual_port == 0:
                     raise RuntimeError(f"Fallback bind to '{listen_addr}' also failed (port 0).")
                log.warning(f"Bound to fallback address '{listen_addr}' on port {actual_port}. HostPort might not work correctly.")
                bind_addr = listen_addr # Update bind_addr for logging
            except Exception as fallback_e:
                 log.critical(f"CRITICAL: Fallback server bind failed: {fallback_e}")
                 sys.exit(1) # Exit pod on total bind failure
        # --- END MODIFIED BINDING LOGIC ---

        server.start()
        log.info(f"'{ctrlr.name}' gRPC server started, listening internally on '{bind_addr}' (reported port: {actual_port})")
        return server, actual_port # Return the port it *actually* bound to

    def controller_shutdown():
        log.info("Requested termination")
        log.info("Calling ctrlr.terminate()")
        ctrlr.terminate()
        log.info("ctrlr.terminate() completed")

    def kill_me(sig, frame):
        l = get_logger("controller.kill_me")
        l.info("Sending SIGKILL")
        if ctrlr.top_segment_controller:
            if hasattr(ctrlr, "connectivity_service") and ctrlr.connectivity_service:
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
        # Pass commandfacility (which becomes listen_addr inside serve)
        server, actual_port_bound = serve(commandfacility)

        # --- MODIFIED ADVERTISE LOGIC ---
        # We need to advertise the EXTERNAL address, not the internal '[::]'
        advertise_host = commandfacility
        if advertise_host.startswith('grpc://'):
             advertise_host = advertise_host[len('grpc://'):]
        advertise_host = advertise_host.split(':')[0] # Get 'localhost' or IP from -c arg

        # Resolve 'localhost' to the node's real, external IP for advertising
        advertise_host_resolved = resolve_localhost_and_127_ip_to_network_ip(advertise_host)
        
        # Use the actual port the server bound to
        advertise_address = f"grpc://{advertise_host_resolved}:{actual_port_bound}"
        
        log.info(f"Advertising controller address as: {advertise_address}")
        # Update the controller's internal URI, which is used by describe()
        ctrlr.advertise_control_address(advertise_address) 
        # --- END MODIFIED ADVERTISE LOGIC ---

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
        log.critical("Controller_cli failed to start, exiting.")
        controller_shutdown() # Try to clean up
        sys.exit(1) # Exit with error

