import concurrent
import os
import signal
import types

import click
import grpc
from daqpytools.logging import logging_log_levels
from druncschema.run_control_pb2_grpc import add_RunControlServicer_to_server

from drunc.run_control.configuration import (
    get_run_control_server_configuration,
)
from drunc.run_control.run_control import RunControl
from drunc.utils.utils import (
    get_logger,
    get_root_logger,
    resolve_localhost_and_127_ip_to_network_ip,
)

_cleanup_coroutines = []


def deploy_run_control_server(conf: dict[str, str | int | float | bool]) -> None:
    """
    Deploys the run control server with the provided configuration.

    Args:
        conf (dict): Configuration dictionary for the run control server.

    Returns:
        None

    Raises:
        DruncSetupException: if there are issues during deployment.
    """
    # Setup logging for the run control server
    log = get_logger(
        logger_name=conf["run_control_server"]["name"],
        rich_handler=True,
        file_handler_path=conf["run_control_server"]["log_path"],
    )
    log.debug("Running [green]deploy_run_control_server[/green]")

    # parent_death_pact()

    # Set environment variables from the configuration
    # This is done to ensure that the run control server has access to the necessary
    # environment settings, including those useful for debugging and logging.
    for key, value in conf["run_control_server"]["environment"].items():
        os.environ[key] = value

    # Setup the Run Control instance
    log.info("Setting up run control instance")
    run_control = RunControl(conf)
    log.debug("Setup up run control instance")

    server: grpc.Server | None = None

    def serve(conf: dict[str, str | int | float | bool]) -> None:
        """
        Starts the gRPC server to serve the Run Control service.

        Args:
            address (str): The address on which to serve the gRPC server.

        Returns:
            None

        Raises:
            DruncSetupException: if the address is not specified.
        """
        # Resolve hostname to network IP if it's localhost
        host = conf["run_control_server"]["host"]
        port = conf["run_control_server"]["port"]
        address = resolve_localhost_and_127_ip_to_network_ip(f"{host}:{port}")
        log.debug("[blue]serve[/] called")

        # Setup the gRPC server for the run control service
        nonlocal server
        server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=conf["run_control_server"]["grpc_config"]["max_workers"]
            ),
            options=conf["run_control_server"]["grpc_config"]["options"],
            # interceptors=[RichErrorServerInterceptor()], # TODO: Implement me later!
        )

        # Add the Run Control service to the gRPC server
        add_RunControlServicer_to_server(run_control, server)
        port = server.add_insecure_port(address)

        # Start the gRPC server and log the address it's serving on
        server.start()
        log.info(
            f"process_manager communicating through address [bold green]{host}:{port}[/bold green]"
        )  # bold as part of the address was already formatting, couldn't figure out why

        # Include the server shutdown coroutine in the cleanup coroutines list for
        # graceful shutdown
        _cleanup_coroutines.append(server_shutdown)
        server.wait_for_termination()

    def server_shutdown() -> None:
        """
        Cleanly shuts down the gRPC server.

        Shuts down the server with 1 seconds of grace period. During the grace period,
        the server won't accept new connections and allow existing RPCs to continue
        within the grace period.
        """

        nonlocal server
        if server:
            log.info("Shutting down the process manager server")
            server.stop(1)
            server = None
        return

    def handle_sigterm(signum: int, frame: types.FrameType) -> None:
        """
        Handle the SIGTERM signal to gracefully shut down the server.

        Args:
            signum: The signal number.
            frame: The current stack frame (not used).
        """

        log.debug("SIGTERM received, shutting down server...")
        server_shutdown()
        return

    # Register the SIGTERM handler to gracefully shut down the server
    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        log.debug("Serving run control")
        serve(conf)
    except Exception as e:
        log.error("Serving the RunControl received an Exception")
        log.exception(e)
    finally:
        if _cleanup_coroutines:
            for coroutine in _cleanup_coroutines:
                coroutine()


@click.command()
@click.option(
    "-c",
    "--configuration",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    default=None,
    help=(
        "Specify the path to the configuration file for the run control server. If not "
        "provided, the default configuration will be used.",
    ),
)
@click.option(
    "-p",
    "--port-override",
    type=int,
    default=None,
    help="Override the endpoint port number from the configuration. If a port is specified in the configuration file, the use of this parameter will override the port specified.",
)
@click.option(
    "-lp",
    "--log-path-override",
    type=str,
    default=None,
    help="Override the path to the log file from the configuration. If a log path is specified in the configuration file, the use of this parameter will override the log path specified.",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
@click.option(
    "-o/-no",
    "--override-logs/--no-override-logs",
    type=bool,
    default=True,
    help="Override logs, if --no-override-logs filenames have the timestamp of the run.",
)
def run_control_server_cli(
    configuration: click.Path | None,
    port_overrride: int | None,
    log_path_override: str | None,
    override_logs: bool,
    log_level: str | None,
) -> None:
    """
    Command-line interface for running the run control server.

    Args:
        configuration (click.Path | None): Path to the configuration file.
        port_overrride (int | None): Port number to override the configuration.
        log_path_override (str | None): Path to the log file to override the configuration.
        override_logs (bool): Flag to determine if logs should be overridden.
        log_level (str | None): Log level for the server.

    Returns:
        None

    Raises:
        DruncSetupException: if there are issues during setup or deployment.
        TODO: Complete me!
    """
    get_root_logger(log_level)
    conf = get_run_control_server_configuration(
        configuration,
        port_override=port_overrride,
        log_path_override=log_path_override,
        override_logs=override_logs,
        log_level=log_level,
    )
    deploy_run_control_server(conf)
