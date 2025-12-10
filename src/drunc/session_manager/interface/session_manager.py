"""Session Manager CLI interface for Drunc."""

from concurrent import futures
from logging import getLogger

import click
import grpc
from druncschema.session_manager_pb2_grpc import add_SessionManagerServicer_to_server

from drunc.grpc_settings import (
    MANAGER_SERVER_GRPC_CONFIG,
    MANAGER_SERVER_GRPC_MAX_WORKERS,
)
from drunc.session_manager.configuration import SessionManagerConfHandler
from drunc.session_manager.session_manager import SessionManager
from drunc.utils.grpc_utils import RichErrorServerInterceptor
from drunc.utils.utils import get_logger, get_root_logger


def serve(session_manager: SessionManager, address: str) -> None:
    """Start the gRPC server for the session manager.

    Args:
        session_manager: The session manager instance to serve.
        address: The address to bind the server to.
    """
    logger = getLogger("drunc.session_manager")
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MANAGER_SERVER_GRPC_MAX_WORKERS),
        options=MANAGER_SERVER_GRPC_CONFIG,
        interceptors=[RichErrorServerInterceptor()],
    )
    add_SessionManagerServicer_to_server(session_manager, server)
    port = server.add_insecure_port(address)
    server.start()
    logger.info(f"{session_manager.name} listening on port {port}")
    server.wait_for_termination(timeout=None)


@click.command()
# @click.option(
#     '--log-level',
#     type=click.Choice(list(logging_log_levels.keys()), case_sensitive=False),
#     default="INFO",
#     help="Verbosity of the session manager logger.",
# )
# @click.option(
#     '--log-path',
#     type=str,
#     default=None,
#     help="Path of the session manager log file.",
# )
# def session_manager_cli(log_level: str, log_path: str):
def session_manager_cli():
    """CLI interface for the Drunc session manager.

    This command starts the session manager service, which allows clients to manage
    and interact with drunc sessions.
    """
    app_name = "session_manager"
    log_level = "DEBUG"

    get_root_logger(log_level)
    logger = get_logger(app_name, rich_handler=True)

    # Load the configuration for the session manager.
    config = SessionManagerConfHandler()
    logger.info(f"Using '{config}' as the SessionManager configuration.")

    # Load the session manager.
    session_manager = SessionManager("session_manager", config)
    logger.info("Creating session manager.")

    try:
        serve(session_manager, "0.0.0.0:50000")
    except Exception as e:
        logger.error("Error whilst serving the session manager.")
        logger.exception(e)
