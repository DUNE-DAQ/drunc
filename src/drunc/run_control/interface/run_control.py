from __future__ import annotations

import concurrent
import signal
import types
from collections.abc import Callable
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event

import click
import grpc
from druncschema.run_control_pb2_grpc import add_RunControlServicer_to_server

from drunc.exceptions import DruncSetupException
from drunc.grpc_settings import (
    MANAGER_SERVER_GRPC_CONFIG,
    MANAGER_SERVER_GRPC_MAX_WORKERS,
)
from drunc.run_control.run_control import RunControl
from drunc.utils.grpc_utils import RichErrorServerInterceptor
from drunc.utils.utils import (
    get_logger,
    get_root_logger,
    resolve_localhost_and_127_ip_to_network_ip,
)

_cleanup_coroutines: list[Callable[[], None]] = []


def run_rc(
    rc_address: str,
    ready_event: Event | None = None,
    generated_port: Synchronized[int] | None = None,
):
    appName = "run_control"
    log = get_logger(logger_name=appName, rich_handler=True)

    log.info("Running [green]run_rc[/green]")

    rc = RunControl()  # rcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrcrc
    log.info("Set up Run Controller")

    server: grpc.Server | None = None

    def serve(address: str) -> None:
        address = resolve_localhost_and_127_ip_to_network_ip(address)
        log.info("serving")

        if not address:
            raise DruncSetupException("your address sucks")
        nonlocal server
        server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=MANAGER_SERVER_GRPC_MAX_WORKERS
            ),
            options=MANAGER_SERVER_GRPC_CONFIG,
            interceptors=[RichErrorServerInterceptor()],
        )

        add_RunControlServicer_to_server(rc, server)
        port = server.add_insecure_port(address)
        if generated_port is not None:
            generated_port.value = port

        server.start()
        host = address.split(":")[0]
        log.info(
            f"run_control communicating through address [bold green]{host}:{port}[/bold green]"
        )

        _cleanup_coroutines.append(server_shutdown)
        if ready_event is not None:
            ready_event.set()

        server.wait_for_termination()

    def server_shutdown() -> None:
        nonlocal server
        if server:
            log.info("Shutting down the run control server")
            server.stop(1)
            server = None
        return

    def handle_sigterm(signum: int, frame: types.FrameType | None) -> None:
        log.info("SIGTERM received, shutting down server...")
        server_shutdown()
        return

    # Register sigterm handler
    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        log.info("Serving run_control")
        serve(rc_address)
    except Exception as e:
        log.error("There was an exception")
        log.exception(e)
    finally:
        if _cleanup_coroutines:
            for coroutine in _cleanup_coroutines:
                coroutine()


@click.command()
@click.argument("rc-port", type=int)
def rc_cli(rc_port: int):
    get_root_logger("info")
    run_rc(rc_address=f"0.0.0.0:{rc_port}")
