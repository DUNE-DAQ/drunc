import signal
import threading
import time
from concurrent import futures
from typing import Callable, TypeVar, cast

from grpc import Server, insecure_channel
from grpc import server as grpc_server

from drunc.grpc_testing_tools.child_controller import (
    ChildControllerServiceImpl,
)
from drunc.grpc_testing_tools.grpc_log_util import (
    stderr_observer,
    stdout_observer,
)
from drunc.grpc_testing_tools.process_manager import ManagerServiceImpl
from drunc.grpc_testing_tools.root_controller import RootControllerServiceImpl
from drunc.grpc_testing_tools.test_services_pb2_grpc import (
    add_ChildControllerServiceServicer_to_server,
    add_ManagerServiceServicer_to_server,
    add_RootControllerServiceServicer_to_server,
)
from drunc.process_manager.configuration import ProcessManagerTypes

SERVER_GRACE_PERIOD = 2
T = TypeVar("T")


def run_grpc_server(
    server_name: str,
    servicer_instance: T,
    add_servicer_func: Callable[[T, Server], None],
    max_workers: int,
    server_port: int,
    log_file: str,
    server_options: list[tuple[str, object]] | None = None,
    upstream_connection: dict[str, object] | None = None,
    ready_event: threading.Event | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """Generic gRPC server runner that handles the common server lifecycle."""

    stderr_observer(log_file)
    stdout_observer(log_file)

    shutdown_requested = False

    def signal_handler(signum: int, frame: object) -> None:
        """Handle SIGTERM and SIGINT for graceful server shutdown."""
        nonlocal shutdown_requested
        shutdown_requested = True
        if stop_event:
            stop_event.set()
        print(f"{server_name} server received signal {signum}, shutting down...")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    server = grpc_server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=server_options or [],
    )

    add_servicer_func(servicer_instance, server)
    port = server.add_insecure_port(f"[::]:{server_port}")

    upstream_channel = None
    try:
        server.start()

        if upstream_connection:
            upstream_host = str(upstream_connection["host"])
            upstream_port = cast(int, upstream_connection["port"])
            client_options = cast(
                list[tuple[str, object]], upstream_connection.get("options", [])
            )

            upstream_channel = insecure_channel(
                f"{upstream_host}:{upstream_port}", options=client_options
            )
            print(
                f"{server_name} server started on port {port}, connected to upstream on {upstream_port}"
            )
        else:
            print(f"{server_name} server started on port {port}")

        if ready_event:
            ready_event.set()

        if stop_event:
            while not shutdown_requested and not stop_event.is_set():
                time.sleep(0.1)
        else:
            while not shutdown_requested:
                time.sleep(0.1)

    except Exception as e:
        print(f"{server_name} server error: {e}")
    finally:
        print(f"Shutting down {server_name} server...")
        if upstream_channel:
            upstream_channel.close()
        server.stop(grace=SERVER_GRACE_PERIOD)


def run_process_manager_server(
    manager_max_workers: int,
    server_port: int,
    log_file: str,
    server_options: list[tuple[str, object]] | None = None,
    ready_event: threading.Event | None = None,
    stop_event: threading.Event | None = None,
    lifetime_manager_type: ProcessManagerTypes = ProcessManagerTypes.SSH_SHELL,
) -> None:
    """Run Manager server process with output logging."""

    run_grpc_server(
        server_name="Manager",
        servicer_instance=ManagerServiceImpl(
            lifetime_manager_type=lifetime_manager_type
        ),
        add_servicer_func=add_ManagerServiceServicer_to_server,
        max_workers=manager_max_workers,
        server_port=server_port,
        log_file=log_file,
        server_options=server_options,
        upstream_connection=None,
        ready_event=ready_event,
        stop_event=stop_event,
    )


def run_root_controller_server(
    controller_max_workers: int,
    server_port: int,
    manager_port: int,
    log_file: str,
    server_options: list[tuple[str, object]] | None = None,
    client_options: list[tuple[str, object]] | None = None,
    ready_event: threading.Event | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """Run RootController server with Manager client connection."""

    run_grpc_server(
        server_name="RootController",
        servicer_instance=RootControllerServiceImpl(),
        add_servicer_func=add_RootControllerServiceServicer_to_server,
        max_workers=controller_max_workers,
        server_port=server_port,
        log_file=log_file,
        server_options=server_options,
        upstream_connection={
            "host": "localhost",
            "port": manager_port,
            "options": client_options or [],
        },
        ready_event=ready_event,
        stop_event=stop_event,
    )


def run_child_controller_server(
    controller_max_workers: int,
    server_port: int,
    root_port: int,
    child_name: str,
    log_file: str,
    server_options: list[tuple[str, object]] | None = None,
    client_options: list[tuple[str, object]] | None = None,
    ready_event: threading.Event | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """Run ChildController server with RootController client connection."""

    run_grpc_server(
        server_name=child_name,
        servicer_instance=ChildControllerServiceImpl(child_name),
        add_servicer_func=add_ChildControllerServiceServicer_to_server,
        max_workers=controller_max_workers,
        server_port=server_port,
        log_file=log_file,
        server_options=server_options,
        upstream_connection={
            "host": "localhost",
            "port": root_port,
            "options": client_options or [],
        },
        ready_event=ready_event,
        stop_event=stop_event,
    )
