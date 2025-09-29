import signal
import time
from concurrent import futures
from typing import Any, Dict, List, Optional, Tuple

from drunc.tests.grpc.grpc_log_util import stderr_observer, stdout_observer

SERVER_GRACE_PERIOD = 2


def run_grpc_server(
    server_name: str,
    servicer_instance: Any,
    add_servicer_func: Any,
    max_workers: int,
    server_port: int,
    log_file: str,
    server_options: Optional[List[Tuple[str, Any]]] = None,
    upstream_connection: Optional[Dict[str, Any]] = None,
    ready_event=None,
    stop_event=None,
) -> None:
    """Generic gRPC server runner that handles the common server lifecycle."""
    import os
    import sys

    print(f"[{server_name}] grpc already imported: {'grpc' in sys.modules}")
    print(f"[{server_name}] GRPC_TRACE={os.environ.get('GRPC_TRACE', 'NOT SET')}")
    stderr_observer(log_file)
    stdout_observer(log_file)

    from grpc import insecure_channel
    from grpc import server as grpc_server

    shutdown_requested = False

    def signal_handler(signum: int, frame) -> None:
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
            upstream_host = upstream_connection["host"]
            upstream_port = upstream_connection["port"]
            client_options = upstream_connection.get("options", [])

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
    server_options: Optional[List[Tuple[str, Any]]] = None,
    ready_event=None,
    stop_event=None,
) -> None:
    """Run Manager server process with output logging."""
    from drunc.tests.grpc.process_manager import ManagerServiceImpl
    from drunc.tests.grpc.test_pb2_grpc import add_ManagerServiceServicer_to_server

    run_grpc_server(
        server_name="Manager",
        servicer_instance=ManagerServiceImpl(),
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
    server_options: Optional[List[Tuple[str, Any]]] = None,
    client_options: Optional[List[Tuple[str, Any]]] = None,
    ready_event=None,
    stop_event=None,
) -> None:
    """Run RootController server with Manager client connection."""
    from drunc.tests.grpc.root_controller import RootControllerServiceImpl
    from drunc.tests.grpc.test_pb2_grpc import (
        add_RootControllerServiceServicer_to_server,
    )

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
    server_options: Optional[List[Tuple[str, Any]]] = None,
    client_options: Optional[List[Tuple[str, Any]]] = None,
    ready_event=None,
    stop_event=None,
) -> None:
    """Run ChildController server with RootController client connection."""
    from drunc.tests.grpc.child_controller import ChildControllerServiceImpl
    from drunc.tests.grpc.test_pb2_grpc import (
        add_ChildControllerServiceServicer_to_server,
    )

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
