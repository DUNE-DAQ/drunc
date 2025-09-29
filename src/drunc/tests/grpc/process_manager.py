"""
Manager Service Implementation

Provides the gRPC servicer implementation for the Manager service,
which acts as the top-level coordinator in the system hierarchy.
Supports booting remote servers via SSH and managing their lifecycle.
"""

import os
import signal
import threading
import time
from typing import Dict

from grpc import RpcError, StatusCode, insecure_channel

from drunc.tests.grpc.available_grpc_servers import ServerType
from drunc.tests.grpc.grpc_server_config import GrpcServerConfig
from drunc.tests.grpc.grpc_server_manager import GrpcServerManager
from drunc.tests.grpc.remote_cli_command_builder import RemoteCLICommandBuilder
from drunc.tests.grpc.ssh_connection_manager import SSHConnectionManager
from drunc.tests.grpc.test_pb2 import (
    BootRequest,
    BootResponse,
    DummyResponse,
    KillRequest,
    KillResponse,
)
from drunc.tests.grpc.test_pb2_grpc import (
    ChildControllerServiceStub,
    ManagerServiceServicer,
    ManagerServiceStub,
    RootControllerServiceStub,
)


class ManagerServiceImpl(ManagerServiceServicer):
    """
    Implementation of Manager gRPC service.

    The Manager service acts as the top-level coordinator and does not
    connect to any upstream services. It handles basic connectivity
    testing, dynamic server booting via SSH, and graceful shutdown requests.
    """

    def __init__(self):
        """Initialise the Manager service implementation."""
        # Track booted processes and their managers
        self.booted_servers: Dict[str, Dict] = {}
        self.boot_lock = threading.Lock()

    def MakeRequest(self, request, context):
        """
        Handle incoming connectivity test requests.

        Args:
            request: DummyRequest containing message and timestamp
            context: gRPC context object

        Returns:
            DummyResponse with echoed message confirming Manager is responsive
        """
        return DummyResponse(reply=f"Manager server response: {request.message}")

    def Boot(self, request: BootRequest, context) -> BootResponse:
        """
        Boot a new gRPC server process via SSH.

        Creates the necessary command builder, connection manager, and server manager
        to start a remote gRPC server. Tracks all booted servers for lifecycle management.

        Args:
            request: BootRequest containing server configuration and SSH details
            context: gRPC context object

        Returns:
            BootResponse indicating success/failure and providing server details
        """
        with self.boot_lock:
            # Validate process_id is unique
            if request.process_id in self.booted_servers:
                return BootResponse(
                    success=False,
                    process_id=request.process_id,
                    message=f"Process ID '{request.process_id}' already exists",
                    port=0,
                )

            try:
                # Validate server type
                try:
                    server_type = ServerType[request.server_type.upper()]
                except KeyError:
                    return BootResponse(
                        success=False,
                        process_id=request.process_id,
                        message=f"Invalid server type: {request.server_type}",
                        port=0,
                    )

                # Create command builder with SSH configuration
                command_builder = RemoteCLICommandBuilder(
                    env_setup_script=request.env_setup_script,
                    python_executable="python3",
                    working_directory=None,
                    default_user=request.user,
                    hosts=[request.host],
                    disable_host_key_check=True,
                    ssh_options=[],
                    env_vars={"GRPC_TRACE": "http"},
                )

                # Create server configuration
                server_config = GrpcServerConfig(
                    server_id=request.process_id,
                    server_type=server_type,
                    host=request.host,
                    port=request.port,
                    max_workers=request.max_workers,
                    log_file=request.log_file,
                    server_options=[],
                    client_options=[],
                    **dict(request.extra_params),
                )

                # Create SSH connection manager
                ssh_connection_manager = SSHConnectionManager(
                    command_builder=command_builder,
                    boot_command_configs={request.process_id: server_config},
                    log_directory=None,
                )

                # Create SSH server manager
                ssh_server_manager = GrpcServerManager(
                    connection_manager=ssh_connection_manager
                )

                # Start the server based on type
                if server_type == ServerType.MANAGER:
                    server_handle = ssh_server_manager.start_manager_server(
                        server_config
                    )
                elif server_type == ServerType.ROOT_CONTROLLER:
                    server_handle = ssh_server_manager.start_root_controller_server(
                        server_config
                    )
                elif server_type == ServerType.CHILD_CONTROLLER:
                    server_handle = ssh_server_manager.start_child_controller_server(
                        server_config
                    )
                else:
                    return BootResponse(
                        success=False,
                        process_id=request.process_id,
                        message=f"Unsupported server type: {server_type}",
                        port=0,
                    )

                # Wait for server to be ready with timeout
                ready = ssh_server_manager.wait_for_server_ready(
                    request.process_id, timeout=30.0
                )

                if not ready:
                    # Cleanup failed server
                    ssh_server_manager.cleanup()
                    startup_error = ssh_connection_manager.get_process_startup_error(
                        server_handle
                    )
                    error_msg = f"Server failed to become ready: {startup_error or 'Unknown error'}"
                    return BootResponse(
                        success=False,
                        process_id=request.process_id,
                        message=error_msg,
                        port=0,
                    )

                # Store server management objects for lifecycle control
                self.booted_servers[request.process_id] = {
                    "server_id": request.process_id,
                    "server_handle": server_handle,
                    "connection_manager": ssh_connection_manager,
                    "server_manager": ssh_server_manager,
                    "server_type": server_type,
                    "port": request.port,
                    "host": request.host,
                }

                return BootResponse(
                    success=True,
                    process_id=request.process_id,
                    message=f"Successfully booted {server_type.name} on {request.host}:{request.port}",
                    port=request.port,
                )

            except Exception as e:
                return BootResponse(
                    success=False,
                    process_id=request.process_id,
                    message=f"Boot failed: {str(e)}",
                    port=0,
                )

    def _kill_booted_server_via_grpc(
        self,
        server_id: str,
        host: str,
        port: int,
        server_type: ServerType,
        grace_period: int,
    ) -> tuple[bool, str]:
        """
        Send Kill gRPC request to a booted server to shut it down gracefully.

        Args:
            server_id: Identifier of the server to kill
            host: Host where server is running
            port: Port where server is listening
            server_type: Type of server (determines which stub to use)
            grace_period: Grace period for shutdown in seconds

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Create gRPC channel to the booted server
            channel = insecure_channel(f"{host}:{port}")

            # Create appropriate stub based on server type
            if server_type == ServerType.MANAGER:
                stub = ManagerServiceStub(channel)
            elif server_type == ServerType.ROOT_CONTROLLER:
                stub = RootControllerServiceStub(channel)
            elif server_type == ServerType.CHILD_CONTROLLER:
                stub = ChildControllerServiceStub(channel)
            else:
                channel.close()
                return False, f"Unknown server type: {server_type}"

            # Send Kill request
            kill_request = KillRequest(
                reason="Killed by Manager during shutdown",
                grace_period_seconds=grace_period,
            )

            kill_response = stub.Kill(kill_request, timeout=5.0)
            channel.close()

            if kill_response.shutdown_initiated:
                return True, f"Kill request accepted: {kill_response.message}"
            else:
                return False, f"Kill request rejected: {kill_response.message}"

        except RpcError as e:
            # If server is already down, that's acceptable
            if e.code() in [
                StatusCode.UNAVAILABLE,
                StatusCode.DEADLINE_EXCEEDED,
            ]:
                return (
                    True,
                    f"Server already unavailable (may have terminated): {e.code()}",
                )
            return False, f"gRPC error sending Kill request: {e.code()} - {e.details()}"
        except Exception as e:
            return False, f"Error sending Kill request: {str(e)}"

    def Kill(self, request: KillRequest, context) -> KillResponse:
        """
        Handle graceful shutdown requests for the Manager service.

        First sends Kill gRPC requests to all booted child servers, waits for their
        termination, then shuts down the Manager itself. Returns error if any children
        fail to terminate properly.

        Args:
            request: KillRequest containing shutdown parameters
            context: gRPC context object

        Returns:
            KillResponse indicating shutdown status
        """
        grace_period = (
            max(request.grace_period_seconds, 1)
            if request.grace_period_seconds > 0
            else 2
        )

        reason = request.reason or "No reason provided"

        # Kill all booted servers first
        kill_failures = []
        with self.boot_lock:
            if self.booted_servers:
                print(
                    f"Manager killing {len(self.booted_servers)} booted servers before shutdown..."
                )

                for process_id, server_info in list(self.booted_servers.items()):
                    try:
                        server_id = server_info["server_id"]
                        server_manager = server_info["server_manager"]
                        connection_manager = server_info["connection_manager"]
                        server_handle = server_info["server_handle"]
                        server_type = server_info["server_type"]
                        host = server_info["host"]
                        port = server_info["port"]

                        print(f"Stopping booted server: {server_id}")

                        # First, send Kill gRPC request to the booted server
                        kill_success, kill_msg = self._kill_booted_server_via_grpc(
                            server_id, host, port, server_type, grace_period
                        )
                        print(f"Kill gRPC request to {server_id}: {kill_msg}")

                        # Wait for the server process to terminate
                        start_time = time.time()
                        while (
                            time.time() - start_time
                        ) < grace_period and connection_manager.is_process_alive(
                            server_handle
                        ):
                            time.sleep(0.1)

                        # If still alive, stop via server manager (SSH termination)
                        if connection_manager.is_process_alive(server_handle):
                            print(
                                f"Server {server_id} still alive, terminating SSH connection"
                            )
                            server_manager.stop_server(server_id, timeout=grace_period)

                            # Wait again for process termination after SSH kill
                            start_time = time.time()
                            while (
                                time.time() - start_time
                            ) < grace_period and connection_manager.is_process_alive(
                                server_handle
                            ):
                                time.sleep(0.1)

                        # Check final status
                        if connection_manager.is_process_alive(server_handle):
                            kill_failures.append(
                                f"{server_id} (process still alive after timeout)"
                            )
                        else:
                            print(f"Successfully stopped booted server: {server_id}")

                        # Cleanup server manager
                        server_manager.cleanup()

                    except Exception as e:
                        error_msg = f"{process_id} ({str(e)})"
                        kill_failures.append(error_msg)
                        print(f"Error stopping booted server {process_id}: {e}")

                # Clear the booted servers dict
                self.booted_servers.clear()

        # If any children failed to terminate, return error
        if kill_failures:
            failure_details = "; ".join(kill_failures)
            response_message = (
                f"Manager Kill incomplete - failed to terminate {len(kill_failures)} "
                f"booted server(s): {failure_details}"
            )
            return KillResponse(shutdown_initiated=False, message=response_message)

        # All children terminated successfully, now shutdown Manager
        booted_count = len(self.booted_servers) if self.booted_servers else 0
        response_details = [
            "Manager Kill method executed successfully",
            f"Terminated {booted_count} booted server(s)"
            if booted_count > 0
            else "No booted servers to terminate",
            f"Reason: {reason}",
            f"Grace period: {grace_period}s",
            f"PID: {os.getpid()}",
            "Shutdown thread starting...",
        ]

        def delayed_shutdown():
            """Send SIGTERM to this process after a brief delay."""
            time.sleep(0.5)  # Allow response to be sent
            os.kill(os.getpid(), signal.SIGTERM)

        # Start shutdown in separate thread to avoid blocking the response
        shutdown_thread = threading.Thread(target=delayed_shutdown)
        shutdown_thread.daemon = True
        shutdown_thread.start()

        return KillResponse(
            shutdown_initiated=True, message=" | ".join(response_details)
        )
