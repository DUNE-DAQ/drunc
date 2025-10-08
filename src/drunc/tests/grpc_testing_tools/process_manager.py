"""
Manager Service Implementation

Provides the gRPC servicer implementation for the Manager service,
which acts as the top-level coordinator in the system hierarchy.
Supports booting remote servers via SSH and managing their lifecycle.
"""

import logging
import os
import signal
import threading
import time
from typing import Dict

from grpc import RpcError, StatusCode, insecure_channel

from drunc.ssh.ssh_connection_manager import SSHConnectionManager
from drunc.tests.grpc_testing_tools.test_services_pb2 import (
    BootRequest,
    DummyResponse,
    KillRequest,
    KillResponse,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ResponseFlag,
)
from drunc.tests.grpc_testing_tools.test_services_pb2_grpc import (
    ManagerServiceServicer,
    ManagerServiceStub,
    RootControllerServiceStub,
)


class ManagerServiceImpl(ManagerServiceServicer):
    """
    Implementation of Manager gRPC service compatible with druncschema components.

    The Manager service acts as the top-level coordinator and uses
    SSHConnectionManager for SSH-based process execution.
    """

    def __init__(self):
        """Initialise the Manager service implementation."""
        self.ssh_manager = SSHConnectionManager(
            disable_host_key_check=True,
            disable_localhost_host_key_check=True,
            logger=logging.getLogger(__name__),
        )

        # Track booted processes
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

    def boot(self, request: BootRequest, context) -> ProcessInstanceList:
        """
        Boot a new gRPC server process via SSH using druncschema-style BootRequest.

        Args:
            request: BootRequest containing ProcessDescription and restrictions
            context: gRPC context object

        Returns:
            ProcessInstanceList indicating success/failure and providing server details
        """
        with self.boot_lock:
            process_uuid = request.process_description.metadata.uuid.uuid
            process_name = request.process_description.metadata.name

            # Validate process UUID is unique
            if process_uuid in self.booted_servers:
                return ProcessInstanceList(
                    name="boot_error",
                    token=request.token,
                    values=[],
                    flag=ResponseFlag(
                        success=False,
                        message=f"Process UUID '{process_uuid}' already exists",
                    ),
                )

            try:
                # Extract connection details from process metadata
                hostname = request.process_description.metadata.hostname
                user = request.process_description.metadata.user

                # Build command based on process executable and arguments
                command = self._build_server_command_from_description(
                    request.process_description
                )

                # Set up environment variables from process description
                env_vars = dict(request.process_description.env)
                env_vars.update({"GRPC_TRACE": "http"})

                # Extract log file path
                log_file = request.process_description.process_logs_path

                # Execute via SSH using SSHConnectionManager
                process = self.ssh_manager.execute_ssh_command(
                    uuid=process_uuid,
                    boot_request=request,
                    hostname=hostname,
                    user=user,
                    command=command,
                    log_file=log_file,
                    env_vars=env_vars,
                )

                # Store server info
                self.booted_servers[process_uuid] = {
                    "process": process,
                    "request": request,
                    "command": command,
                }

                # Create successful process instance
                process_instance = ProcessInstance(
                    process_description=request.process_description,
                    process_restriction=request.process_restriction,
                    status_code=ProcessInstance.StatusCode.RUNNING,
                    return_code=0,
                    uuid=request.process_description.metadata.uuid,
                )

                return ProcessInstanceList(
                    name=process_name,
                    token=request.token,
                    values=[process_instance],
                    flag=ResponseFlag(
                        success=True,
                        message=f"Successfully booted {process_name} on {hostname}",
                    ),
                )

            except Exception as e:
                return ProcessInstanceList(
                    name=process_name,
                    token=request.token,
                    values=[],
                    flag=ResponseFlag(
                        success=False,
                        message=f"Boot failed: {str(e)}",
                    ),
                )

    def _build_server_command_from_description(
        self, process_desc: ProcessDescription
    ) -> str:
        """
        Build the command to execute from ProcessDescription.

        Args:
            process_desc: ProcessDescription containing executable and arguments

        Returns:
            Command string to execute remotely
        """
        if not process_desc.executable_and_arguments:
            raise ValueError("No executable specified in process description")

        # Use the first executable and arguments
        exec_and_args = process_desc.executable_and_arguments[0]

        # Build command from executable and arguments
        cmd_parts = [exec_and_args.exec] + list(exec_and_args.args)
        command = " ".join(cmd_parts)

        return command

    def _kill_booted_server_via_grpc(
        self,
        server_uuid: str,
        host: str,
        port: int,
        server_type: str,
        grace_period: int,
    ) -> tuple[bool, str]:
        """
        Send Kill gRPC request to a booted server to shut it down gracefully.

        Args:
            server_uuid: UUID of the server to kill
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
            if server_type == "MANAGER":
                stub = ManagerServiceStub(channel)
            elif server_type == "ROOT_CONTROLLER" or server_type == "RootController":
                stub = RootControllerServiceStub(channel)
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
        termination, then shuts down the Manager itself.

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

                for process_uuid, server_info in list(self.booted_servers.items()):
                    try:
                        boot_request = server_info["request"]
                        process_desc = boot_request.process_description

                        print(f"Stopping booted server: {process_uuid}")

                        # Extract server details from process description
                        hostname = process_desc.metadata.hostname
                        # Extract port from process arguments
                        port = self._extract_port_from_args(process_desc)
                        server_type = (
                            process_desc.metadata.name
                        )  # Assuming name indicates type

                        # First, send Kill gRPC request to the booted server
                        kill_success, kill_msg = self._kill_booted_server_via_grpc(
                            process_uuid,
                            hostname,
                            port,
                            server_type,
                            grace_period,
                        )
                        print(f"Kill gRPC request to {process_uuid}: {kill_msg}")

                        # Wait for the server process to terminate
                        start_time = time.time()
                        while (
                            time.time() - start_time
                        ) < grace_period and self.ssh_manager.is_process_alive(
                            process_uuid
                        ):
                            time.sleep(0.1)

                        # If still alive, stop via SSH termination
                        if self.ssh_manager.is_process_alive(process_uuid):
                            print(
                                f"Server {process_uuid} still alive, terminating SSH connection"
                            )
                            self.ssh_manager.terminate_process(
                                process_uuid, timeout=grace_period
                            )

                            # Wait again for process termination after SSH kill
                            start_time = time.time()
                            while (
                                time.time() - start_time
                            ) < grace_period and self.ssh_manager.is_process_alive(
                                process_uuid
                            ):
                                time.sleep(0.1)

                        # Check final status
                        if self.ssh_manager.is_process_alive(process_uuid):
                            kill_failures.append(
                                f"{process_uuid} (process still alive after timeout)"
                            )
                        else:
                            print(f"Successfully stopped booted server: {process_uuid}")

                        # Cleanup process
                        self.ssh_manager.cleanup_process(process_uuid)

                    except Exception as e:
                        error_msg = f"{process_uuid} ({str(e)})"
                        kill_failures.append(error_msg)
                        print(f"Error stopping booted server {process_uuid}: {e}")

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

    def _extract_port_from_args(self, process_desc: ProcessDescription) -> int:
        """
        Extract port number from process arguments.

        Handles both structured argument lists and single command strings.

        Args:
            process_desc: ProcessDescription containing executable arguments

        Returns:
            Port number as integer

        Raises:
            ValueError: If port cannot be found in arguments
        """
        if not process_desc.executable_and_arguments:
            raise ValueError("No executable arguments to extract port from")

        args = process_desc.executable_and_arguments[0].args

        for arg in args:
            if "--port" in arg:
                # Handle space-separated: "--port 50091" within the string
                import re

                match = re.search(r"--port\s+(\d+)", arg)
                if match:
                    return int(match.group(1))

        raise ValueError("Port not found in process arguments")
