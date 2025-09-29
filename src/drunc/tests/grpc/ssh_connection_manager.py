"""
SSH connection manager using subprocess for managing processes created from
pre-built SSH commands.

This implementation uses subprocess.Popen instead of sh.Command to avoid
fork safety issues when spawning processes from within multi-threaded gRPC servers.
"""

import os
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from drunc.tests.grpc.grpc_server_manager import GrpcServerConfig
from drunc.tests.grpc.process_connection_manager import (
    ProcessConnectionManager,
    RunningGrpcServer,
)
from drunc.tests.grpc.remote_cli_command_builder import (
    BootServerCommand,
    RemoteCLICommandBuilder,
)


class SSHConnectionManager(ProcessConnectionManager):
    """
    SSH connection manager using subprocess.Popen for executing pre-built server boot commands.

    Uses subprocess instead of sh.Command to provide better compatibility with
    multi-threaded gRPC servers by avoiding fork safety issues. Handles SSH process
    execution, termination, and monitoring.
    """

    def __init__(
        self,
        command_builder: RemoteCLICommandBuilder,
        boot_command_configs: Dict[str, GrpcServerConfig],
        log_directory: str = None,
    ):
        """
        Initialise SSH connection manager with pre-built boot commands.

        Args:
            command_builder: Builder for constructing SSH commands
            boot_command_configs: Dict mapping server_id to GrpcServerConfig objects
            log_directory: Directory for storing remote process logs
        """
        super().__init__(env_vars={})  # No env_vars needed - handled in boot commands
        self.boot_command_configs = boot_command_configs
        self.boot_commands: Dict[str, BootServerCommand] = {}
        for server_id, config in boot_command_configs.items():
            self.boot_commands[server_id] = command_builder.build_server_command(
                config.server_type,
                config.server_id,
                config.port,
                config.max_workers,
                config.log_file,
                **config.extra_params,
            )

        # Set up log directory
        if log_directory:
            self.log_directory = Path(log_directory)
            self.log_directory.mkdir(parents=True, exist_ok=True)
        else:
            self.temp_dir = tempfile.mkdtemp(prefix="ssh_grpc_logs_")
            self.log_directory = Path(self.temp_dir)

        # Process monitoring
        self.watchers: List[threading.Thread] = []

        print(
            f"SSH connection manager initialised with {len(self.boot_commands)} boot commands:"
        )
        for server_id, boot_cmd in self.boot_commands.items():
            print(f"  {server_id}: {boot_cmd.description}")

    def create_process(
        self, process_id: str, target_func: Any, *args, **kwargs
    ) -> RunningGrpcServer:
        """
        Create a process handle for SSH execution.

        For SSH execution, the target function and arguments are not directly used
        since the complete command is pre-built. This method creates a handle that
        stores the function signature for reference but execution uses the pre-built
        SSH command associated with the server_id.

        Args:
            process_id: Unique identifier for the process
            target_func: Function to execute remotely (stored for reference)
            *args: Arguments for the target function (stored for reference)
            **kwargs: Keyword arguments for the target function (stored for reference)

        Returns:
            RunningGrpcServer configured for SSH execution (ready_event and stop_event remain None)
        """
        handle = RunningGrpcServer(process_id, target_func, args, kwargs)
        self.process_handles[process_id] = handle
        return handle

    def start_process(self, handle: RunningGrpcServer) -> None:
        """
        Start a process using the pre-built boot command for the server ID.

        Uses subprocess.Popen to spawn the SSH process, which provides better
        compatibility with multi-threaded environments than fork-based approaches.

        Args:
            handle: RunningGrpcServer to execute remotely (must have server_id set)

        Raises:
            ValueError: If server_id is not set or not found in boot_commands
            RuntimeError: If SSH execution fails
        """
        if handle.started:
            raise RuntimeError(f"Process {handle.process_id} is already started")

        if not handle.server_id:
            raise ValueError(f"Process {handle.process_id} has no server_id set")

        if handle.server_id not in self.boot_commands:
            raise ValueError(f"No boot command found for server ID: {handle.server_id}")

        boot_command = self.boot_commands[handle.server_id]

        # Create local log file for SSH output
        log_file = self.log_directory / f"{handle.process_id}.log"

        try:
            # Execute SSH command using subprocess.Popen
            ssh_process = subprocess.Popen(
                boot_command.complete_ssh_command,
                start_new_session=True,
                close_fds=True,
            )

            # Check if process failed immediately
            poll_result = ssh_process.poll()
            if poll_result is not None:
                error_output = self._read_recent_log_output(log_file, max_lines=100)
                raise RuntimeError(
                    f"=== SSH process for {handle.process_id} failed to start.\n\n"
                    f"=== Command:\n {' '.join(boot_command.complete_ssh_command)}.\n\n"
                    f"=== Exit code: {poll_result}\n\n"
                    f"=== Log output:\n {error_output}"
                )

            # Store process and log handle
            handle.set_process(ssh_process)
            handle.mark_started()

            # Start monitoring thread
            self._start_process_watcher(handle, boot_command)

            print(
                f"SSH process {handle.process_id} started successfully (PID: {ssh_process.pid})"
            )

        except Exception as e:
            error_msg = f"Failed to start SSH process {handle.process_id}: {e}"
            raise RuntimeError(error_msg)

    def get_expected_port_for_server_id(self, server_id: str) -> Optional[int]:
        """
        Get the expected port for a server ID from its boot command.

        Args:
            server_id: Server identifier

        Returns:
            Expected port number if server ID exists, None otherwise
        """
        boot_command = self.boot_commands.get(server_id)
        return boot_command.expected_port if boot_command else None

    def get_process_startup_error(self, handle: RunningGrpcServer) -> Optional[str]:
        """
        Get any startup error reported by the process watcher.

        Args:
            handle: RunningGrpcServer to check for startup errors

        Returns:
            Error message if startup failed, None if no error
        """
        return getattr(handle, "startup_error", None)

    def _read_recent_log_output(self, log_file: Path, max_lines: int = 20) -> str:
        """
        Read recent output from a log file for error reporting.

        Args:
            log_file: Path to log file to read
            max_lines: Maximum number of lines to read

        Returns:
            Recent log content or error message if file cannot be read
        """
        try:
            if log_file.exists():
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                    recent_lines = (
                        lines[-max_lines:] if len(lines) > max_lines else lines
                    )
                    return "".join(recent_lines).strip()
            return "Log file not yet created"
        except Exception as e:
            return f"Could not read log file: {e}"

    def _start_process_watcher(
        self, handle: RunningGrpcServer, boot_command: BootServerCommand
    ) -> None:
        """
        Start a monitoring thread for a subprocess.

        Args:
            handle: RunningGrpcServer to monitor
            boot_command: BootServerCommand used to start the process
        """

        def watch_process():
            """Monitor subprocess and handle termination."""
            try:
                # Wait for process to complete
                exit_code = handle.process.wait()

                # SSH exit code 255 is common when the remote process keeps running
                if exit_code == 0:
                    print(f"SSH process {handle.process_id} completed successfully")
                elif exit_code == 255:
                    print(
                        f"SSH process {handle.process_id} exited with code 255 (remote process may still be running)"
                    )
                else:
                    # Read log output for error details
                    log_file = self.log_directory / f"{handle.process_id}.log"
                    error_output = self._read_recent_log_output(log_file, 100)

                    error_msg = (
                        f"SSH process {handle.process_id} failed. "
                        f"Exit code: {exit_code}. "
                        f"Command: {' '.join(boot_command.complete_ssh_command)}. "
                        f"Recent output: {error_output}"
                    )
                    print(f"ERROR: {error_msg}")
                    handle.startup_error = error_msg

            except Exception as e:
                error_msg = f"SSH process {handle.process_id} watcher encountered exception: {e}"
                print(f"ERROR: {error_msg}")
                handle.startup_error = error_msg

        watcher = threading.Thread(
            target=watch_process, name=f"SSHWatcher-{handle.process_id}", daemon=True
        )
        watcher.start()
        self.watchers.append(watcher)

    def stop_process(self, handle: RunningGrpcServer, timeout: float = 10.0) -> None:
        """
        Stop a subprocess by terminating the SSH connection.

        Sends SIGTERM to the process group to ensure child processes are also terminated.
        Forces SIGKILL if graceful shutdown does not complete within timeout.

        Args:
            handle: RunningGrpcServer for the SSH process to stop
            timeout: Maximum time to wait for graceful shutdown
        """
        if not handle.started or (handle.process is None):
            return

        ssh_process = handle.process

        # Check if already terminated
        if ssh_process.poll() is not None:
            return

        try:
            print(
                f"Stopping SSH process {handle.process_id} (PID: {ssh_process.pid})..."
            )

            # Send SIGTERM to process group (negative PID targets entire process group)
            try:
                os.killpg(os.getpgid(ssh_process.pid), signal.SIGTERM)
            except ProcessLookupError:
                # Process already terminated
                return
            except Exception as e:
                print(f"Warning: Could not send SIGTERM to process group: {e}")
                # Fall back to terminating just the main process
                ssh_process.terminate()

            # Wait for graceful termination
            start_time = time.time()
            while ssh_process.poll() is None and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            # Force kill if still alive
            if ssh_process.poll() is None:
                print(f"Force killing SSH process {handle.process_id} after timeout")
                try:
                    os.killpg(os.getpgid(ssh_process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception as e:
                    print(f"Warning: Could not send SIGKILL to process group: {e}")
                    ssh_process.kill()

                # Wait a bit more for forced termination
                start_time = time.time()
                while ssh_process.poll() is None and (time.time() - start_time) < 2.0:
                    time.sleep(0.1)

            print(f"SSH process {handle.process_id} stopped")

        except Exception as e:
            print(f"Warning: Error stopping SSH process {handle.process_id}: {e}")

    def is_process_alive(self, handle: RunningGrpcServer) -> bool:
        """
        Check if a subprocess is still running.

        Args:
            handle: RunningGrpcServer to check

        Returns:
            True if SSH connection is active, False otherwise
        """
        if not handle.started or (handle.process is None):
            return False

        try:
            # poll() returns None if process is still running, exit code otherwise
            return handle.process.poll() is None
        except Exception:
            return False

    def wait_for_termination(
        self, handle: RunningGrpcServer, timeout: Optional[float] = None
    ) -> None:
        """
        Wait for subprocess to terminate.

        Args:
            handle: RunningGrpcServer to wait for
            timeout: Maximum time to wait
        """
        if handle.started and handle.process:
            try:
                handle.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                pass
            except Exception:
                pass

    def cleanup(self) -> None:
        """Stop all SSH processes and clean up resources."""
        print("Cleaning up SSH connection manager...")

        # Stop all processes
        for handle in list(self.process_handles.values()):
            try:
                self.stop_process(handle)
            except Exception as e:
                print(f"Warning: Error stopping SSH process {handle.process_id}: {e}")

        # Wait for watcher threads to complete
        for watcher in self.watchers:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        # Clean up temporary directory if we created it
        if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
            try:
                import shutil

                shutil.rmtree(self.temp_dir)
            except Exception as e:
                print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")

        self.process_handles.clear()
        self.watchers.clear()

        print("SSH connection manager cleanup completed")
