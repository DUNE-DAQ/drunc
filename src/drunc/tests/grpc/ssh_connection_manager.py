"""
SSH connection manager for executing pre-built server commands.

This module provides simple SSH-based process execution using complete
pre-built SSH commands. All command construction is handled externally.
"""

import os
import signal
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import sh

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
    SSH connection manager for executing pre-built server boot commands.

    Executes complete SSH commands that have been pre-built externally.
    Handles only process execution and monitoring - no command construction.
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
            boot_commands: Dict mapping server_id to complete BootServerCommand objects
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

        Args:
            process_id: Unique identifier for the process
            target_func: Function to execute remotely (placeholder for SSH)
            *args: Arguments for the target function
            **kwargs: Keyword arguments for the target function

        Returns:
            RunningGrpcServer configured for SSH execution
        """
        handle = RunningGrpcServer(process_id, target_func, args, kwargs)
        self.process_handles[process_id] = handle

        return handle

    def start_process(self, handle: RunningGrpcServer) -> None:
        """
        Start a process using the pre-built boot command for the server ID.

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
            print(f"Starting {boot_command.description}")

            # Execute the complete pre-built command
            ssh_process = sh.Command(boot_command.complete_ssh_command[0])(
                *boot_command.complete_ssh_command[1:],
                _out=str(log_file),
                _err=str(log_file),
                _bg=True,
                _bg_exc=False,
                _new_session=True,
            )

            # Check if SSH process failed immediately
            if not ssh_process.is_alive():
                error_output = self._read_recent_log_output(log_file, max_lines=100)
                raise RuntimeError(
                    f"=== SSH process for {handle.process_id} failed to start.\n\n"
                    f"=== Command:\n {' '.join(boot_command.complete_ssh_command)}.\n\n"
                    f"=== Log output:\n {error_output}"
                )

            handle.set_process(ssh_process)
            handle.mark_started()

            # Start monitoring thread
            self._start_process_watcher(handle, boot_command)

            print(f"SSH process {handle.process_id} started successfully")

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
            Recent log content or error message if file can't be read
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
        Start a monitoring thread for an SSH process.

        Args:
            handle: RunningGrpcServer to monitor
            boot_command: BootServerCommand used to start the process
        """

        def watch_process():
            """Monitor SSH process and handle termination."""
            try:
                handle.process.wait()
                exit_code = (
                    handle.process.exit_code
                    if hasattr(handle.process, "exit_code")
                    else None
                )

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
                error_msg = (
                    f"SSH process {handle.process_id} terminated with exception: {e}"
                )
                print(f"ERROR: {error_msg}")
                handle.startup_error = error_msg

        watcher = threading.Thread(
            target=watch_process, name=f"SSHWatcher-{handle.process_id}", daemon=True
        )
        watcher.start()
        self.watchers.append(watcher)

    def stop_process(self, handle: RunningGrpcServer, timeout: float = 10.0) -> None:
        """
        Stop an SSH process by terminating the SSH connection.

        Args:
            handle: ProcessHandle for the SSH process to stop
            timeout: Maximum time to wait for graceful shutdown
        """
        if not handle.started or not handle.process:
            return

        ssh_process = handle.process

        if not ssh_process.is_alive():
            return

        try:
            print(f"Stopping SSH process {handle.process_id}...")

            # Send termination signals with proper grace period
            ssh_process.signal_group(signal.SIGTERM)

            # Wait for graceful termination
            start_time = time.time()
            while ssh_process.is_alive() and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            # Force kill if still alive
            if ssh_process.is_alive():
                print(f"Force killing SSH process {handle.process_id} after timeout")
                ssh_process.signal_group(signal.SIGKILL)

                # Wait a bit more for forced termination
                start_time = time.time()
                while ssh_process.is_alive() and (time.time() - start_time) < 2.0:
                    time.sleep(0.1)

            print(f"SSH process {handle.process_id} stopped")

        except Exception as e:
            print(f"Warning: Error stopping SSH process {handle.process_id}: {e}")

    def is_process_alive(self, handle: RunningGrpcServer) -> bool:
        """
        Check if an SSH process is still running.

        Args:
            handle: ProcessHandle to check

        Returns:
            True if SSH connection is active, False otherwise
        """
        if not handle.started or (handle.process is None):
            return False

        try:
            return handle.process.is_alive()
        except Exception:
            return False

    def wait_for_termination(
        self, handle: RunningGrpcServer, timeout: Optional[float] = None
    ) -> None:
        """
        Wait for SSH process to terminate.

        Args:
            handle: ProcessHandle to wait for
            timeout: Maximum time to wait
        """
        if handle.started and handle.process:
            try:
                start_time = time.time()
                while handle.process.is_alive():
                    if timeout and (time.time() - start_time) > timeout:
                        break
                    time.sleep(0.1)
            except Exception:
                pass

    def cleanup(self) -> None:
        """Stop all SSH processes and cleanup resources."""
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
