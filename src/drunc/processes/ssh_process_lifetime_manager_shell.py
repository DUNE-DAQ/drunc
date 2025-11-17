"""
Provides SSH connection and lifetime management using sh library.

This implementation uses the sh library to execute SSH commands,
replicating the behaviour of the original SSHProcessManager.
"""

import getpass
import logging
import os
import signal
import tempfile
import threading
from time import sleep
from typing import Callable, Dict, List, Optional

import sh
from druncschema.process_manager_pb2 import BootRequest

from drunc.process_manager.utils import on_parent_exit
from drunc.processes.ssh_process_lifetime_manager import ProcessLifetimeManager
from drunc.utils.utils import get_logger


class ProcessWatcherThread(threading.Thread):
    """
    Thread that monitors a background SSH process and invokes callback on exit.
    """

    def __init__(
        self,
        uuid: str,
        process: sh.RunningCommand,
        on_exit: Optional[Callable[[str, Optional[int], Optional[Exception]], None]],
        logger: logging.Logger,
    ):
        """
        Initialise process watcher thread.

        Args:
            uuid: Process UUID to monitor
            process: sh.RunningCommand instance to monitor
            on_exit: Callback function invoked on process exit
            logger: Logger instance for output
        """
        super().__init__(name=f"ShellWatcher-{uuid}", daemon=True)
        self.uuid = uuid
        self.process = process
        self.on_exit = on_exit
        self.logger = logger

    def run(self):
        """
        Monitor process and invoke callback on exit.
        """
        exception = None
        exit_code = None

        try:
            # Wait for process to complete
            self.process.wait()
            exit_code = self.process.exit_code
            self.logger.debug(f"Shell process {self.uuid} exited with code {exit_code}")

        except sh.ErrorReturnCode as e:
            # Process exited with non-zero code
            exception = e
            exit_code = e.exit_code
            self.logger.error(f"Shell process {self.uuid} error: {e}")

        except Exception as e:
            # Unexpected error during monitoring
            exception = e
            self.logger.error(f"Shell process {self.uuid} watcher error: {e}")

        # Invoke callback with results
        if self.on_exit:
            try:
                self.on_exit(self.uuid, exit_code, exception)
            except Exception as callback_error:
                self.logger.error(
                    f"Error in process exit callback for {self.uuid}: {callback_error}"
                )


class SSHProcessLifetimeManagerShell(ProcessLifetimeManager):
    """
    Manages process lifecycle using sh library for SSH connections.

    This implementation uses the sh library's SSH command wrapper to start
    and manage remote processes, matching the behaviour of the original
    SSHProcessManager implementation.
    """

    def __init__(
        self,
        disable_host_key_check: bool = False,
        disable_localhost_host_key_check: bool = False,
        logger: Optional[logging.Logger] = None,
        on_process_exit: Optional[
            Callable[[str, Optional[int], Optional[Exception]], None]
        ] = None,
    ):
        """
        Initialise SSH process lifetime manager using sh library.

        Args:
            disable_host_key_check: Disable SSH host key verification for all hosts
            disable_localhost_host_key_check: Disable SSH host key verification for localhost
            logger: Logger instance for real-time output logging
            on_process_exit: Optional callback function(uuid, exit_code, exception) invoked when process exits
        """
        self.disable_host_key_check = disable_host_key_check
        self.disable_localhost_host_key_check = disable_localhost_host_key_check
        self.log = logger if logger else get_logger(__name__)
        self.on_process_exit = on_process_exit

        # Create SSH command wrapper
        self.ssh = sh.Command("/usr/bin/ssh")

        # Process tracking (one per UUID)
        self.process_store: Dict[str, sh.RunningCommand] = {}

        # Thread tracking for monitoring
        self.watchers: List[threading.Thread] = []

        # Thread-safe lock for process store modifications
        self.lock = threading.Lock()

    def get_active_process_keys(self) -> List[str]:
        """
        Get list of active process UUIDs.

        Returns:
            List of active process UUID strings
        """
        with self.lock:
            return list(self.process_store.keys())

    def start_process(self, uuid: str, boot_request: BootRequest) -> None:
        """
        Start a remote process via SSH using the boot request configuration.

        Extracts all necessary parameters from the boot request and executes
        the process on the remote host using sh library's SSH wrapper.

        Args:
            uuid: Unique identifier for this process
            boot_request: BootRequest containing process configuration, metadata,
                        environment variables, and execution parameters

        Raises:
            RuntimeError: If SSH connection or process execution fails
        """
        # Extract connection parameters from boot request metadata
        hostname = boot_request.process_description.metadata.hostname
        user = boot_request.process_description.metadata.user
        log_file = boot_request.process_description.process_logs_path

        # Extract environment variables from boot request
        env_vars = (
            dict(boot_request.process_description.env)
            if boot_request.process_description.env
            else {}
        )

        # Build command string from executable and arguments
        cmd = ""
        for exe_arg in boot_request.process_description.executable_and_arguments:
            cmd += exe_arg.exec
            for arg in exe_arg.args:
                cmd += f" {arg}"
            cmd += ";"

        # Remove trailing semicolon if present
        if cmd.endswith(";"):
            cmd = cmd[:-1]

        # Execute the command via SSH
        self._execute_ssh_command(
            uuid=uuid,
            boot_request=boot_request,
            hostname=hostname,
            user=user if user else getpass.getuser(),
            command=cmd,
            log_file=log_file,
            env_vars=env_vars,
        )

    def _execute_ssh_command(
        self,
        uuid: str,
        boot_request: BootRequest,
        hostname: str,
        user: str,
        command: str,
        log_file: str,
        env_vars: Dict[str, str] = None,
    ) -> None:
        """
        Execute SSH command using sh library.

        Args:
            uuid: Unique identifier for this process
            boot_request: Original boot request
            hostname: Target hostname
            user: SSH username
            command: Remote command to execute
            log_file: Path to log file for output (on remote host)
            env_vars: Environment variables to export

        Raises:
            RuntimeError: If SSH connection or execution fails
        """
        try:
            # Determine platform for platform-specific options
            platform = os.uname().sysname.lower()
            is_macos = "darwin" in platform

            # Build user@host string
            user_host = f"{user}@{hostname}"

            # Determine host key checking policy
            disable_host_key_check = self.disable_host_key_check or (
                self.disable_localhost_host_key_check
                and hostname in ("localhost", "127.0.0.1", "::1")
            )

            # Build remote command with environment setup and output redirection
            remote_cmd = (
                'echo "SSHPM: Starting process $$ on host $HOSTNAME as user $USER";'
            )

            # Add environment variables
            if env_vars:
                cmd_env = ";".join([f'export {n}="{v}"' for n, v in env_vars.items()])
                remote_cmd += cmd_env + ";"

            # Add working directory change if specified
            if hasattr(boot_request.process_description, "process_execution_directory"):
                remote_cmd += f"cd {boot_request.process_description.process_execution_directory} ; "

            # Add the actual command with output redirection
            remote_cmd += f"{{ {command} ; }} &> {log_file}"

            # Build SSH arguments
            arguments = [user_host, "-tt", "-o", "StrictHostKeyChecking=no"]

            # Add host key check bypass options if configured
            if disable_host_key_check:
                arguments.extend(
                    [
                        "-o",
                        "LogLevel=error",
                        "-o",
                        "GlobalKnownHostsFile=/dev/null",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                    ]
                )

            # Add the remote command
            arguments.append(remote_cmd)

            # Execute SSH command in background
            process = self.ssh(
                *arguments,
                _out=self.log.debug,
                _err=self.log.error,
                _bg=True,
                _bg_exc=False,
                _new_session=True,
                _preexec_fn=on_parent_exit(signal.SIGTERM) if not is_macos else None,
            )

            # Store process for lifecycle management
            with self.lock:
                self.process_store[uuid] = process

            # Start monitoring thread for exit detection
            self._start_process_watcher(uuid, process)

            self.log.debug(f"SSH command started for {uuid}: {command}")

        except Exception as e:
            # Clean up on failure
            with self.lock:
                if uuid in self.process_store:
                    del self.process_store[uuid]
            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def _start_process_watcher(self, uuid: str, process: sh.RunningCommand) -> None:
        """
        Start a monitoring thread for a process.

        This thread waits for the process to complete, captures the exit code,
        and invokes the exit callback if provided.

        Args:
            uuid: Process UUID
            process: sh.RunningCommand to monitor
        """
        watcher = ProcessWatcherThread(
            uuid=uuid,
            process=process,
            on_exit=self.on_process_exit,
            logger=self.log,
        )
        watcher.start()
        self.watchers.append(watcher)

    def is_process_alive(self, uuid: str) -> bool:
        """
        Check if process is alive.

        Args:
            uuid: Process UUID to check

        Returns:
            True if process is alive, False otherwise
        """
        if uuid not in self.process_store:
            return False

        process = self.process_store[uuid]
        return process.is_alive()

    def get_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated, None if still running or not found
        """
        if uuid not in self.process_store:
            return None

        process = self.process_store[uuid]
        if process.is_alive():
            return None

        try:
            return process.exit_code
        except Exception:
            return None

    def terminate_process(self, uuid: str, timeout: float = 10.0) -> None:
        """
        Terminate process by sending signals.

        Sends SIGQUIT followed by SIGKILL if necessary, with the configured
        timeout between signals.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout between signals in seconds
        """
        if uuid not in self.process_store:
            return

        process = self.process_store[uuid]

        if not process.is_alive():
            return  # Already terminated

        try:
            # Signal sequence: SIGQUIT (graceful) then SIGKILL (forceful)
            signal_sequence = [
                signal.SIGQUIT,
                signal.SIGKILL,
            ]

            for sig in signal_sequence:
                if not process.is_alive():
                    self.log.info(f"Process {uuid} terminated")
                    break

                self.log.debug(
                    f"Sending signal '{str(sig).split('.')[-1]}' to process {uuid}"
                )
                process.signal_group(sig)

                if not process.is_alive():
                    break

                sleep(timeout)

        except Exception as e:
            self.log.warning(f"Error terminating process {uuid}: {e}")

    def get_process_stdout(self, uuid: str) -> Optional[str]:
        """
        Get stdout from process.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stdout content as string, None if not found
        """
        if uuid not in self.process_store:
            return None

        try:
            process = self.process_store[uuid]
            if hasattr(process, "stdout"):
                stdout_data = process.stdout
                if stdout_data:
                    return str(stdout_data)
        except Exception as e:
            self.log.debug(f"Error getting stdout for {uuid}: {e}")

        return None

    def get_process_stderr(self, uuid: str) -> Optional[str]:
        """
        Get stderr from process.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stderr content as string, None if not found
        """
        if uuid not in self.process_store:
            return None

        try:
            process = self.process_store[uuid]
            if hasattr(process, "stderr"):
                stderr_data = process.stderr
                if stderr_data:
                    return str(stderr_data)
        except Exception as e:
            self.log.debug(f"Error getting stderr for {uuid}: {e}")

        return None

    def cleanup_process(self, uuid: str) -> None:
        """
        Clean up process resources.

        Terminates the process (if still running) and releases all associated resources.

        Args:
            uuid: Process UUID to clean up
        """
        # Terminate if still running
        if uuid in self.process_store:
            process = self.process_store[uuid]
            if process.is_alive():
                self.terminate_process(uuid)

        # Remove from process store
        with self.lock:
            if uuid in self.process_store:
                del self.process_store[uuid]

    def cleanup_all(self) -> None:
        """
        Clean up all processes and resources.

        Terminates all managed processes and releases all associated resources.
        """
        # Get list of UUIDs to terminate
        with self.lock:
            uuids = list(self.process_store.keys())

        # Terminate all processes
        for uuid in uuids:
            self.cleanup_process(uuid)

        # Wait for watcher threads
        for watcher in self.watchers:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        self.watchers.clear()

    def read_log_file(
        self, hostname: str, user: str, log_file: str, num_lines: int = 100
    ) -> List[str]:
        """
        Read remote log file via SSH.

        Creates a temporary SSH connection to read the log file and returns
        the last N lines using the tail command.

        Args:
            hostname: Target hostname
            user: SSH username
            log_file: Remote log file path
            num_lines: Number of lines to read from end of file

        Returns:
            List of log lines
        """
        # Create temporary file for output
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        try:
            # Build user@host string
            user_host = f"{user}@{hostname}"

            # Determine host key checking policy
            disable_host_key_check = self.disable_host_key_check or (
                self.disable_localhost_host_key_check
                and hostname in ("localhost", "127.0.0.1", "::1")
            )

            # Build SSH arguments
            arguments = [user_host, "-tt", "-o", "StrictHostKeyChecking=no"]

            # Add host key check bypass options if configured
            if disable_host_key_check:
                arguments.extend(
                    [
                        "-o",
                        "LogLevel=error",
                        "-o",
                        "GlobalKnownHostsFile=/dev/null",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                    ]
                )

            # Add tail command
            arguments.extend(["tail", f"-{num_lines}", log_file])

            # Execute SSH command with output redirection
            self.ssh(
                *arguments,
                _out=temp_file.name,
                _err_to_out=True,
            )

            # Read output lines from temporary file
            with open(temp_file.name) as f:
                lines = f.readlines()

                # Remove SSH connection closure message if present
                if lines and "Connection to " in lines[-1] and " closed." in lines[-1]:
                    lines = lines[:-1]

                return lines

        except Exception as e:
            self.log.error(f"Failed to read remote log file: {e}")
            return [f"Could not retrieve logs: {e!s}"]

        finally:
            # Clean up temporary file
            try:
                os.remove(temp_file.name)
            except Exception:
                pass

    def validate_host_connection(
        self,
        host: str,
        auth_method: str,
        user: str = getpass.getuser(),
    ) -> None:
        """
        Validate SSH connection to the specified host.

        Attempts to establish an SSH connection to the host and execute a
        simple echo command to verify connectivity.

        Args:
            host: Target hostname
            auth_method: Authentication method (not used in sh implementation)
            user: SSH username (default: current user)

        Raises:
            RuntimeError: If SSH connection or command execution fails
        """
        try:
            # Build user@host string
            user_host = f"{user}@{host}"

            # Determine host key checking policy
            disable_host_key_check = self.disable_host_key_check or (
                self.disable_localhost_host_key_check
                and host in ("localhost", "127.0.0.1", "::1")
            )

            # Build remote command
            remote_cmd = f'echo "{user} established SSH successfully";'

            # Build SSH arguments
            arguments = [user_host, "-tt", "-o", "StrictHostKeyChecking=no"]

            # Add host key check bypass options if configured
            if disable_host_key_check:
                arguments.extend(
                    [
                        "-o",
                        "LogLevel=error",
                        "-o",
                        "GlobalKnownHostsFile=/dev/null",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                    ]
                )

            # Add the remote command
            arguments.append(remote_cmd)

            # Execute SSH command and wait for completion
            self.ssh(*arguments)

            self.log.debug(f"SSH validation successful for {user}@{host}")

        except Exception as e:
            self.log.error(f"SSH validation failed for {user}@{host}: {e}")
            raise RuntimeError(f"SSH connection validation failed: {e}")
