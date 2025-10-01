"""
SSH Connection Manager for SSHProcessManager

This module provides SSH connection management specifically designed to be used
within SSHProcessManager, replacing direct sh.Command usage with subprocess-based
SSH execution that provides better compatibility and control.
"""

import os
import signal
import subprocess
import tempfile
import threading
from typing import Dict, List, Optional

from druncschema.process_manager_pb2 import BootRequest


class SSHConnectionManager:
    """
    SSH connection manager using subprocess for SSH execution.

    Designed to replace sh.Command usage in SSHProcessManager with subprocess-based
    SSH execution while maintaining the same interface and behavior patterns.
    """

    def __init__(
        self,
        disable_host_key_check: bool = False,
        disable_localhost_host_key_check: bool = False,
        ssh_executable: str = "/usr/bin/ssh",
    ):
        """
        Initialize SSH connection manager.

        Args:
            disable_host_key_check: Disable SSH host key verification for all hosts
            disable_localhost_host_key_check: Disable SSH host key verification for localhost
            ssh_executable: Path to SSH executable
        """
        self.disable_host_key_check = disable_host_key_check
        self.disable_localhost_host_key_check = disable_localhost_host_key_check
        self.ssh_executable = ssh_executable

        # Process tracking
        self.processes: Dict[str, subprocess.Popen] = {}
        self.watchers: List[threading.Thread] = []

        # Output capture for compatibility with sh.Command
        self.stdout_buffers: Dict[str, List[str]] = {}
        self.stderr_buffers: Dict[str, List[str]] = {}

        # Create logger (matches SSHProcessManager pattern)
        import logging

        self.log = logging.getLogger(__name__)

    def execute_ssh_command(
        self,
        uuid: str,
        boot_request: BootRequest,
        hostname: str,
        user: str,
        command: str,
        log_file: str,
        env_vars: Dict[str, str] = None,
    ) -> subprocess.Popen:
        """
        Execute SSH command using subprocess.

        This method replaces the sh.Command SSH execution in SSHProcessManager
        with subprocess-based execution while maintaining the same behavior.

        Args:
            uuid: Unique identifier for this process
            boot_request: Original boot request
            hostname: Target hostname
            user: SSH username
            command: Remote command to execute
            log_file: Path to log file for output
            env_vars: Environment variables to export

        Returns:
            subprocess.Popen process object

        Raises:
            RuntimeError: If SSH execution fails
        """
        # Build user@host format
        user_host = f"{user}@{hostname}" if user else hostname

        # Determine SSH options based on host
        disable_host_key_check = self.disable_host_key_check or (
            self.disable_localhost_host_key_check
            and hostname in ("localhost", "127.0.0.1", "::1")
        )

        # Build SSH arguments (matches SSHProcessManager pattern)
        ssh_args = [
            self.ssh_executable,
            user_host,
            "-tt",
            "-o",
            "StrictHostKeyChecking=no",
        ]

        if disable_host_key_check:
            ssh_args.extend(
                [
                    "-o",
                    "LogLevel=error",
                    "-o",
                    "GlobalKnownHostsFile=/dev/null",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                ]
            )

        # Build remote command with environment setup
        remote_cmd = (
            'echo "SSHPM: Starting process $$ on host $HOSTNAME as user $USER";'
        )

        # Add environment variables
        if env_vars:
            cmd_env = ";".join([f'export {n}="{v}"' for n, v in env_vars.items()])
            remote_cmd += cmd_env + ";"

        # Add working directory change
        if hasattr(boot_request.process_description, "process_execution_directory"):
            remote_cmd += (
                f"cd {boot_request.process_description.process_execution_directory} ; "
            )

        # Add the actual command
        remote_cmd += command

        # Complete SSH command with output redirection
        ssh_args.append(f"{{ {remote_cmd} ; }} &> {log_file}")

        try:
            # Execute SSH command using subprocess with real-time logging
            process = subprocess.Popen(
                ssh_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                close_fds=True,
                text=True,
                bufsize=1,  # Line buffered
            )

            # Store process for tracking
            self.processes[uuid] = process

            # Start real-time logging threads
            self._start_output_logging(uuid, process)

            # Start monitoring thread
            self._start_process_watcher(uuid, process)

            self.log.debug(f"SSH command: {' '.join(ssh_args)}")
            return process

        except Exception as e:
            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def is_process_alive(self, uuid: str) -> bool:
        """
        Check if SSH process is alive.

        Args:
            uuid: Process UUID to check

        Returns:
            True if process is alive, False otherwise
        """
        if uuid not in self.processes:
            return False

        process = self.processes[uuid]
        return process.poll() is None

    def get_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated, None if still running or not found
        """
        if uuid not in self.processes:
            return None

        process = self.processes[uuid]
        return process.poll()

    def terminate_process(self, uuid: str, timeout: float = 5.0) -> None:
        """
        Terminate SSH process.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination
        """
        if uuid not in self.processes:
            return

        process = self.processes[uuid]

        if process.poll() is not None:
            return  # Already terminated

        try:
            # Send SIGTERM to process group
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except ProcessLookupError:
                return
            except Exception:
                # Fallback to terminating just the main process
                process.terminate()

            # Wait for graceful termination
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Force kill
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception:
                    process.kill()

                # Wait a bit more
                try:
                    process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    pass

        except Exception as e:
            self.log.warning(f"Error terminating process {uuid}: {e}")

    def signal_process(self, uuid: str, sig: signal.Signals) -> None:
        """
        Send signal to SSH process group.

        Args:
            uuid: Process UUID
            sig: Signal to send
        """
        if uuid not in self.processes:
            return

        process = self.processes[uuid]

        if process.poll() is not None:
            return  # Already terminated

        try:
            os.killpg(os.getpgid(process.pid), sig)
        except ProcessLookupError:
            pass
        except Exception as e:
            self.log.warning(f"Error sending signal {sig} to process {uuid}: {e}")

    def get_process_stdout(self, uuid: str) -> Optional[str]:
        """
        Get stdout from process (for compatibility with sh.Command interface).

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stdout content as string, None if not found
        """
        if uuid not in self.stdout_buffers:
            return None
        return "\n".join(self.stdout_buffers[uuid])

    def get_process_stderr(self, uuid: str) -> Optional[str]:
        """
        Get stderr from process (for compatibility with sh.Command interface).

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stderr content as string, None if not found
        """
        if uuid not in self.stderr_buffers:
            return None
        return "\n".join(self.stderr_buffers[uuid])

    def cleanup_process(self, uuid: str) -> None:
        """
        Clean up process resources.

        Args:
            uuid: Process UUID to clean up
        """
        if uuid in self.processes:
            process = self.processes[uuid]

            # Ensure process is terminated
            if process.poll() is None:
                self.terminate_process(uuid)

            # Close file handles
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()

            del self.processes[uuid]

        # Clean up output buffers
        if uuid in self.stdout_buffers:
            del self.stdout_buffers[uuid]
        if uuid in self.stderr_buffers:
            del self.stderr_buffers[uuid]

    def cleanup_all(self) -> None:
        """Clean up all processes and resources."""
        # Terminate all processes
        for uuid in list(self.processes.keys()):
            self.cleanup_process(uuid)

        # Wait for watcher threads
        for watcher in self.watchers:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        self.watchers.clear()

    def _start_output_logging(self, uuid: str, process: subprocess.Popen) -> None:
        """
        Args:
            uuid: Process UUID
            process: subprocess.Popen to monitor
        """
        # Initialize buffers
        self.stdout_buffers[uuid] = []
        self.stderr_buffers[uuid] = []

        def log_stdout():
            try:
                for line in iter(process.stdout.readline, ""):
                    if line:
                        line_stripped = line.rstrip()
                        # Log in real-time (matches sh.Command _out behavior)
                        self.log.debug(line_stripped)
                        # Also capture for later retrieval (matches sh.Command .stdout access)
                        self.stdout_buffers[uuid].append(line_stripped)
                    if process.poll() is not None:
                        break
            except Exception as e:
                self.log.error(f"Error reading stdout for {uuid}: {e}")
            finally:
                process.stdout.close()

        def log_stderr():
            try:
                for line in iter(process.stderr.readline, ""):
                    if line:
                        line_stripped = line.rstrip()
                        # Log in real-time (matches sh.Command _err behavior)
                        self.log.error(line_stripped)
                        # Also capture for later retrieval (matches sh.Command .stderr access)
                        self.stderr_buffers[uuid].append(line_stripped)
                    if process.poll() is not None:
                        break
            except Exception as e:
                self.log.error(f"Error reading stderr for {uuid}: {e}")
            finally:
                process.stderr.close()

        # Start logging threads
        stdout_thread = threading.Thread(
            target=log_stdout, name=f"SSHStdout-{uuid}", daemon=True
        )
        stderr_thread = threading.Thread(
            target=log_stderr, name=f"SSHStderr-{uuid}", daemon=True
        )

        stdout_thread.start()
        stderr_thread.start()

        self.watchers.extend([stdout_thread, stderr_thread])

    def _start_process_watcher(self, uuid: str, process: subprocess.Popen) -> None:
        """
        Start a monitoring thread for a subprocess.

        Args:
            uuid: Process UUID
            process: subprocess.Popen to monitor
        """

        def watch_process():
            try:
                exit_code = process.wait()
                self.log.debug(f"SSH process {uuid} exited with code {exit_code}")
            except Exception as e:
                self.log.error(f"SSH process {uuid} watcher error: {e}")

        watcher = threading.Thread(
            target=watch_process, name=f"SSHWatcher-{uuid}", daemon=True
        )
        watcher.start()
        self.watchers.append(watcher)

    def read_remote_log_file(
        self, hostname: str, user: str, log_file: str, num_lines: int = 100
    ) -> List[str]:
        """
        Read remote log file via SSH (for _logs_impl compatibility).

        Args:
            hostname: Target hostname
            user: SSH username
            log_file: Remote log file path
            num_lines: Number of lines to read

        Returns:
            List of log lines
        """
        user_host = f"{user}@{hostname}" if user else hostname

        disable_host_key_check = self.disable_host_key_check or (
            self.disable_localhost_host_key_check
            and hostname in ("localhost", "127.0.0.1", "::1")
        )

        # Create temporary file for output
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
            temp_file = f.name

        try:
            # Build SSH command for reading log
            ssh_args = [
                self.ssh_executable,
                user_host,
                "-tt",
                "-o",
                "StrictHostKeyChecking=no",
            ]

            if disable_host_key_check:
                ssh_args.extend(
                    [
                        "-o",
                        "LogLevel=error",
                        "-o",
                        "GlobalKnownHostsFile=/dev/null",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                    ]
                )

            ssh_args.extend(["tail", f"-{num_lines}", log_file])

            # Execute command
            subprocess.run(
                ssh_args,
                stdout=open(temp_file, "w"),
                stderr=subprocess.STDOUT,
                timeout=10.0,
            )

            # Read results
            with open(temp_file, "r") as f:
                lines = f.readlines()

            # Remove SSH connection close message if present
            if lines and "Connection to " in lines[-1] and " closed." in lines[-1]:
                lines = lines[:-1]

            return lines

        except Exception as e:
            return [f"Could not retrieve logs: {e!s}"]
        finally:
            try:
                os.remove(temp_file)
            except Exception:
                pass
