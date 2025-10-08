"""
Provides SSH connection management using paramiko
"""

import logging
import os
import threading
import time
from typing import Callable, Dict, List, Optional

import paramiko
from druncschema.process_manager_pb2 import BootRequest


class SSHConnectionManager:
    """
    SSH connection manager using paramiko for SSH execution.
    Supports process lifecycle management of processes started via
    SSH, output capture, and exit code tracking.
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
        Initialise SSH connection manager.

        Args:
            disable_host_key_check: Disable SSH host key verification for all hosts
            disable_localhost_host_key_check: Disable SSH host key verification for localhost
            logger: Logger instance for real-time output logging
            on_process_exit: Optional callback function(uuid, exit_code, exception) invoked when process exits
        """
        self.disable_host_key_check = disable_host_key_check
        self.disable_localhost_host_key_check = disable_localhost_host_key_check
        self.log = logger if logger else logging.getLogger(__name__)
        self.on_process_exit = on_process_exit

        # Connection and channel tracking (one per UUID)
        self.connections: Dict[str, paramiko.SSHClient] = {}
        self.channels: Dict[str, paramiko.Channel] = {}

        # Thread tracking for monitoring
        self.watchers: List[threading.Thread] = []

        # Output capture for process monitoring
        self.stdout_buffers: Dict[str, List[str]] = {}
        self.stderr_buffers: Dict[str, List[str]] = {}

        # Exit code tracking
        self.exit_codes: Dict[str, Optional[int]] = {}

        # Thread-safe locks
        self.locks: Dict[str, threading.Lock] = {}
        self.global_lock = threading.Lock()

    def _load_ssh_config(self, hostname: str) -> Dict[str, any]:
        """
        Load SSH configuration from ~/.ssh/config for the given hostname.

        Reads the user's SSH config file and returns configuration options
        for the specified host, including host aliases, custom ports, identity
        files, and other SSH parameters.

        Args:
            hostname: Target hostname to look up in SSH config

        Returns:
            Dictionary of SSH configuration options for the host
        """
        ssh_config_path = os.path.expanduser("~/.ssh/config")
        ssh_config = {}

        if os.path.exists(ssh_config_path):
            try:
                config = paramiko.SSHConfig()
                with open(ssh_config_path, "r") as f:
                    config.parse(f)
                ssh_config = config.lookup(hostname)
                self.log.debug(
                    f"Loaded SSH config for {hostname}: {list(ssh_config.keys())}"
                )
            except Exception as e:
                self.log.warning(f"Could not parse SSH config: {e}")

        self.log.debug(f"SSH config identity files: {ssh_config.get('identityfile')}")
        return ssh_config

    def _create_ssh_client(
        self, hostname: str, user: str, enable_agent: bool = True
    ) -> paramiko.SSHClient:
        """
        Create and connect an SSH client

        Args:
            hostname: Target hostname (may be alias from SSH config)
            user: Default SSH username (overridden by SSH config if specified)
            enable_agent: Whether to allow SSH agent and key lookups (default: True)

        Returns:
            Connected paramiko.SSHClient instance

        Raises:
            RuntimeError: If connection fails
        """
        client = paramiko.SSHClient()

        try:
            # Load SSH config for this host
            ssh_config = self._load_ssh_config(hostname)

            # Determine actual connection parameters from SSH config
            actual_hostname = ssh_config.get("hostname", hostname)
            actual_user = ssh_config.get("user", user)
            port = int(ssh_config.get("port", 22))
            identity_files = ssh_config.get("identityfile", None)

            # Determine host key checking policy
            # Special case: localhost connections may bypass host key checks
            disable_host_key_check = self.disable_host_key_check or (
                self.disable_localhost_host_key_check
                and hostname in ("localhost", "127.0.0.1", "::1")
            )

            if disable_host_key_check:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            else:
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.RejectPolicy())

            # Build connection parameters with common timeout values
            connect_kwargs = {
                "hostname": actual_hostname,
                "username": actual_user,
                "port": port,
                "timeout": 10.0,
                "banner_timeout": 10.0,
            }

            # Configure authentication methods based on caller requirements
            if not enable_agent:
                connect_kwargs["look_for_keys"] = False
                connect_kwargs["allow_agent"] = False

            # Add identity file from SSH config if present
            if identity_files:
                connect_kwargs["key_filename"] = identity_files
                self.log.debug(
                    f"Using identity files from SSH config: {identity_files}"
                )
            else:
                self.log.warning(
                    f"No identity files found in SSH config for {hostname}"
                )

            # Establish connection to remote host
            self.log.debug(f"Connecting to {actual_user}@{actual_hostname}:{port}")
            client.connect(**connect_kwargs)

            return client

        except Exception as e:
            # Clean up client on connection failure
            try:
                client.close()
            except Exception:
                pass
            raise RuntimeError(f"Failed to connect to {user}@{hostname}: {e}") from e

    def get_active_process_keys(self) -> List[str]:
        """Get list of active process UUIDs."""
        with self.global_lock:
            return list(self.connections.keys())

    def execute_ssh_command(
        self,
        uuid: str,
        boot_request: BootRequest,
        hostname: str,
        user: str,
        command: str,
        log_file: str,
        env_vars: Dict[str, str] = None,
    ) -> paramiko.Channel:
        """
        Execute SSH command

        Args:
            uuid: Unique identifier for this process
            boot_request: Original boot request
            hostname: Target hostname
            user: SSH username
            command: Remote command to execute
            log_file: Path to log file for output (on remote host)
            env_vars: Environment variables to export

        Returns:
            paramiko.Channel object

        Raises:
            RuntimeError: If SSH connection or execution fails
        """
        try:
            # Create and connect SSH client
            client = self._create_ssh_client(hostname, user, enable_agent=True)

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

            transport = client.get_transport()
            channel = transport.open_session()
            # PTY ensures remote process receives SIGHUP when connection closes
            channel.get_pty()
            channel.exec_command(remote_cmd)

            # Store connection and channel for lifecycle management
            with self.global_lock:
                self.connections[uuid] = client
                self.channels[uuid] = channel
                self.locks[uuid] = threading.Lock()
                self.exit_codes[uuid] = None
                self.stdout_buffers[uuid] = []
                self.stderr_buffers[uuid] = []

            # Start monitoring thread for output capture and exit detection
            self._start_process_watcher(uuid, channel)

            self.log.debug(f"SSH command started for {uuid}: {command}")
            return channel

        except Exception as e:
            # Clean up on failure
            if uuid in self.connections:
                try:
                    self.connections[uuid].close()
                except Exception:
                    pass
                del self.connections[uuid]
            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def is_process_alive(self, uuid: str) -> bool:
        """
        Check if SSH process is alive.

        Args:
            uuid: Process UUID to check

        Returns:
            True if process is alive, False otherwise
        """
        if uuid not in self.channels:
            return False

        channel = self.channels[uuid]
        return not channel.exit_status_ready()

    def get_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated, None if still running or not found
        """
        if uuid not in self.exit_codes:
            return None

        return self.exit_codes[uuid]

    def terminate_process(self, uuid: str, timeout: float = 10.0) -> None:
        """
        Terminate SSH process by closing the connection.

        The PTY allocation ensures the remote process receives SIGHUP when
        the SSH connection closes, causing graceful termination.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination (used for waiting)
        """
        if uuid not in self.connections:
            return

        channel = self.channels.get(uuid)
        if channel and channel.exit_status_ready():
            return  # Already terminated

        try:
            # Close the channel and connection
            # This sends SIGHUP to the remote process due to PTY
            if channel:
                channel.close()

            self.connections[uuid].close()

            # Wait for exit status with timeout
            if channel:
                start_time = time.time()
                while (
                    not channel.exit_status_ready()
                    and (time.time() - start_time) < timeout
                ):
                    time.sleep(0.1)

        except Exception as e:
            self.log.warning(f"Error terminating process {uuid}: {e}")

    def get_process_stdout(self, uuid: str) -> Optional[str]:
        """
        Get stdout from process.

        Note: With output redirection to log files, stdout capture is minimal.
        This primarily captures the initial "SSHPM: Starting process..." message.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stdout content as string, None if not found
        """
        if uuid not in self.stdout_buffers:
            return None

        with self.locks.get(uuid, threading.Lock()):
            return "\n".join(self.stdout_buffers[uuid])

    def get_process_stderr(self, uuid: str) -> Optional[str]:
        """
        Get stderr from process.

        Note: With output redirection to log files, stderr capture is minimal.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stderr content as string, None if not found
        """
        if uuid not in self.stderr_buffers:
            return None

        with self.locks.get(uuid, threading.Lock()):
            return "\n".join(self.stderr_buffers[uuid])

    def cleanup_process(self, uuid: str) -> None:
        """
        Clean up process resources.

        Terminates the process (if still running) and releases all associated resources.

        Args:
            uuid: Process UUID to clean up
        """
        # Terminate if still running
        if uuid in self.channels:
            channel = self.channels[uuid]
            if not channel.exit_status_ready():
                self.terminate_process(uuid)

        # Close and remove connection
        if uuid in self.connections:
            try:
                self.connections[uuid].close()
            except Exception as e:
                self.log.debug(f"Error closing connection for {uuid}: {e}")

            with self.global_lock:
                del self.connections[uuid]

        # Remove channel
        if uuid in self.channels:
            with self.global_lock:
                del self.channels[uuid]

        # Clean up tracking structures
        with self.global_lock:
            if uuid in self.exit_codes:
                del self.exit_codes[uuid]
            if uuid in self.stdout_buffers:
                del self.stdout_buffers[uuid]
            if uuid in self.stderr_buffers:
                del self.stderr_buffers[uuid]
            if uuid in self.locks:
                del self.locks[uuid]

    def cleanup_all(self) -> None:
        """Clean up all processes and resources."""
        # Terminate all processes
        with self.global_lock:
            uuids = list(self.connections.keys())

        for uuid in uuids:
            self.cleanup_process(uuid)

        # Wait for watcher threads
        for watcher in self.watchers:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        self.watchers.clear()

    def _start_process_watcher(self, uuid: str, channel: paramiko.Channel) -> None:
        """
        Start a monitoring thread for a channel.

        This thread waits for the channel to complete, captures the exit code,
        and invokes the exit callback if provided. Output is logged in real-time
        and buffered for later retrieval.

        Args:
            uuid: Process UUID
            channel: paramiko.Channel to monitor
        """

        def watch_process():
            exception = None
            try:
                # Read output for real-time logging
                # Most output goes to log file, but we capture SSH-level messages
                while not channel.exit_status_ready():
                    # Non-blocking read with timeout
                    if channel.recv_ready():
                        data = channel.recv(4096).decode("utf-8", errors="replace")
                        for line in data.splitlines():
                            if line:
                                self.log.debug(line)
                                with self.locks[uuid]:
                                    self.stdout_buffers[uuid].append(line)

                    if channel.recv_stderr_ready():
                        data = channel.recv_stderr(4096).decode(
                            "utf-8", errors="replace"
                        )
                        for line in data.splitlines():
                            if line:
                                self.log.error(line)
                                with self.locks[uuid]:
                                    self.stderr_buffers[uuid].append(line)

                    time.sleep(0.1)

                # Note that because we use SIGHUP to terminate
                # we don't get an exit code from the remote process
                # in this case paramiko give us a -1 exit status.
                exit_code = channel.recv_exit_status()

                with self.global_lock:
                    self.exit_codes[uuid] = exit_code

                self.log.debug(f"SSH process {uuid} exited with code {exit_code}")

            except Exception as e:
                exception = e
                self.log.error(f"SSH process {uuid} watcher error: {e}")
                with self.global_lock:
                    self.exit_codes[uuid] = None

            # Invoke callback with results
            if self.on_process_exit:
                exit_code = self.exit_codes.pop(uuid, None)
                try:
                    self.on_process_exit(uuid, exit_code, exception)
                except Exception as callback_error:
                    self.log.error(
                        f"Error in process exit callback for {uuid}: {callback_error}"
                    )

        watcher = threading.Thread(
            target=watch_process, name=f"SSHWatcher-{uuid}", daemon=True
        )
        watcher.start()
        self.watchers.append(watcher)

    def read_remote_log_file(
        self, hostname: str, user: str, log_file: str, num_lines: int = 100
    ) -> List[str]:
        """
        Read remote log file via SSH.

        Creates a temporary SSH connection to read the log file and returns
        the last N lines.

        Args:
            hostname: Target hostname
            user: SSH username
            log_file: Remote log file path
            num_lines: Number of lines to read from end of file

        Returns:
            List of log lines
        """
        client = None
        try:
            # Create temporary SSH client for log retrieval
            # Disable agent/key lookup for simpler temporary connection
            client = self._create_ssh_client(hostname, user, enable_agent=False)

            # Execute tail command to retrieve last N lines
            stdin, stdout, stderr = client.exec_command(
                f"tail -{num_lines} {log_file}", timeout=10.0
            )

            # Read output lines
            lines = stdout.readlines()

            # Check for errors during command execution
            error_output = stderr.read().decode("utf-8", errors="replace")
            if error_output:
                self.log.warning(f"Error reading log file: {error_output}")
                return [f"Could not retrieve logs: {error_output}"]

            return lines

        except Exception as e:
            self.log.error(f"Failed to read remote log file: {e}")
            return [f"Could not retrieve logs: {e!s}"]

        finally:
            # Always close temporary connection
            if client:
                try:
                    client.close()
                except Exception:
                    pass
