"""
Provides SSH connection and lifetime management
"""

import getpass
import importlib.util
import logging
import os
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import paramiko
from druncschema.process_manager_pb2 import BootRequest

from drunc.processes.connection_utils import wait_for
from drunc.processes.process_metadata import ProcessMetadata
from drunc.processes.ssh_process_lifetime_manager import ProcessLifetimeManager
from drunc.utils.utils import get_logger


class SSHProcessLifetimeManagerParamiko(ProcessLifetimeManager):
    """
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
        self.log = logger if logger else get_logger(__name__)
        self.on_process_exit = on_process_exit

        # Connection and channel tracking (one per UUID)
        self.connections: Dict[str, paramiko.SSHClient] = {}
        self.channels: Dict[str, paramiko.Channel] = {}
        self.metadata: Dict[str, ProcessMetadata] = {}

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
        self.issued_GSS_API_warning = False

    @staticmethod
    def get_metadata_file_path(uuid: str) -> str:
        """Generate metadata file path for a given process UUID."""
        return (
            f"${{XDG_RUNTIME_DIR:?XDG_RUNTIME_DIR not set}}/drunc/metadata_{uuid}.json"
        )

    def _load_ssh_config(self, hostname: str) -> Dict[str, any]:
        """
        Load SSH configuration for the given hostname.

        Reads the user's SSH config file and returns configuration options
        for the specified host, including host aliases, custom ports, identity
        files, and other SSH parameters.

        Args:
            hostname: Target hostname to look up in SSH config

        Returns:
            Dictionary of SSH configuration options for the host
        """

        # Possible SSH config file locations in order of preference
        ssh_config_paths = [
            os.path.expanduser("~/.ssh/config"),  # standard location
            "/root/.ssh/config",  # CI environments
        ]

        for ssh_config_path in ssh_config_paths:
            if os.path.exists(ssh_config_path):
                try:
                    config = paramiko.SSHConfig()
                    with open(ssh_config_path, "r") as f:
                        config.parse(f)
                    ssh_config = config.lookup(hostname)
                    self.log.debug(
                        f"Loaded SSH config for {hostname} from {ssh_config_path}: {list(ssh_config.keys())}"
                    )
                    # use the first config that was found
                    return ssh_config
                except Exception as e:
                    self.log.warning(
                        f"Could not parse SSH config at {ssh_config_path}: {e}"
                    )
                    continue

    def _add_identity_file(
        self, connect_kwargs: Dict[str, Any], identity_files: List[str]
    ) -> None:
        """
        Add identity files to the SSH connection parameters.

        Args:
            connect_kwargs: The connection parameters dictionary to update.
            identity_files: A list of identity file paths to add.
        """
        if identity_files:
            connect_kwargs["key_filename"] = identity_files
            self.log.debug(f"Using identity files from SSH config: {identity_files}")
        else:
            self.log.critical("No identity files found!")
            raise RuntimeError(
                "No identity files specified for public key authentication"
            )

    def _has_gss_api_support(self) -> bool:
        """
        Check if paramiko has GSS-API support available.

        Returns:
            bool: True if GSS-API support is available, False otherwise
        """

        if importlib.util.find_spec("gssapi") is not None:
            return True
        else:
            return False

    def _create_ssh_client(
        self,
        hostname: str,
        user: str,
        enable_agent: bool = True,
        auth_methods: List[str] | None = None,
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

            # As we are supporting GSSAPI authentication with kerberos, we need to map
            # the name "localhost" to the actual hostname of the machine (the client
            # must request the hostname rather than the alias "localhost" or host IP
            # address to match the server principal in the kerberos ticket, as kerberos
            # requires a SPN that matches the server's hostname, IP addresses are
            # ignored).
            if actual_hostname == "localhost":
                actual_hostname = socket.gethostname()

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

            has_gssapi = self._has_gss_api_support()

            # Configure authentication methods
            kPublicKeyAuth: str = "publickey"
            kKerberosAuth: str = "gssapi-with-mic"
            if auth_methods is not None and auth_methods == [kKerberosAuth]:
                if has_gssapi:
                    connect_kwargs["gss_auth"] = True
                    connect_kwargs["gss_kex"] = True
                    connect_kwargs["gss_deleg_creds"] = True
                else:
                    self.log.error("GSS-API authentication requested but not supported")
                connect_kwargs["allow_agent"] = False
                connect_kwargs["look_for_keys"] = False
            elif auth_methods is not None and auth_methods == [kPublicKeyAuth]:
                connect_kwargs["allow_agent"] = enable_agent
                connect_kwargs["look_for_keys"] = enable_agent
                self._add_identity_file(connect_kwargs, identity_files)
            elif auth_methods is None or (
                kKerberosAuth in auth_methods and kPublicKeyAuth in auth_methods
            ):
                if has_gssapi:
                    connect_kwargs["gss_auth"] = True
                    connect_kwargs["gss_kex"] = True
                    connect_kwargs["gss_deleg_creds"] = True
                else:
                    if not self.issued_GSS_API_warning:
                        self.log.warning(
                            "GSS-API authentication requested but not supported"
                        )
                        self.issued_GSS_API_warning = True
                connect_kwargs["allow_agent"] = enable_agent
                connect_kwargs["look_for_keys"] = enable_agent
                self._add_identity_file(connect_kwargs, identity_files)

            # Ensure agent options are honored if disabled by caller
            if not enable_agent:
                connect_kwargs["look_for_keys"] = False
                connect_kwargs["allow_agent"] = False

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
            self.log.error(f"SSH client failed to connect to {user}@{hostname}: {e}")
            return None

    def get_active_process_keys(self) -> List[str]:
        """Get list of active process UUIDs."""
        with self.global_lock:
            return list(self.connections.keys())

    def start_process(self, uuid: str, boot_request: BootRequest) -> None:
        """
        Start a remote process via SSH using the boot request configuration.

        Extracts all necessary parameters from the boot request and delegates
        to _execute_ssh_command for SSH connection and process execution.

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

    def _cleanup_connection(self, uuid: str) -> None:
        """
        Clean up SSH connection for a given UUID.

        Args:
            uuid: Process UUID to clean up
        """
        if uuid in self.connections:
            try:
                self.connections[uuid].close()
            except Exception as e:
                self.log.debug(f"Error closing connection for {uuid}: {e}")
                pass

            with self.global_lock:
                del self.connections[uuid]

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
        channel_alive = not channel.exit_status_ready()

        metadata: ProcessMetadata = self.metadata.get(uuid, None)
        if metadata is None:
            self.log.debug(f"No metadata found for UUID {uuid} whilst checking alive.")
            return channel_alive

        remote_process_alive = self._is_remote_process_alive(
            metadata.hostname, metadata.user, metadata.pid
        )
        return channel_alive and remote_process_alive

    def pop_early_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code. Cleaning up all process resources if the
        exit code is found. The only way this doesn't return None is if
        the process is dead without kill_process being called.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated, None if still running or not found
        """

        if uuid not in self.exit_codes:
            self.log.debug(f"Process {uuid} not found in store for exit code retrieval")
            return None

        with self.global_lock:
            early_exit_code = self.exit_codes.get(uuid)

        if early_exit_code is not None:
            self.log.warning(
                f"Process {uuid} exited early without being killed. Exit code {early_exit_code}"
            )
            self.log.debug(
                f"Cleaning up resources for process {uuid} with exit code {early_exit_code}"
            )
            self._cleanup_process_resources(uuid)

        return early_exit_code

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

    def _cleanup_process_resources(self, uuid: str) -> None:
        """
        Remove all resources associated with a process UUID.

        Cleans up connections, channels, buffers, locks, and metadata.

        Args:
            uuid: Process UUID to clean up
        """
        with self.global_lock:
            # Close and remove connection
            if uuid in self.connections:
                try:
                    self.connections[uuid].close()
                except Exception as e:
                    self.log.debug(f"Error closing connection for {uuid}: {e}")
                del self.connections[uuid]

            # Remove channel
            if uuid in self.channels:
                del self.channels[uuid]

            # Clean up tracking structures
            if uuid in self.exit_codes:
                del self.exit_codes[uuid]
            if uuid in self.stdout_buffers:
                del self.stdout_buffers[uuid]
            if uuid in self.stderr_buffers:
                del self.stderr_buffers[uuid]
            if uuid in self.locks:
                del self.locks[uuid]
            if uuid in self.metadata:
                del self.metadata[uuid]

    def kill_all_processes(self) -> dict[str, Optional[int]]:
        """
        Clean up all processes and resources.

        Terminates all managed processes and releases all associated resources.
        Safe to call multiple times.
        """
        # Get list of UUIDs to terminate
        with self.global_lock:
            uuids = list(self.connections.keys())

        process_exit_codes: dict[str, Optional[int]] = {}

        # Terminate all processes (each kill_process call auto-cleans up on success)
        for uuid in uuids:
            try:
                process_exit_codes[uuid] = self.kill_process(
                    uuid,
                    timeout=ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
                )
            except Exception as e:
                self.log.error(f"Error during cleanup of process {uuid}: {e}")

        # Wait for watcher threads to complete
        for watcher in self.watchers:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        self.watchers.clear()
        return process_exit_codes

    def _start_process_watcher(
        self,
        uuid: str,
        channel: paramiko.Channel,
        metadata_file: str,
        hostname: str,
        user: str,
    ) -> None:
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
                self.metadata[uuid] = self.read_process_metadata(
                    uuid, metadata_file, hostname, user
                )
                if self.metadata[uuid] is None:
                    # if we don't have metadata we won't be able to send signals directly to the remote process
                    self.log.warning(
                        f"Failed to read metadata for process {uuid} within timeout. Lifecycle management will be limited."
                    )
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

                # Process runs in background but wait $PID keeps SSH channel open
                # When the process exits, wait returns with the real exit code
                exit_code = channel.recv_exit_status()

                with self.global_lock:
                    self.exit_codes[uuid] = exit_code

                self.log.debug(f"SSH process {uuid} exited with code {exit_code}")
                if exit_code == -1:
                    self.log.warning(
                        f"Process {uuid} received SIGHUP (-1). Channel status: exit_ready={channel.exit_status_ready()}, closed={channel.closed}"
                    )

            except Exception as e:
                exception = e
                self.log.error(f"SSH process {uuid} watcher error: {e}")
                with self.global_lock:
                    self.exit_codes[uuid] = None

            # Invoke callback with results
            if self.on_process_exit:
                exit_code = self.exit_codes.get(uuid, None)
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

    def read_log_file(
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
        # Create temporary SSH client for log retrieval
        # Disable agent/key lookup for simpler temporary connection
        client = self._create_ssh_client(hostname, user, enable_agent=False)
        if client is None:
            self.log.debug(
                f"Returning empty log lines for {hostname}, was unable to connect."
            )
            return []

        try:
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
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def validate_host_connection(
        self,
        host: str,
        auth_method: str,
        user: str = getpass.getuser(),
    ) -> None:
        """
        Validate SSH connections for all hosts in the collected applications.

        This method attempts to establish an SSH connection to the specified host
        and execute a simple command to verify connectivity. Used to validate access.

        Args:
            host: Target hostname
            auth_method: Authentication method to use ('publickey' or 'gssapi-with-mic')
            user: SSH username (default: current user)

        Returns:
            None

        Raises:
            RuntimeError: If SSH connection or command execution fails
        """
        # Create and connect SSH client
        client = self._create_ssh_client(
            hostname=host, user=user, enable_agent=True, auth_methods=[auth_method]
        )
        if client is None:
            raise RuntimeError("SSH connection failed")
        try:
            # Attempt SSH connection and command execution
            remote_cmd = f'echo "{user} established SSH successfully";'
            stdin, stdout, stderr = client.exec_command(remote_cmd, timeout=10.0)

            # recv_exit_status() blocks until the remote command has finished and
            # returns the exit code
            exit_status = stdout.channel.recv_exit_status()

            self.log.debug(f"SSH doctor command exit status: {exit_status}")

        finally:
            if client:
                client.close()

    def read_process_metadata(
        self,
        uuid: str,
        metadata_file: str,
        hostname: str,
        user: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_READING_METADATA,
    ) -> Optional[ProcessMetadata]:
        """
        Read process metadata from remote JSON file with retry logic.

        Args:
            uuid: Process UUID
            metadata_file: Remote path to metadata file (may contain shell variables)
            hostname: Target hostname
            user: SSH username
            timeout: Maximum time to wait for metadata file in seconds

        Returns:
            ProcessMetadata instance if file exists and is valid, None otherwise
        """

        def attempt_read():
            try:
                # Create temporary SSH connection for metadata read
                client = self._create_ssh_client(hostname, user, enable_agent=True)
                if client is None:
                    self.log.debug(f"Failed to connect for metadata read: {uuid}")
                    return None

                try:
                    # Expand shell variables in path since SFTP doesn't do this
                    stdin, stdout, stderr = client.exec_command(
                        f"echo {metadata_file}", timeout=5.0
                    )
                    expanded_path = stdout.read().decode("utf-8").strip()

                    # Use SFTP to read metadata file with expanded path
                    sftp = client.open_sftp()

                    # Read remote file
                    with sftp.file(expanded_path, "r") as f:
                        json_content = f.read().decode("utf-8")

                    sftp.close()

                    # Parse JSON and create metadata object
                    metadata = ProcessMetadata.from_json(json_content)

                    self.log.debug(f"Read metadata for {uuid} from {expanded_path}")
                    return metadata

                finally:
                    client.close()

            except Exception as e:
                self.log.debug(f"Failed to read metadata for {uuid}: {e}")
                return None

        # Use wait_for to retry reading metadata until timeout
        return wait_for(
            attempt_read,
            expected_value=lambda x: x is not None,
            timeout=timeout,
            poll_interval=0.5,
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
    ) -> paramiko.Channel:
        """Execute SSH command."""
        client = self._create_ssh_client(hostname, user, enable_agent=True)
        if client is None:
            self._cleanup_connection(uuid)
            raise RuntimeError(f"SSH connection for {uuid} failed")

        try:
            remote_cmd = (
                'echo "SSHPM: Starting process $$ on host $HOSTNAME as user $USER";'
            )

            if env_vars:
                cmd_env = ";".join([f'export {n}="{v}"' for n, v in env_vars.items()])
                remote_cmd += cmd_env + ";"

            if hasattr(boot_request.process_description, "process_execution_directory"):
                remote_cmd += f"cd {boot_request.process_description.process_execution_directory} ; "

            metadata_file = SSHProcessLifetimeManagerParamiko.get_metadata_file_path(
                uuid
            )
            remote_cmd += (
                f"mkdir -p ${{XDG_RUNTIME_DIR:?XDG_RUNTIME_DIR not set}}/drunc ; "
                f"{command} &> {log_file} & PID=$! ; "
                f'echo \'{{"pid": \'$PID\', "hostname": "{hostname}", "user": "{user}", "started_at": \'$(date +%s)\'}}\'  > {metadata_file} ; '
                f"wait $PID"
            )

            transport = client.get_transport()
            channel = transport.open_session()
            channel.get_pty()
            channel.exec_command(remote_cmd)

            # Store process execution info for this UUID
            with self.global_lock:
                self.connections[uuid] = client
                self.channels[uuid] = channel
                self.locks[uuid] = threading.Lock()
                self.exit_codes[uuid] = None
                self.stdout_buffers[uuid] = []
                self.stderr_buffers[uuid] = []

            self._start_process_watcher(uuid, channel, metadata_file, hostname, user)
            self.log.debug(f"SSH command started for {uuid}")
            return channel

        except Exception as e:
            self._cleanup_connection(uuid)

            # Check for XDG_RUNTIME_DIR error
            error_msg = str(e)
            if (
                "XDG_RUNTIME_DIR not set" in error_msg
                or "XDG_RUNTIME_DIR: parameter not set" in error_msg
            ):
                raise RuntimeError(
                    f"Failed to execute SSH command for {uuid}: XDG_RUNTIME_DIR environment variable is not set on {hostname}. "
                    f"Ensure the remote session has XDG_RUNTIME_DIR configured, or run processes as a logged-in user."
                )

            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def _kill_process_channel(self, uuid: str, channel: paramiko.Channel) -> None:
        """Attempt to terminate remote process by killing the SSH session. Remote process will recieve SIGHUP."""
        try:
            if channel:
                channel.close()
            self.connections[uuid].close()
        except Exception as e:
            self.log.debug(
                f"Exception during connection close for {uuid}. Exception: {e}"
            )
            pass

    def _wait_for_process_exit_code(self, uuid: str, timeout: float) -> Optional[int]:
        """
        Wait for specified timeout to see if a process exit code is available.

        Args:
            uuid: Process UUID to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            Exit code if process has terminated, None if still running or not found
        """

        def get_exit_code():
            with self.global_lock:
                return self.exit_codes.get(uuid, None) != None

        got_exit_code = wait_for(get_exit_code, expected_value=True, timeout=timeout)
        if got_exit_code:
            with self.global_lock:
                return self.exit_codes.get(uuid, None)
        else:
            self.log.debug(f"Timeout waiting for exit code of process {uuid}")
            return None

    def kill_process(
        self,
        uuid: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> Optional[int]:
        """
        Kill a remote process and clean up associated resources upon successful termination.

        Sends termination signals to the remote process and waits for it to die.
        If the process terminates successfully, cleans up all associated resources
        including remote metadata files and internal tracking. If termination fails,
        forcibly closes the SSH channel to send a SIGHUP to the remote process.

        Safe to call multiple times - subsequent calls will have no effect if
        resources have already been cleaned up.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination in seconds

        Returns:
            Exit code of the terminated process, or None if not found or still running
        """
        if uuid not in self.connections:
            return None

        channel = self.channels.get(uuid)
        if channel and channel.exit_status_ready():
            return channel.recv_exit_status()

        metadata = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.warning(
                f"No metadata or PID for {uuid}, closing SSH connection. Cannot guarantee process termination."
            )
            self._kill_process_channel(uuid, channel)
            exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
            self._cleanup_process_resources(uuid)
            return exit_code

        hostname = metadata.hostname
        user = metadata.user
        remote_pid = metadata.pid
        metadata_file = SSHProcessLifetimeManagerParamiko.get_metadata_file_path(uuid)
        process_dead = False

        try:
            signal_client = self._create_ssh_client(hostname, user, enable_agent=True)
            if signal_client is None:
                self.log.warning(
                    f"Could not create signal client for {uuid}, closing connection"
                )
                self._kill_process_channel(uuid, channel)
                exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
                self._cleanup_process_resources(uuid)
                return exit_code

            try:
                if not self.is_process_alive(uuid):
                    self.log.info(
                        f"Skipping killing remote process {uuid} (PID {remote_pid}). It is already dead."
                    )
                    process_dead = True

                if not process_dead:
                    self.log.debug(f"Sending SIGQUIT to remote PID {remote_pid}")
                    self._send_remote_signal(signal_client, remote_pid, "QUIT")
                    process_dead = self.wait_for_process_to_die(uuid, timeout=timeout)
                    if process_dead:
                        self.log.info(
                            f"Remote process {uuid} (PID {remote_pid}) terminated gracefully following SIGQUIT signal."
                        )
                    else:
                        self.log.debug(
                            f"Remote process {uuid} (PID {remote_pid}) did not terminate after SIGQUIT signal."
                        )

                if not process_dead:
                    self.log.debug(f"Sending SIGKILL to remote PID {remote_pid}")
                    self._send_remote_signal(signal_client, remote_pid, "KILL")
                    process_dead = self.wait_for_process_to_die(uuid, timeout=timeout)

                    if process_dead:
                        self.log.info(
                            f"Remote process {uuid} (PID {remote_pid}) terminated gracefully following SIGKILL signal."
                        )
                    else:
                        self.log.debug(
                            f"Remote process {uuid} (PID {remote_pid}) did not terminate after SIGKILL signal."
                        )

                if not process_dead:
                    self.log.error(
                        f"Remote process {uuid} (PID {remote_pid}) did not terminate after SIGKILL signal."
                    )
                    # Forcibly close channel since graceful termination failed
                    self._kill_process_channel(uuid, channel)
                    exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
                    # Don't clean up resources to aid debugging
                    return exit_code
                else:
                    # Process died successfully - wait for watcher thread to capture exit code
                    exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
                    # Clean up remote metadata file on successful termination
                    self._cleanup_remote_file_paramiko(signal_client, metadata_file)
                    # Clean up local resources
                    self._cleanup_process_resources(uuid)
                    return exit_code

            finally:
                signal_client.close()

        except Exception as e:
            self.log.error(f"Error terminating remote process {uuid}: {e}")
            # Exception during termination - forcibly close channel
            self._kill_process_channel(uuid, channel)
            return None

    def _send_remote_signal(
        self, client: paramiko.SSHClient, pid: int, signal_name: str
    ) -> None:
        """Send signal to remote process."""
        try:
            stdin, stdout, stderr = client.exec_command(
                f"kill -{signal_name} {pid}", timeout=5.0
            )
            stdout.channel.recv_exit_status()
        except Exception as e:
            self.log.debug(f"Failed to send {signal_name} to PID {pid}: {e}")

    def _is_remote_process_alive(self, hostname: str, user: str, pid: int) -> bool:
        """
        Check if remote process exists via /proc filesystem.
        Creates an independent SSH connection for the check to avoid
        interfering with the main process connection.

        Args:
            hostname: Remote hostname
            user: SSH username
            pid: Process ID to check

        Returns:
            True if process exists, False otherwise
        """
        try:
            # Create independent SSH connection for process check
            check_client = self._create_ssh_client(hostname, user, enable_agent=False)
            if check_client is None:
                self.log.debug(f"Could not create client to check PID {pid}")
                return False

            try:
                _, stdout, _ = check_client.exec_command(
                    f"[ -d /proc/{pid} ]", timeout=2.0
                )
                return stdout.channel.recv_exit_status() == 0
            finally:
                check_client.close()
        except Exception as e:
            self.log.debug(f"Exception checking if PID {pid} alive: {e}")
            return False

    def _cleanup_remote_file_paramiko(
        self, client: paramiko.SSHClient, remote_file: str
    ) -> None:
        """Remove remote file after expanding any shell variables."""
        try:
            # Expand shell variables in path
            stdin, stdout, stderr = client.exec_command(
                f"echo {remote_file}", timeout=5.0
            )
            expanded_path = stdout.read().decode("utf-8").strip()

            # Remove the file
            stdin, stdout, stderr = client.exec_command(
                f"rm -f {expanded_path}", timeout=5.0
            )
            stdout.channel.recv_exit_status()
        except Exception:
            pass
