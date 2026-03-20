"""
Provides SSH connection and lifetime management using sh library
to invoke shell commands over SSH.
"""

import getpass
import logging
import os
import signal
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

import sh
from druncschema.process_manager_pb2 import BootRequest

from drunc.process_manager.configuration import PROCESS_SHUTDOWN_ORDERING
from drunc.process_manager.utils import on_parent_exit
from drunc.processes.connection_utils import wait_for
from drunc.processes.process_metadata import ProcessMetadata
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
        manager: "SSHProcessLifetimeManagerShell",
        hostname: str,
        user: str,
        metadata_file: str,
        on_exit: Optional[Callable[[str, Optional[int], Optional[Exception]], None]],
        logger: logging.Logger,
    ):
        """
        Initialise process watcher thread.

        Args:
            uuid: Process UUID to monitor
            process: sh.RunningCommand instance to monitor
            manager: Parent manager instance for metadata updates
            hostname: Remote hostname for metadata retrieval
            user: Remote user for metadata retrieval
            metadata_file: Path to metadata file on remote host
            on_exit: Callback function invoked on process exit
            logger: Logger instance for output
        """
        super().__init__(name=f"ShellWatcher-{uuid}", daemon=True)
        self.uuid = uuid
        self.process = process
        self.manager = manager
        self.hostname = hostname
        self.user = user
        self.metadata_file = metadata_file
        self.on_exit = on_exit
        self.logger = logger
        self.__is_monitoring_remotely = False

    def run(self):
        """
        Monitor process, read metadata asynchronously, and invoke callback on exit.
        """
        try:
            metadata = self.manager.read_process_metadata(
                self.uuid,
                self.metadata_file,
                self.hostname,
                self.user,
            )
            if metadata:
                with self.manager.lock:
                    self.manager.metadata[self.uuid] = metadata
                self.logger.debug(f"Metadata retrieved for process {self.uuid}")

                # Log the terminal commands used to manually SIGKILL this process
                # from outside the process manager which can be useful for debugging
                # unexpected process deaths
                if metadata.pid is not None:
                    self.logger.debug(
                        f"To manually kill remote process '{metadata.name}' (UUID: {self.uuid}), run: "
                        f"ssh {self.user}@{self.hostname} kill -9 {metadata.pid}"
                    )
                self.logger.debug(
                    f"To manually kill the local SSH client for '{metadata.name}' (UUID: {self.uuid}), run: "
                    f"kill -9 {self.process.pid}"
                )
            else:
                # If metadata could not be read, fall back to monitoring SSH client
                self.logger.warning(
                    f"Failed to retrieve metadata for process {self.uuid}. "
                    f"Falling back to SSH client monitoring."
                )
                self._monitor_ssh_client()
                return
        except Exception as e:
            self.logger.warning(
                f"Exception reading metadata for process {self.uuid}: {e}. "
                f"Falling back to SSH client monitoring."
            )
            self._monitor_ssh_client()
            return

        # Monitor the remote process directly
        self._monitor_remote_process(metadata.pid)

    def _monitor_remote_process(self, remote_pid: int) -> None:
        """
        Monitor remote process by polling until PID disappears.

        Uses SSH to run a blocking command that exits when the process dies.
        """
        exception = None
        exit_code = None

        try:
            user_host = f"{self.user}@{self.hostname}"
            arguments = self.manager._build_ssh_arguments(self.hostname, user_host)

            # Remote ssh command that will block until process exits
            remote_cmd = (
                f"while kill -0 {remote_pid} 2>/dev/null; do sleep 0.1; done; exit 0"
            )
            arguments.append(remote_cmd)

            self.__is_monitoring_remotely = True
            # This ssh command will block until the remote process exits
            self.manager.ssh(*arguments)
            self.__is_monitoring_remotely = False
            self.logger.debug(
                f"Remote process {self.uuid} (PID {remote_pid}) has exited"
            )

            self.process.wait()
            exit_code = self.process.exit_code
            self.logger.debug(
                f"SSH client for {self.uuid} exited with code {exit_code}"
            )
        except sh.ErrorReturnCode as e:
            exception = e
            exit_code = e.exit_code
            self.logger.debug(f"Remote process {self.uuid} monitoring error: {e}")

        except Exception as e:
            exception = e
            self.logger.error(f"Remote process {self.uuid} watcher error: {e}")

        # Invoke callback with results
        if self.on_exit:
            try:
                self.on_exit(self.uuid, exit_code, exception)
            except Exception as callback_error:
                self.logger.error(
                    f"Error in process exit callback for {self.uuid}: {callback_error}"
                )

    def _monitor_ssh_client(self) -> None:
        """
        Monitor the SSH client process until it stops, this can be used as a
        fallback if the remote PID of the process is unavailable.
        """
        exception = None
        exit_code = None

        try:
            self.process.wait()
            exit_code = self.process.exit_code
            self.logger.debug(
                f"SSH client for {self.uuid} exited with code {exit_code}"
            )

        except sh.ErrorReturnCode as e:
            exception = e
            exit_code = e.exit_code
            self.logger.debug(f"SSH client for {self.uuid} error: {e}")

        except Exception as e:
            exception = e
            self.logger.error(f"SSH client for {self.uuid} watcher error: {e}")

        if self.on_exit:
            try:
                self.on_exit(self.uuid, exit_code, exception)
            except Exception as callback_error:
                self.logger.error(
                    f"Error in process exit callback for {self.uuid}: {callback_error}"
                )

    def is_monitoring_remotely(self) -> bool:
        """
        Check if the watcher is monitoring the remote process directly.

        Returns:
            True if monitoring remote process, False if monitoring SSH client
        """
        return self.__is_monitoring_remotely


class SSHProcessLifetimeManagerShell(ProcessLifetimeManager):
    """
    Manages process lifecycle using sh library for SSH connections.
    Uses the sh library's SSH command wrapper to start
    and manage remote processes.
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
        self.watchers: Dict[str, threading.Thread] = {}

        # Thread-safe lock for process store modifications
        self.lock = threading.Lock()

        # metadata for each process
        self.metadata: Dict[str, ProcessMetadata] = {}

    @staticmethod
    def get_metadata_file_path(uuid: str) -> str:
        """
        Generate metadata file path for a given process UUID.

        Uses XDG_RUNTIME_DIR if available, otherwise falls back to /tmp.
        The path will be expanded on the remote host when the command executes.

        Args:
            uuid: Process UUID to generate metadata file path for

        Returns:
            Shell-expandable path string containing environment variable reference
        """
        return f"${{XDG_RUNTIME_DIR:-/tmp}}/drunc/metadata_{uuid}.json"

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
        self._execute_bootrequest_via_ssh(
            uuid=uuid,
            boot_request=boot_request,
            hostname=hostname,
            user=user if user else getpass.getuser(),
            command=cmd,
            log_file=log_file,
            env_vars=env_vars,
        )

    def _start_process_watcher(
        self,
        uuid: str,
        process: sh.RunningCommand,
        hostname: str,
        user: str,
        metadata_file: str,
    ) -> None:
        """
        Start a monitoring thread for a process.

        This thread waits for the process to complete, captures the exit code,
        retrieves metadata asynchronously, and invokes the exit callback if provided.

        Args:
            uuid: Process UUID
            process: sh.RunningCommand to monitor
            hostname: Remote hostname for metadata retrieval
            user: Remote user for metadata retrieval
            metadata_file: Path to metadata file on remote host
        """
        watcher = ProcessWatcherThread(
            uuid=uuid,
            process=process,
            manager=self,
            hostname=hostname,
            user=user,
            metadata_file=metadata_file,
            on_exit=self.on_process_exit,
            logger=self.log,
        )
        watcher.start()
        with self.lock:
            self.watchers[uuid] = watcher

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

        process = self.process_store[uuid]["process"]
        metadata: ProcessMetadata = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.debug(
                f"No metadata or PID found for {uuid}, relying on SSH client process status"
            )
            return process.is_alive()

        remote_process_alive = self._is_remote_process_alive(
            self.process_store[uuid]["hostname"],
            self.process_store[uuid]["user"],
            metadata.pid,
            uuid,
        )
        return process.is_alive() and remote_process_alive

    def pop_early_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code if process exited early without being killed.

        This method checks if a process has terminated unexpectedly (without
        kill_process being called). If an exit code is found, the process
        resources are cleaned up automatically.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated early, None if still running or not found
        """
        if uuid not in self.process_store:
            self.log.debug(f"Process {uuid} not found in store for exit code retrieval")
            return None

        process = self.process_store[uuid]["process"]
        if process.is_alive():
            return None

        try:
            early_exit_code = process.exit_code
        except Exception as e:
            self.log.debug(f"Exception thrown getting exit code for {uuid}: {e}")
            return None

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

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stdout content as string, None if not found
        """
        if uuid not in self.process_store:
            return None

        try:
            process = self.process_store[uuid]["process"]
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
            process = self.process_store[uuid]["process"]
            if hasattr(process, "stderr"):
                stderr_data = process.stderr
                if stderr_data:
                    return str(stderr_data)
        except Exception as e:
            self.log.debug(f"Error getting stderr for {uuid}: {e}")

        return None

    def kill_processes_by_role(
        self,
        role: str,
        candidate_uuids: List[str],
        process_timeouts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Optional[int]]:
        """
        Kill all processes with the specified role from candidate UUID list.

        Filters candidate UUIDs by matching metadata roles, then terminates
        matching processes asynchronously using a thread pool.

        Args:
            role: Process role to match
            candidate_uuids: List of process UUIDs to filter by role
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds. Uses default timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping terminated process UUIDs to their exit codes
        """
        self.log.debug(f"process_timeouts: {process_timeouts}")
        if process_timeouts is None:
            process_timeouts = {}

        # Filter candidate UUIDs by role using process metadata
        uuids_to_kill = []
        with self.lock:
            for uuid in candidate_uuids:
                metadata = self.metadata.get(uuid, None)
                if metadata and metadata.role == role:
                    uuids_to_kill.append(uuid)
                    if uuid not in process_timeouts:
                        process_timeouts[uuid] = (
                            self.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS
                        )

        if not uuids_to_kill:
            self.log.debug(f"No processes found with role '{role}' in candidate list")
            return {}

        self.log.info(
            f"Killing {len(uuids_to_kill)} process(es) with role '{role}' "
            f"from {len(candidate_uuids)} candidates"
        )

        exit_codes: Dict[str, Optional[int]] = {}

        # Terminate processes asynchronously using thread pool
        with ThreadPoolExecutor(max_workers=len(uuids_to_kill)) as executor:
            # Submit kill tasks for all matching processes
            future_to_uuid = {
                executor.submit(self.kill_process, uuid, process_timeouts[uuid]): uuid
                for uuid in uuids_to_kill
            }

            # Collect results as they complete
            for future in as_completed(future_to_uuid):
                uuid = future_to_uuid[future]
                try:
                    exit_code = future.result()
                    exit_codes[uuid] = exit_code
                except Exception as e:
                    self.log.error(
                        f"Error during termination of process {uuid} with role '{role}': {e}"
                    )
                    exit_codes[uuid] = None

        return exit_codes

    def kill_processes(
        self, uuids: List[str], process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[int]]:
        """
        Kill multiple processes by their UUIDs in role-based shutdown order.

        Executes a staged shutdown by role. Processes within each role are terminated
        asynchronously. After all roles complete, any remaining processes
        are killed asynchronously as a fallback.

        Args:
            uuids: List of process UUIDs to terminate
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds. Uses default timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping process UUIDs to their exit codes
        """
        if not uuids:
            self.log.debug("No processes to kill")
            return {}

        if process_timeouts is None:
            process_timeouts = {}

        # Ensure all UUIDs have timeout values
        for uuid in uuids:
            if uuid not in process_timeouts:
                process_timeouts[uuid] = self.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS

        all_exit_codes: Dict[str, Optional[int]] = {}
        killed_uuids = set()

        # Execute role-based shutdown in stages
        for role in PROCESS_SHUTDOWN_ORDERING:
            self.log.info(
                f"--- Shutdown stage: Terminating role '{role}' from provided UUIDs ---"
            )
            role_exit_codes = self.kill_processes_by_role(
                role, uuids, process_timeouts=process_timeouts
            )
            all_exit_codes.update(role_exit_codes)
            killed_uuids.update(role_exit_codes.keys())

            if role_exit_codes:
                self.log.info(f"--- Shutdown stage: Role '{role}' complete ---")

        # Identify processes not killed during role-based shutdown
        remaining_uuids = [uuid for uuid in uuids if uuid not in killed_uuids]

        if remaining_uuids:
            self.log.info(
                f"Fallback: Killing {len(remaining_uuids)} process(es) without "
                f"role metadata asynchronously"
            )

            # Kill remaining processes asynchronously without role ordering
            fallback_exit_codes: Dict[str, Optional[int]] = {}

            with ThreadPoolExecutor(max_workers=len(remaining_uuids)) as executor:
                # Submit kill tasks for all remaining processes
                future_to_uuid = {
                    executor.submit(
                        self.kill_process, uuid, process_timeouts[uuid]
                    ): uuid
                    for uuid in remaining_uuids
                }

                # Collect results as they complete
                for future in as_completed(future_to_uuid):
                    uuid = future_to_uuid[future]
                    try:
                        exit_code = future.result()
                        fallback_exit_codes[uuid] = exit_code
                    except Exception as e:
                        self.log.error(
                            f"Error during fallback termination of process {uuid}: {e}"
                        )
                        fallback_exit_codes[uuid] = None

            all_exit_codes.update(fallback_exit_codes)

        return all_exit_codes

    def kill_all_processes(
        self, process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[int]]:
        """
        Kill all active processes in role-based shutdown order.

        Retrieves all active process UUIDs and delegates to kill_processes()
        for role-based termination. Waits for all monitoring threads to
        complete after termination.

        Args:
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds. Uses default timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping all process UUIDs to their exit codes
        """
        # Retrieve all active process UUIDs
        with self.lock:
            active_uuids = list(self.process_store.keys())

        if not active_uuids:
            self.log.info("No processes to terminate")
            return {}

        self.log.info(f"Terminating all {len(active_uuids)} active process(es)")

        # Delegate to kill_processes for role-based shutdown ordering
        all_exit_codes = self.kill_processes(active_uuids, process_timeouts)

        # Wait for all watcher threads to complete
        with self.lock:
            watchers_to_join = list(self.watchers.values())

        for watcher in watchers_to_join:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        with self.lock:
            self.watchers.clear()

        return all_exit_codes

    def _build_ssh_arguments(self, hostname: str, user_host: str) -> List[str]:
        """
        Build standard SSH arguments with host key checking policy.

        Args:
            hostname: Target hostname for policy determination
            user_host: User@hostname string for SSH connection

        Returns:
            List of SSH command arguments
        """
        disable_host_key_check = self.disable_host_key_check or (
            self.disable_localhost_host_key_check
            and hostname in ("localhost", "127.0.0.1", "::1")
        )

        arguments = [user_host, "-tt", "-o", "StrictHostKeyChecking=no"]

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

        return arguments

    def read_log_file(
        self, hostname: str, user: str, log_file: str, num_lines: int = 100
    ) -> List[str]:
        """Read remote log file via SSH."""
        # Create temporary file for output
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        try:
            # Build user@host string
            user_host = f"{user}@{hostname}"

            # Build SSH arguments using helper method
            arguments = self._build_ssh_arguments(hostname, user_host)
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
        """Validate SSH connection to the specified host."""
        try:
            # Build user@host string
            user_host = f"{user}@{host}"

            # Build remote command
            remote_cmd = f'echo "{user} established SSH successfully";'

            # Build SSH arguments using helper method
            arguments = self._build_ssh_arguments(host, user_host)
            arguments.append(remote_cmd)

            # Execute SSH command and wait for completion
            self.ssh(*arguments)

            self.log.debug(f"SSH validation successful for {user}@{host}")

        except Exception as e:
            self.log.error(f"SSH validation failed for {user}@{host}: {e}")
            raise RuntimeError(f"SSH connection validation failed: {e}")

    def read_process_metadata(
        self,
        uuid: str,
        metadata_file: str,
        hostname: str,
        user: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_READING_METADATA,
    ) -> Optional[ProcessMetadata]:
        """
        Read process metadata from remote JSON file with a single SSH call.

        Uses a remote-side wait loop to avoid multiple SSH round-trips. The remote
        command polls for file existence and reads it once available, all within
        a single SSH session.

        Args:
            uuid: Process UUID for identification in logs
            metadata_file: Absolute path to metadata file on remote host
            hostname: Target hostname for SSH connection
            user: SSH username for authentication
            timeout: Maximum time in seconds to wait for metadata file availability

        Returns:
            ProcessMetadata instance if file exists and is valid, None otherwise
        """
        try:
            # Build user@host string for SSH connection
            user_host = f"{user}@{hostname}"

            # Build SSH arguments including connection parameters
            arguments = self._build_ssh_arguments(hostname, user_host)

            # Remote command: wait for file to exist, then read it
            # Polls every 50ms, times out after specified duration
            remote_command = (
                f"timeout {timeout} bash -c '"
                f"while [ ! -f {metadata_file} ]; do sleep 0.05; done; "
                f"cat {metadata_file}"
                f"'"
            )
            arguments.append(remote_command)

            # Execute SSH command to wait for and read file (single round-trip)
            result = self.ssh(*arguments)
            json_content = str(result).strip()

            # Parse JSON content and instantiate metadata object
            metadata = ProcessMetadata.from_json(json_content)

            return metadata

        except Exception as e:
            self.log.debug(f"Failed to read metadata for {uuid}: {e}")
            return None

    def _ssh_client_stderr_logger(self, chunk):
        """Filter the logging of an SSH client stderr to the
        appropriate log level
        """
        # convery bytes to string if necessary
        if isinstance(chunk, bytes):
            msg = chunk.decode("utf-8", errors="replace")
        else:
            msg = chunk

        if "Connection to" in msg and "closed." in msg:
            self.log.debug(msg.strip())
        else:
            self.log.error(msg.strip())

    def _execute_bootrequest_via_ssh(
        self,
        uuid: str,
        boot_request: BootRequest,
        hostname: str,
        user: str,
        command: str,
        log_file: str,
        env_vars: Dict[str, str] = None,
    ) -> None:
        """Execute SSH command using sh library."""
        try:
            platform = os.uname().sysname.lower()
            is_macos = "darwin" in platform
            user_host = f"{user}@{hostname}"

            # Build remote command with metadata file writing
            remote_cmd = (
                'echo "SSHPM: Starting process $$ on host $HOSTNAME as user $USER";'
            )

            if env_vars:
                cmd_env = ";".join([f'export {n}="{v}"' for n, v in env_vars.items()])
                remote_cmd += cmd_env + ";"

            if hasattr(boot_request.process_description, "process_execution_directory"):
                remote_cmd += f"cd {boot_request.process_description.process_execution_directory} ; "

            metadata_file = SSHProcessLifetimeManagerShell.get_metadata_file_path(uuid)
            tree_id = boot_request.process_description.metadata.tree_id
            name = boot_request.process_description.metadata.name
            role = ProcessMetadata.compute_role_from_tree_id(tree_id)

            remote_metadata_json = (
                f"{{\"pid\": '$PID', "
                f'"hostname": "{hostname}", '
                f'"user": "{user}", '
                f"\"started_at\": '$(date +%s)', "
                f'"tree_id": "{tree_id}", '
                f'"role": "{role}", '
                f'"name": "{name}"}}'
            )

            remote_cmd += (
                f"mkdir -p ${{XDG_RUNTIME_DIR:-/tmp}}/drunc ; "
                f"{command} &> {log_file} & PID=$! ; "
                f"echo '{remote_metadata_json}' > {metadata_file} ; "
                f"wait $PID"
            )

            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.append(remote_cmd)

            process = self.ssh(
                *arguments,
                _out=self.log.debug,
                _err=self._ssh_client_stderr_logger,
                _bg=True,
                _bg_exc=False,
                _new_session=True,
                _preexec_fn=on_parent_exit(signal.SIGTERM) if not is_macos else None,
            )
            # Store process info
            with self.lock:
                self.process_store[uuid] = {
                    "process": process,
                    "hostname": hostname,
                    "user": user,
                }
                # Metadata will be populated asynchronously by watcher thread
                self.metadata[uuid] = None

            self._start_process_watcher(uuid, process, hostname, user, metadata_file)
            self.log.debug(f"SSH command started for {uuid}")
        except Exception as e:
            with self.lock:
                if uuid in self.process_store:
                    del self.process_store[uuid]
            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def _kill_client_process(self, process_info: Dict) -> None:
        """
        Kill a local SSH client process. The remote process will typically
        recieve a SIGHUP when the SSH client terminates.
        """
        try:
            process_info["process"].signal_group(signal.SIGKILL)
        except Exception as e:
            self.log.debug(
                f"Exception was raised when terminating SSH client process: {e}"
            )

    def _wait_for_process_exit_code(self, uuid: str, timeout: float) -> Optional[int]:
        """
        Wait for specified timeout to see if a process exit code is available.

        Args:
            uuid: Process UUID to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            Exit code if process has terminated, None if still running or not found
        """
        # Get process reference under lock to avoid race condition
        with self.lock:
            if uuid not in self.process_store:
                return None
            process = self.process_store[uuid]["process"]

        def check_exit_status():
            return not process.is_alive()

        # Wait for process to exit
        got_exit = wait_for(check_exit_status, expected_value=True, timeout=timeout)

        if got_exit:
            try:
                return process.exit_code
            except Exception as e:
                self.log.debug(f"Exception getting exit code for {uuid}: {e}")
                return None
        else:
            self.log.debug(f"Timeout waiting for exit code of process {uuid}")
            return None

    def kill_process(
        self,
        uuid: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> int | None:
        """
        Kill a remote process and clean up all associated resources.

        Sends termination signals to the remote process, waits for it to die,
        cleans up remote metadata files, terminates the SSH client, and removes
        the process from internal tracking. Safe to call multiple times.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination in seconds

        Returns:
            Exit code of the terminated process, or None if not found or still running
        """
        if uuid not in self.process_store:
            return None

        process_info = self.process_store[uuid]

        hostname = process_info["hostname"]
        user = process_info["user"]
        metadata_file = SSHProcessLifetimeManagerShell.get_metadata_file_path(uuid)

        # Read metadata to get remote PID
        metadata = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.warning(
                f"No remote PID for {uuid}, terminating SSH client. Cannot guarantee remote process termination."
            )
            self._kill_client_process(process_info)
            exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
            self._cleanup_process_resources(uuid)
            return exit_code

        remote_pid = metadata.pid
        process_dead = False

        try:
            if not self.is_process_alive(uuid):
                self.log.info(
                    f"Skipping killing remote process {uuid} (PID {remote_pid}). It is already dead."
                )
                process_dead = True

            if not process_dead:
                self.log.debug(f"Sending SIGQUIT to remote PID {remote_pid}")
                self._send_remote_signal(hostname, user, remote_pid, "QUIT")
                process_dead = self.wait_for_process_to_die(
                    uuid, timeout=timeout, logger=self.log
                )
                if process_dead:
                    self.log.info(
                        f"Remote process {uuid} (PID {remote_pid}) terminated gracefully following SIGQUIT signal."
                    )
                else:
                    self.log.info(
                        f"Remote process {uuid} (PID {remote_pid}) did not terminate after SIGQUIT signal."
                    )

            if not process_dead:
                self.log.debug(f"Sending SIGKILL to remote PID {remote_pid}")
                self._send_remote_signal(hostname, user, remote_pid, "KILL")
                process_dead = self.wait_for_process_to_die(
                    uuid, timeout=timeout, logger=self.log
                )

                if process_dead:
                    self.log.info(
                        f"Remote process {uuid} (PID {remote_pid}) terminated forcibly following SIGKILL signal."
                    )
                else:
                    self.log.debug(
                        f"Remote process {uuid} (PID {remote_pid}) did not terminate after SIGKILL signal."
                    )

            if not process_dead:
                self.log.error(
                    f"Remote process {uuid} (PID {remote_pid}) still did not terminate after SIGKILL signal."
                )
            else:
                exit_code = self._wait_for_process_exit_code(uuid, timeout=timeout)
                self._cleanup_remote_file(hostname, user, metadata_file)
                self._cleanup_process_resources(uuid)
                return exit_code

        except Exception as e:
            self.log.error(f"Error terminating remote process {uuid}: {e}")
            return None

        return None

    def _cleanup_process_resources(self, uuid: str) -> None:
        """Remove all resources associated with a process UUID."""
        with self.lock:
            if uuid in self.process_store:
                del self.process_store[uuid]
            if uuid in self.metadata:
                del self.metadata[uuid]
            if uuid in self.watchers:
                del self.watchers[uuid]

    def _send_remote_signal(
        self, hostname: str, user: str, pid: int, signal_name: str
    ) -> None:
        """Send signal to remote process via SSH."""
        try:
            user_host = f"{user}@{hostname}"
            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.extend(["kill", f"-{signal_name}", str(pid)])
            self.ssh(*arguments)
        except Exception as e:
            self.log.debug(f"Failed to send {signal_name} to PID {pid}: {e}")

    def _is_remote_process_alive(
        self, hostname: str, user: str, pid: int, uuid: str
    ) -> bool:
        """
        Check if remote process is running.
        - If the watcher thread is connected remotely and alive, assume process is alive.
        - If not, we use the slower method of checking via SSH.

        Args:
            hostname: Remote hostname
            user: Remote user
            pid: Process ID to check
            uuid: Process UUID for watcher thread lookup

        Returns:
            True if process is alive, False otherwise
        """
        with self.lock:
            watcher = self.watchers.get(uuid)

        if watcher and watcher.is_alive() and watcher.is_monitoring_remotely():
            # Watcher is blocking on remote process, so remote process must be alive
            return True

        # Verify remote process via SSH (requires another connection)
        try:
            user_host = f"{user}@{hostname}"
            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.extend([f"[ -d /proc/{pid} ]"])
            self.ssh(*arguments)
            return True
        except Exception:
            return False

    def _cleanup_remote_file(self, hostname: str, user: str, remote_file: str) -> None:
        """Remove remote file via SSH."""
        try:
            user_host = f"{user}@{hostname}"
            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.extend(["rm", "-f", remote_file])
            self.ssh(*arguments)
        except Exception:
            pass
