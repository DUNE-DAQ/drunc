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
from drunc.processes.exit_status import ExitStatus, ExitStatusSource
from drunc.processes.process_metadata import ProcessMetadata
from drunc.processes.ssh_process_lifetime_manager import (
    ProcessLifetimeManager,
    RemotePidResult,
)
from drunc.processes.ssh_shell_process import RunningSSHProcess
from drunc.utils.utils import get_logger


class ProcessWatcherThread(threading.Thread):
    """
    Thread that monitors a background SSH process and invokes callback on exit.
    """

    def __init__(
        self,
        uuid: str,
        running_process: RunningSSHProcess,
        manager: "SSHProcessLifetimeManagerShell",
        hostname: str,
        user: str,
        metadata_file: str,
        on_exit: Optional[
            Callable[[str, Optional[ExitStatus], Optional[Exception]], None]
        ],
        logger: logging.Logger,
    ):
        """
        Initialise process watcher thread.

        Args:
            uuid: Process UUID to monitor
            running_process: Runtime model for the process being monitored
            manager: Parent manager instance for metadata updates
            hostname: Remote hostname for metadata retrieval
            user: Remote user for metadata retrieval
            metadata_file: Path to metadata file on remote host
            on_exit: Callback function invoked on process exit
            logger: Logger instance for output
        """
        super().__init__(name=f"ShellWatcher-{uuid}", daemon=True)
        self.uuid = uuid
        self.running_process = running_process
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
                    f"kill -9 {self.running_process.process.pid}"
                )
            else:
                self.logger.warning(
                    f"Failed to retrieve metadata for process {self.uuid}. "
                    f"Remote process monitoring will not be started."
                )
                return
        except Exception as e:
            self.logger.warning(
                f"Exception reading metadata for process {self.uuid}: {e}. "
                f"Remote process monitoring will not be started."
            )
            return

        if metadata.pid is None:
            self.logger.warning(
                f"Metadata for process {self.uuid} did not contain a PID. "
                f"Remote process monitoring will not be started."
            )
            return

        # Monitor the remote process directly
        self.manager._register_remote_process_watcher(self.uuid, self)
        self._monitor_remote_process(metadata.pid)

    def _monitor_remote_process(self, remote_pid: int) -> None:
        """
        Monitor remote process until the remote PID disappears.

        Uses SSH to run a blocking command that exits when the process dies.
        """
        assert self.running_process is not None, (
            "running_process must be set before monitoring"
        )
        exception = None
        raw_exit_code = None

        try:
            user_host = f"{self.user}@{self.hostname}"

            # Superuser accounts have persistent SSH connections that cause watcher
            # threads to not close when monitored processes exit, so we do not allocate
            # TTYs for monitoring commands to avoid this issue.
            arguments = self.manager._build_ssh_arguments(
                self.hostname, user_host, use_tty=False
            )

            # Remote ssh command that will block until process exits
            # Output the remote monitoring process PID, then run the monitoring loop
            remote_cmd = f"echo $$ && while kill -0 {remote_pid} 2>/dev/null; do sleep 0.1; done; exit 0"
            arguments.append(remote_cmd)

            self.__is_monitoring_remotely = True
            monitoring_process = self.manager.ssh(*arguments, _bg=True, _bg_exc=False)
            assert isinstance(monitoring_process, sh.RunningCommand), (
                "Expected remote monitoring process to be a RunningCommand instance"
            )

            self.running_process.remote_monitoring_pid = getattr(
                monitoring_process, "pid", None
            )

            try:
                monitoring_process.wait()
            except sh.ErrorReturnCode as remote_monitor_error:
                exception = remote_monitor_error

            self.__is_monitoring_remotely = False
            self.logger.debug(
                f"Remote process {self.uuid} (PID {remote_pid}) has exited"
            )

            raw_exit_code = self.manager.wait_for_process_exit_code(
                self.uuid, timeout=30.0
            )
            self.logger.debug(
                f"SSH client for {self.uuid} exited with code {raw_exit_code}"
            )
        except sh.ErrorReturnCode as e:
            exception = e
            raw_exit_code = e.exit_code
            self.logger.debug(f"Remote process {self.uuid} monitoring error: {e}")

        except Exception as e:
            exception = e
            self.logger.error(f"Remote process {self.uuid} watcher error: {e}")

        self.manager._emit_exit_callback_once(
            self.uuid,
            self.running_process,
            ExitStatusSource.REMOTE_MONITORING,
            raw_exit_code,
            exception,
        )

    def is_monitoring_remotely(self) -> bool:
        """
        Check if the watcher is monitoring the remote process directly.

        Returns:
            True if monitoring remote process, False if monitoring SSH client
        """
        return self.__is_monitoring_remotely


class SSHClientWatcherThread(threading.Thread):
    """Thread that monitors the local SSH client and classifies its exit."""

    def __init__(
        self,
        uuid: str,
        running_process: RunningSSHProcess,
        manager: "SSHProcessLifetimeManagerShell",
        hostname: str,
        user: str,
        metadata_file: str,
        logger: logging.Logger,
    ):
        super().__init__(name=f"ShellClientWatcher-{uuid}", daemon=True)
        self.uuid = uuid
        self.running_process = running_process
        self.manager = manager
        self.hostname = hostname
        self.user = user
        self.metadata_file = metadata_file
        self.logger = logger

    def run(self) -> None:
        self._monitor_ssh_client()

    def _monitor_ssh_client(self) -> None:
        """Monitor the SSH client process until it stops."""
        exception = None
        raw_exit_code = None

        try:
            self.running_process.process.wait()
            client_exit_code = self.running_process.process.exit_code
            self.logger.debug(
                f"SSH client for {self.uuid} exited with code {client_exit_code}"
            )
        except sh.ErrorReturnCode as e:
            self.logger.debug(f"SSH client for {self.uuid} error: {e}")
            exception = e
            client_exit_code = e.exit_code
        except Exception as e:
            self.logger.error(f"SSH client for {self.uuid} watcher error: {e}")
            exception = e
            client_exit_code = None

        remote_exit_code = self.manager.wait_for_process_exit_code(
            self.uuid, timeout=30.0
        )
        if remote_exit_code is not None:
            raw_exit_code = remote_exit_code
        else:
            raw_exit_code = client_exit_code

        default_source = ExitStatusSource.CLIENT_MONITORING
        remote_pid = self.running_process.remote_pid

        # External SIGQUIT hit the SSH client unexpectedly; trigger remote-PID failsafe cleanup.
        if (
            self.running_process.pending_exit_status_source is None
            and raw_exit_code == 255
        ):
            self.manager._handle_external_client_sigquit(
                self.uuid,
                self.hostname,
                self.user,
                remote_pid,
                self.metadata_file,
            )
        # Client exited after the remote PID is dead, so classify this as remote-driven termination.
        elif (
            self.running_process.pending_exit_status_source is None
            and remote_pid is not None
            and not self.manager._is_remote_pid_alive(
                self.hostname,
                self.user,
                remote_pid,
            )
        ):
            default_source = ExitStatusSource.REMOTE_MONITORING

        self.manager._emit_exit_callback_once(
            self.uuid,
            self.running_process,
            default_source,
            raw_exit_code,
            exception,
        )


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
            Callable[[str, Optional[ExitStatus], Optional[Exception]], None]
        ] = None,
    ):
        """
        Initialise SSH process lifetime manager using sh library.

        Args:
            disable_host_key_check: Disable SSH host key verification for all hosts
            disable_localhost_host_key_check: Disable SSH host key verification for localhost
            logger: Logger instance for real-time output logging
            on_process_exit: Optional callback function(uuid, exit_status, exception) invoked when process exits
        """
        self.disable_host_key_check = disable_host_key_check
        self.disable_localhost_host_key_check = disable_localhost_host_key_check
        # self.log = logger if logger else get_logger(__name__)
        self.log = get_logger("PM_LMS_TEST", rich_handler=True)
        self._on_process_exit = on_process_exit

        # Create SSH command wrapper
        self.ssh = sh.Command("/usr/bin/ssh")

        # Process tracking (one per UUID)
        self.process_store: Dict[str, RunningSSHProcess] = {}

        # Thread tracking for monitoring
        self.client_watchers: Dict[str, SSHClientWatcherThread] = {}
        self.remote_process_watchers: Dict[str, ProcessWatcherThread] = {}

        # Thread-safe lock for process store modifications
        self.lock = threading.Lock()

        # metadata for each process
        self.metadata: Dict[str, Optional[ProcessMetadata]] = {}

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

    def get_remote_pid(self, uuid: str) -> RemotePidResult:
        """
        Return the remote PID for the process identified by *uuid*.

        Args:
            uuid: Process UUID to query.

        Returns:
            RemotePidResult with ``pid`` set on success, or ``reason``
            set to ``"no metadata"`` when the metadata file has not yet
            been written or could not be read.
        """
        with self.lock:
            running_process = self.process_store.get(uuid)
            if running_process is not None and running_process.remote_pid is not None:
                return RemotePidResult(pid=running_process.remote_pid)
            metadata = self.metadata.get(uuid)
        if metadata is None or metadata.pid is None:
            return RemotePidResult(reason="no metadata")
        return RemotePidResult(pid=metadata.pid)

    def get_runtime_pids(self, uuid: str) -> Dict[str, Optional[int]]:
        """Return best-effort runtime PID snapshot for a managed process."""
        with self.lock:
            running_process = self.process_store.get(uuid)
            if running_process is None:
                return {
                    "ssh_client_pid": None,
                    "remote_pid": None,
                    "remote_monitoring_pid": None,
                    "client_monitoring_pid": None,
                }

            return {
                "ssh_client_pid": running_process.ssh_client_pid,
                "remote_pid": running_process.remote_pid,
                "remote_monitoring_pid": running_process.remote_monitoring_pid,
                "client_monitoring_pid": running_process.client_monitoring_pid,
            }

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
        # self.log.critical(
        #     f"Starting process {uuid} on {hostname} as {user} with log file {log_file}"
        # )

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
                if arg.endswith("daq_app_rte.sh"):
                    cmd += f" {os.getenv('DBT_AREA_ROOT')}/install/daq_app_rte.sh"
                else:
                    cmd += f" {arg}"
            cmd += ";"

        # Remove trailing semicolon if present
        if cmd.endswith(";"):
            cmd = cmd[:-1]

        # self.log.critical(f"Built command for {uuid}: {cmd}: {boot_request}")

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
        running_process: RunningSSHProcess,
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
            running_process: Runtime model for process state and handles
            hostname: Remote hostname for metadata retrieval
            user: Remote user for metadata retrieval
            metadata_file: Path to metadata file on remote host
        """
        client_watcher = SSHClientWatcherThread(
            uuid=uuid,
            running_process=running_process,
            manager=self,
            hostname=hostname,
            user=user,
            metadata_file=metadata_file,
            logger=self.log,
        )
        client_watcher.start()
        with self.lock:
            self.client_watchers[uuid] = client_watcher

        watcher = ProcessWatcherThread(
            uuid=uuid,
            running_process=running_process,
            manager=self,
            hostname=hostname,
            user=user,
            metadata_file=metadata_file,
            on_exit=self._on_process_exit,
            logger=self.log,
        )
        watcher.start()

    def _register_remote_process_watcher(
        self,
        uuid: str,
        watcher: ProcessWatcherThread,
    ) -> None:
        """Track the remote-process watcher once metadata is available."""
        with self.lock:
            self.remote_process_watchers[uuid] = watcher

    def _emit_exit_callback_once(
        self,
        uuid: str,
        running_process: RunningSSHProcess,
        default_source: ExitStatusSource,
        raw_exit_code: Optional[int],
        exception: Optional[Exception],
    ) -> None:
        """Publish the first exit observation across concurrent watcher threads."""
        with self.lock:
            exit_status, should_emit = running_process.finalise_exit_once(
                default_source,
                raw_exit_code,
            )

        if not should_emit or self._on_process_exit is None:
            return

        try:
            self._on_process_exit(uuid, exit_status, exception)
        except Exception as callback_error:
            self.log.error(
                f"Error in process exit callback for {uuid}: {callback_error}"
            )

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

        running_process = self.process_store[uuid]
        process = running_process.process
        metadata: Optional[ProcessMetadata] = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.debug(
                f"No metadata or PID found for {uuid}, relying on SSH client process status"
            )
            return process.is_alive()

        remote_process_alive = self._is_remote_process_alive(
            running_process.hostname,
            running_process.user,
            metadata.pid,
            uuid,
        )
        return process.is_alive() and remote_process_alive

    def pop_early_exit_status(self, uuid: str) -> Optional[ExitStatus]:
        """
        Get process exit code if process exited early without being killed.

        This method checks if a process has terminated unexpectedly (without
        kill_process being called). If an exit code is found, the process
        resources are cleaned up automatically.

        Args:
            uuid: Process UUID

        Returns:
            ExitStatus if process has terminated early, None if still running or not found
        """
        if uuid not in self.process_store:
            self.log.debug(f"Process {uuid} not found in store for exit code retrieval")
            return None

        process = self.process_store[uuid].process
        if process.is_alive():
            return None

        try:
            process.wait()
            early_exit_code = process.exit_code
        except sh.ErrorReturnCode as e:
            early_exit_code = e.exit_code
        except Exception as e:
            self.log.debug(f"Exception thrown getting exit code for {uuid}: {e}")
            return None

        if early_exit_code is not None:
            exit_status = ExitStatus(
                ExitStatusSource.CLIENT_MONITORING,
                early_exit_code,
            )
            self.log.warning(
                f"Process {uuid} exited early without being killed. Exit status {exit_status!r}"
            )
            self.log.debug(
                f"Cleaning up resources for process {uuid} with exit status {exit_status!r}"
            )
            self._cleanup_process_resources(uuid)
            return exit_status

        return None

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
            process = self.process_store[uuid].process
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
            process = self.process_store[uuid].process
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
    ) -> Dict[str, Optional[ExitStatus]]:
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
            Dictionary mapping terminated process UUIDs to their exit statuses
        """
        self.log.debug(f"process_timeouts: {process_timeouts}")
        if process_timeouts is None:
            process_timeouts = {}

        # Filter candidate UUIDs by role using process metadata
        uuids_to_kill = []
        with self.lock:
            for uuid in candidate_uuids:
                metadata = self.metadata.get(uuid)
                if metadata is None or metadata.role != role:
                    continue

                uuids_to_kill.append(uuid)
                process_timeouts.setdefault(
                    uuid,
                    self.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
                )

        if not uuids_to_kill:
            return {}

        self.log.info(f"Killing {len(uuids_to_kill)} process(es) with role '{role}'")

        exit_statuses: Dict[str, Optional[ExitStatus]] = {}

        with ThreadPoolExecutor(max_workers=len(uuids_to_kill)) as executor:
            future_to_uuid = {
                executor.submit(self.kill_process, uuid, process_timeouts[uuid]): uuid
                for uuid in uuids_to_kill
            }

            for future in as_completed(future_to_uuid):
                uuid = future_to_uuid[future]
                try:
                    exit_statuses[uuid] = future.result()
                except Exception as e:
                    self.log.error(
                        f"Error during termination of process {uuid} with role '{role}': {e}"
                    )
                    exit_statuses[uuid] = None

        return exit_statuses

    def kill_processes(
        self, uuids: List[str], process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[ExitStatus]]:
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
            Dictionary mapping process UUIDs to their exit statuses
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

        all_exit_statuses: Dict[str, Optional[ExitStatus]] = {}
        killed_uuids = set()

        # Execute role-based shutdown in stages
        for role in PROCESS_SHUTDOWN_ORDERING:
            with self.lock:
                uuids_in_role = [
                    uuid
                    for uuid in uuids
                    if (metadata := self.metadata.get(uuid)) is not None
                    and metadata.role == role
                ]

            # Match k8s PM behavior: if role is absent, do not log/start/end a stage.
            if not uuids_in_role:
                continue

            self.log.info(
                f"--- Termination of role '{role}' ({len(uuids_in_role)} process(es)) ---"
            )
            role_exit_statuses = self.kill_processes_by_role(
                role, uuids, process_timeouts=process_timeouts
            )
            all_exit_statuses.update(role_exit_statuses)
            killed_uuids.update(role_exit_statuses.keys())

            if role_exit_statuses:
                self.log.info(f"--- Shutdown stage: Role '{role}' complete ---")

        # Identify processes not killed during role-based shutdown
        remaining_uuids = [uuid for uuid in uuids if uuid not in killed_uuids]

        if remaining_uuids:
            self.log.info(
                f"Fallback: Killing {len(remaining_uuids)} process(es) without "
                f"role metadata asynchronously"
            )

            # Kill remaining processes asynchronously without role ordering
            fallback_exit_statuses: Dict[str, Optional[ExitStatus]] = {}

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
                        fallback_exit_statuses[uuid] = future.result()
                    except Exception as e:
                        self.log.error(
                            f"Error during fallback termination of process {uuid}: {e}"
                        )
                        fallback_exit_statuses[uuid] = None

            all_exit_statuses.update(fallback_exit_statuses)

        return all_exit_statuses

    def kill_all_processes(
        self, process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[ExitStatus]]:
        """
        Kill all active processes in role-based shutdown order.

        Retrieves all active process UUIDs and delegates to kill_processes()
        for role-based termination. Waits for all monitoring threads to
        complete after termination.

        Args:
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds. Uses default timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping all process UUIDs to their exit statuses
        """
        # Retrieve all active process UUIDs
        with self.lock:
            active_uuids = list(self.process_store.keys())

        if not active_uuids:
            self.log.info("No processes to terminate")
            return {}

        self.log.info(f"Terminating all {len(active_uuids)} active process(es)")

        # Delegate to kill_processes for role-based shutdown ordering
        all_exit_statuses = self.kill_processes(active_uuids, process_timeouts)

        # Wait for all watcher threads to complete
        with self.lock:
            watchers_to_join = list(self.client_watchers.values()) + list(
                self.remote_process_watchers.values()
            )

        for watcher in watchers_to_join:
            try:
                watcher.join(timeout=2.0)
            except Exception:
                pass

        with self.lock:
            self.client_watchers.clear()
            self.remote_process_watchers.clear()

        return all_exit_statuses

    def _build_ssh_arguments(
        self, hostname: str, user_host: str, use_tty: bool = True
    ) -> List[str]:
        """
        Build standard SSH arguments with host key checking policy.

        Args:
            hostname: Target hostname for policy determination
            user_host: User@hostname string for SSH connection
            use_tty: Whether to allocate a pseudo-terminal

        Returns:
            List of SSH command arguments
        """

        # Determine if host key checking should be disabled based on configuration and
        # target host
        # disable_host_key_check = self.disable_host_key_check or (
        #     self.disable_localhost_host_key_check
        #     and hostname in ("localhost", "127.0.0.1", "::1")
        # )
        superuser_host = getpass.getuser() + "@" + user_host.split("@")[1]
        # self.log.critical(f"Building SSH arguments for {user_host} with superuser host {superuser_host}")
        arguments = [superuser_host, "-o", "StrictHostKeyChecking=no"]
        # self.log.critical(f"SSH arguments after adding StrictHostKeyChecking for {user_host}%s", arguments)
        # self.log.critical(f"{arguments=}")
        # self.log.critical(f"Test list: {test_list_print}")
        # self.log.critical(f"Test list 2: %s", test_list_print)

        # Base SSH arguments with user@host and strict host key checking disabled
        # StrictHostKeyChecking=no is set to as we have an nfs backed home directory and
        # the known_hosts file is not shared across hosts, so we cannot rely on it for
        # host key verification.
        # arguments = [user_host, "-o", "StrictHostKeyChecking=no"]
        # "-F /nfs/home/{user_host.split('@')[0]}/.ssh/config",

        if use_tty:
            arguments.append("-tt")

        # If host key checking is disabled, also disable known hosts file usage and
        # reduce log level to avoid cluttering logs with warnings about host key verification
        # if disable_host_key_check:
        arguments.extend(
            [
                "-o",
                "LogLevel=info",
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]
        )
        # self.log.critical(f"SSH arguments for {user_host}: {arguments}")
        # self.log.critical(
        #     f"PP: {getpass.getuser()} is running on {os.uname().nodename} with disable_host_key_check={self.disable_host_key_check} and disable_localhost_host_key_check={self.disable_localhost_host_key_check}"
        # )

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
            user_host = f"{user}@{hostname}"

            # Metadata read is non-interactive and machine-readable.
            arguments = self._build_ssh_arguments(
                hostname,
                user_host,
                use_tty=False,
            )

            remote_command = (
                f"timeout {timeout} sh -c '"
                f'metadata_file="{metadata_file}"; '
                f'while [ ! -s "$metadata_file" ]; do sleep 0.05; done; '
                f'cat "$metadata_file"'
                f"'"
            )
            arguments.append(remote_command)

            # Execute SSH command to wait for and read file (single round-trip)
            # self.log.critical(
            #     f"Attempting to read metadata for {uuid} from {hostname} with timeout {timeout}s"
            # )
            result = self.ssh(*arguments)
            # self.log.critical(
            #     f"DEBUG - Raw metadata content for {uuid} from {hostname}: {result}"
            # )
            json_content = str(result).strip()
            # self.log.critical("Attempt successful?|???")

            self.log.debug(f"Metadata content for {uuid}: {json_content!r}")

            metadata = ProcessMetadata.from_json(json_content)

            with self.lock:
                running_process = self.process_store.get(uuid)
                if running_process is not None:
                    running_process.populate_from_metadata(metadata)

            return metadata

        except Exception as e:
            self.log.warning(f"Failed to read metadata for {uuid}: {e}")
            return None

    def _handle_external_client_sigquit(
        self,
        uuid: str,
        hostname: str,
        user: str,
        remote_pid: Optional[int],
        metadata_file: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> None:
        """React to an externally delivered SIGQUIT on the SSH client."""
        if remote_pid is None:
            self.log.warning(
                f"SSH client for {uuid} received SIGQUIT without remote PID metadata. "
                f"Remote cleanup cannot be guaranteed."
            )
            return

        self.log.info(
            f"SSH client for {uuid} received external SIGQUIT. "
            f"Enforcing remote cleanup through PID {remote_pid}."
        )

        if self._is_remote_pid_alive(hostname, user, remote_pid):
            self.log.info(
                f"Remote process {uuid} (PID {remote_pid}) is still alive after SSH client SIGQUIT. "
                f"Sending SIGKILL via remote PID as failsafe."
            )
            self._send_remote_signal(hostname, user, remote_pid, "KILL")
            remote_dead = self._wait_for_remote_pid_exit(
                hostname,
                user,
                remote_pid,
                timeout=timeout,
            )
            if not remote_dead:
                self.log.error(
                    f"Remote process {uuid} (PID {remote_pid}) did not exit after remote SIGKILL failsafe."
                )
        else:
            self.log.info(
                f"Remote process {uuid} (PID {remote_pid}) had already exited after SSH client SIGQUIT."
            )

        self._cleanup_remote_file(hostname, user, metadata_file)

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
        env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        """Execute SSH command using sh library."""
        try:
            platform = os.uname().sysname.lower()
            is_macos = "darwin" in platform
            hostname_for_gssapi = hostname
            if hostname_for_gssapi == "localhost":
                hostname_for_gssapi = os.uname().nodename
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
            # self.log.critical(
            #     f"Metadata file for {uuid} will be written to {metadata_file} on remote host"
            # )
            tree_id = boot_request.process_description.metadata.tree_id
            name = boot_request.process_description.metadata.name
            is_controller = any(
                e_and_a.exec == "drunc-controller"
                for e_and_a in boot_request.process_description.executable_and_arguments
            )
            role = ProcessMetadata.compute_role_from_tree_id(
                tree_id, is_controller=is_controller
            )

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
                f"rm {log_file}; "  # delete log file so no issues on ovewriting in th next line
                f"{command} &> {log_file} & PID=$! ; "
                f"trap 'kill -HUP $PID 2>/dev/null || true; wait $PID 2>/dev/null || true' HUP TERM INT QUIT ; "
                f"echo '{remote_metadata_json}' > {metadata_file} ; "
                f"wait $PID"
            )

            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.append(remote_cmd)

            # Test access to CMD
            cd_path = f"{boot_request.process_description.process_execution_directory}"
            touch_cmd = [
                arguments[0],  # assume first arg is username@host
                f"touch {cd_path}/.write_test && rm {cd_path}/.write_test",
            ]
            self.log.debug(f"running {touch_cmd} for CMD access test")
            try:
                access = self.ssh(
                    *touch_cmd,
                    _out=self.log.warning,
                    _err=self.log.error,
                    _bg=True,
                    _bg_exc=False,
                    _new_session=True,
                    _preexec_fn=on_parent_exit(signal.SIGTERM)
                    if not is_macos
                    else None,
                )

                access.wait()
                if access.exit_code != 0:
                    raise RuntimeError("SSH error fails to finish successfully")
            except Exception as e:
                err_msg = (
                    f"No access to {cd_path}"
                    "for multiusers to work, the above path needs elevated permissions for"
                    " the PM superuser to cd and write into. "
                    "Please change the permissions to allow for this."
                )
                self.log.error(err_msg)
                raise RuntimeError from e

            process = self.ssh(
                *arguments,
                _out=self.log.debug,
                _err=self._ssh_client_stderr_logger,
                _bg=True,
                _bg_exc=False,
                _new_session=True,
                _preexec_fn=on_parent_exit(signal.SIGTERM) if not is_macos else None,
            )
            assert isinstance(process, sh.RunningCommand), (
                "Expected a RunningCommand instance from sh library"
            )
            # Store process info
            with self.lock:
                running_process = RunningSSHProcess(
                    process=process,
                    hostname=hostname,
                    user=user,
                )
                self.process_store[uuid] = running_process
                # Metadata will be populated asynchronously by watcher thread
                self.metadata[uuid] = None

            self._start_process_watcher(
                uuid,
                running_process,
                hostname,
                user,
                metadata_file,
            )
            self.log.debug(f"SSH command started for {uuid}")
        except Exception as e:
            with self.lock:
                if uuid in self.process_store:
                    del self.process_store[uuid]
            raise RuntimeError(f"Failed to execute SSH command for {uuid}: {e}")

    def kill_process_without_metadata(
        self,
        uuid: str,
        signal_name: str = "KILL",
        as_manual_pm_kill: bool = True,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> Optional[ExitStatus]:
        """
        Terminate process by signalling the local SSH client without using remote metadata.
        Prefer kill_process(..) to this method, this is mainly intended to help with testing

        Args:
            uuid: Process UUID to terminate
            signal_name: Signal to send to SSH client process group (QUIT/KILL)
            as_manual_pm_kill: If True, classify as process-manager initiated kill.
                               If False, classify as external kill i.e. outside of process manager control
            timeout: Maximum time to wait for process termination in seconds

        Returns:
            ExitStatus if termination state can be determined, None otherwise
        """
        with self.lock:
            running_process = self.process_store.get(uuid)
            metadata = self.metadata.get(uuid)

        if running_process is None:
            self.log.warning(
                f"kill_process_without_metadata called for unknown UUID {uuid}"
            )
            return None

        signal_name = signal_name.upper()

        source_for_return = (
            ExitStatusSource.MANUAL_KILL_THROUGH_SSH_CLIENT
            if as_manual_pm_kill
            else ExitStatusSource.CLIENT_MONITORING
        )
        # Ensure watcher callbacks classify this termination path as requested.
        with self.lock:
            running_process.pending_exit_status_source = source_for_return

        try:
            running_process.kill_client(signal_name=signal_name)
        except Exception as e:
            self.log.debug(
                f"Exception was raised when terminating SSH client process: {e}"
            )

        exit_code = self.wait_for_process_exit_code(uuid, timeout=timeout)

        if exit_code is None and signal_name == "QUIT" and not as_manual_pm_kill:
            remote_pid = metadata.pid if metadata is not None else None
            self._handle_external_client_sigquit(
                uuid,
                running_process.hostname,
                running_process.user,
                remote_pid,
                SSHProcessLifetimeManagerShell.get_metadata_file_path(uuid),
                timeout=timeout,
            )
            exit_code = self.wait_for_process_exit_code(uuid, timeout=timeout)

        if exit_code is None:
            with self.lock:
                running_process.pending_exit_status_source = None
            return None

        self._cleanup_process_resources(uuid)

        return ExitStatus(source_for_return, exit_code)

    def wait_for_process_exit_code(self, uuid: str, timeout: float) -> Optional[int]:
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
            process = self.process_store[uuid].process

        def check_exit_status():
            return not process.is_alive()

        # Wait for process to exit
        got_exit = wait_for(check_exit_status, expected_value=True, timeout=timeout)

        if got_exit:
            try:
                process.wait()
                return process.exit_code
            except sh.ErrorReturnCode as e:
                return e.exit_code
            except Exception as e:
                self.log.debug(f"Exception getting exit code for {uuid}: {e}")
                return None
        else:
            self.log.warning(f"Timeout waiting for exit code of process {uuid}")
            return None

    def kill_process(
        self,
        uuid: str,
        timeout: float = ProcessLifetimeManager.DEFAULT_TIMEOUT_FOR_KILLING_PROCESS,
    ) -> ExitStatus | None:
        """
        Kill a remote process and clean up all associated resources.

        Sends termination signals to the remote process, waits for it to die,
        cleans up remote metadata files, terminates the SSH client, and removes
        the process from internal tracking. Safe to call multiple times.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination in seconds

        Returns:
            ExitStatus of the terminated process, or None if not found or still running
        """
        if uuid not in self.process_store:
            return None

        # Read metadata to get remote PID
        metadata = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.warning(
                f"No remote PID for {uuid}, terminating SSH client. Cannot guarantee remote process termination."
            )
            return self.kill_process_without_metadata(
                uuid,
                as_manual_pm_kill=True,
                timeout=timeout,
            )

        running_process = self.process_store[uuid]

        hostname = running_process.hostname
        user = running_process.user
        metadata_file = SSHProcessLifetimeManagerShell.get_metadata_file_path(uuid)

        remote_pid = metadata.pid
        process_dead = False

        try:
            if not self.is_process_alive(uuid):
                self.log.info(
                    f"Skipping killing remote process {uuid} (PID {remote_pid}). It is already dead."
                )
                exit_code = self.wait_for_process_exit_code(uuid, timeout=timeout)
                self._cleanup_remote_file(hostname, user, metadata_file)
                self._cleanup_process_resources(uuid)
                if exit_code is None:
                    return None
                return ExitStatus(ExitStatusSource.REMOTE_MONITORING, exit_code)

            if not process_dead:
                with self.lock:
                    running_process.pending_exit_status_source = (
                        ExitStatusSource.MANUAL_KILL_THROUGH_REMOTE_PID
                    )
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
                        f"Remote process {uuid} (PID {remote_pid}) did not terminate within timeout of {timeout} seconds after SIGQUIT signal."
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
                    self.log.info(
                        f"Remote process {uuid} (PID {remote_pid}) did not terminate within timeout of {timeout} seconds after SIGKILL signal."
                    )

            if not process_dead:
                self.log.error(
                    f"Remote process {uuid} (PID {remote_pid}) still did not terminate after SIGKILL signal."
                )
                with self.lock:
                    running_process.pending_exit_status_source = None
            else:
                exit_code = self.wait_for_process_exit_code(uuid, timeout=timeout)
                self._cleanup_remote_file(hostname, user, metadata_file)
                self._cleanup_process_resources(uuid)
                if exit_code is None:
                    return None
                return ExitStatus(
                    ExitStatusSource.MANUAL_KILL_THROUGH_REMOTE_PID, exit_code
                )

        except Exception as e:
            with self.lock:
                if uuid in self.process_store:
                    self.process_store[uuid].pending_exit_status_source = None
            self.log.error(f"Error terminating remote process {uuid}: {e}")
            return None

        return None

    def crash_process(self, uuid: str, signal: str = "KILL") -> None:
        """
        Simulate an unexpected process crash by sending by ending the remote process without cleanup.
        This leaves the process manager in the same state
        as if the process had crashed unexpectedly, allowing crash-recovery logic
        to be exercised in tests.

        Args:
            uuid: Process UUID to crash
        """
        if uuid not in self.process_store:
            self.log.warning(f"crash_process called for unknown UUID {uuid}")
            return

        running_process = self.process_store[uuid]
        hostname = running_process.hostname
        user = running_process.user

        metadata = self.metadata.get(uuid, None)
        if metadata is None or metadata.pid is None:
            self.log.warning(
                f"No remote PID for {uuid}, cannot send {signal} to simulate crash."
            )
            return

        remote_pid = metadata.pid
        self.log.debug(
            f"Simulating crash of process {uuid} (PID {remote_pid}): "
            f"sending {signal} without cleanup."
        )
        self._send_remote_signal(hostname, user, remote_pid, signal)

    def _cleanup_process_resources(self, uuid: str) -> None:
        """Remove all resources associated with a process UUID."""
        with self.lock:
            if uuid in self.process_store:
                del self.process_store[uuid]
            if uuid in self.metadata:
                del self.metadata[uuid]
            if uuid in self.client_watchers:
                del self.client_watchers[uuid]
            if uuid in self.remote_process_watchers:
                del self.remote_process_watchers[uuid]

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
            watcher = self.remote_process_watchers.get(uuid)

        if watcher and watcher.is_alive() and watcher.is_monitoring_remotely():
            # Watcher is blocking on remote process, so remote process must be alive
            return True

        return self._is_remote_pid_alive(hostname, user, pid)

    def _is_remote_pid_alive(self, hostname: str, user: str, pid: int) -> bool:
        """Check whether a remote PID is alive (not exited and not zombie)."""
        try:
            user_host = f"{user}@{hostname}"
            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.extend(
                [
                    (
                        f"test -d /proc/{pid} && "
                        f'[ "$(awk \'{{print $3}}\' /proc/{pid}/stat 2>/dev/null)" != "Z" ]'
                    )
                ]
            )
            self.ssh(*arguments)
            return True
        except Exception:
            return False

    def _wait_for_remote_pid_exit(
        self,
        hostname: str,
        user: str,
        pid: int,
        timeout: float,
    ) -> bool:
        """Wait until a remote PID disappears."""
        return bool(
            wait_for(
                lambda: not self._is_remote_pid_alive(hostname, user, pid),
                expected_value=True,
                timeout=timeout,
                poll_interval=0.2,
            )
        )

    def _cleanup_remote_file(self, hostname: str, user: str, remote_file: str) -> None:
        """Remove remote file via SSH."""
        try:
            user_host = f"{user}@{hostname}"
            arguments = self._build_ssh_arguments(hostname, user_host)
            arguments.extend(["rm", "-f", remote_file])
            self.ssh(*arguments)
        except Exception:
            pass
