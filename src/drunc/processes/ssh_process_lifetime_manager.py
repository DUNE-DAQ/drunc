"""
Abstract base class for process lifetime management.

Defines the common interface for managing remote process lifecycles,
including process startup, monitoring, termination, and output capture.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from druncschema.process_manager_pb2 import BootRequest

from drunc.processes.connection_utils import wait_for


class ProcessLifetimeManager(ABC):
    """
    Abstract base class for process lifetime management.

    Provides a common interface for starting, monitoring, and terminating
    processes on remote hosts via SSH. Concrete implementations use
    different underlying SSH libraries (e.g., Paramiko, sh library).
    """

    # The maximum amount of time to wait for a process to die after kill is called
    # before it is considered an error.
    DEFAULT_TIMEOUT_FOR_KILLING_PROCESS = 10.0  # seconds
    # Interval to wait between checking if a process is dead after kill is called.
    KILLING_PROCESS_POLL_INTERVAL = 0.1  # seconds
    # Interval to wait before concluding metadata file writing failed on remote host.
    DEFAULT_TIMEOUT_FOR_READING_METADATA = 10.0  # seconds

    def wait_for_process_to_die(
        self,
        uuid: str,
        timeout: float,
        logger: Optional[Any] = None,
    ) -> bool:
        """
        Wait for a process to terminate within a timeout period.

        Args:
            uuid: Process UUID to monitor

        Returns:
            True if process terminated within timeout, False otherwise
        """
        if logger is not None:
            logger.debug(f"Waiting for process with uuid: {uuid} to terminate...")
        result = wait_for(
            condition=lambda: self.is_process_alive(uuid),
            expected_value=False,
            timeout=timeout,
            poll_interval=self.KILLING_PROCESS_POLL_INTERVAL,
            logger=logger,
        )

        return result == False

    @abstractmethod
    def get_active_process_keys(self) -> List[str]:
        """
        Get list of active process UUIDs.

        Returns:
            List of active process UUID strings
        """
        pass

    @abstractmethod
    def start_process(self, uuid: str, boot_request: BootRequest) -> None:
        """
        Start a remote process using the boot request configuration.

        Extracts all necessary parameters from the boot request and executes
        the process on the remote host via SSH.

        Args:
            uuid: Unique identifier for this process
            boot_request: BootRequest containing process configuration, metadata,
                        environment variables, and execution parameters

        Raises:
            RuntimeError: If SSH connection or process execution fails
        """
        pass

    @abstractmethod
    def is_process_alive(self, uuid: str) -> bool:
        """
        Check if process is alive.

        Args:
            uuid: Process UUID to check

        Returns:
            True if process is alive, False otherwise
        """
        pass

    @abstractmethod
    def pop_early_exit_code(self, uuid: str) -> Optional[int]:
        """
        If a process was killed before kill_process was called. This method
        retrieves and removes the exit code from internal storage. Otherwise
        it will return None.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process is dead, None if still running or not found
        """
        pass

    @abstractmethod
    def kill_process(
        self, uuid: str, timeout: float = DEFAULT_TIMEOUT_FOR_KILLING_PROCESS
    ) -> Optional[int]:
        """
        Kill a remote process and clean up associated resources upon successful termination.
        Sends termination signals to the remote process and waits for it to die.
        Safe to call multiple times - subsequent calls will have no effect if
        resources have already been cleaned up.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination in seconds

        Returns:
          the exit code of the process if it was able to be determined (None otherwise).

        """
        pass

    @abstractmethod
    def kill_processes(
        self, uuids: List[str], process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[int]]:
        """
        Kill multiple processes by their UUIDs in role-based shutdown order.

        Processes are separated by role and terminated in stages to ensure clean
        shutdown. Within each role, processes are killed asynchronously. After
        role-based termination, any remaining processes are killed asynchronously
        as a fallback.

        Args:
            uuids: List of process UUIDs to terminate
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds for graceful termination. Uses default
                            timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping process UUIDs to their exit codes. None indicates
            exit code could not be determined.
        """
        pass

    @abstractmethod
    def kill_all_processes(
        self, process_timeouts: Optional[Dict[str, float]] = None
    ) -> Dict[str, Optional[int]]:
        """
        Kill all managed processes and clean up resources.

        Iterates through all active processes, terminates them, and cleans up
        associated resources.

        Args:
            process_timeouts: Dictionary mapping process UUIDs to their respective timeouts for graceful termination in seconds
                              If not specified a default timeout will be used for all processes.

        Returns:
            Dictionary mapping process UUIDs to their exit codes (None if not determined)
        """
        pass

    @abstractmethod
    def kill_processes_by_role(
        self,
        role: str,
        candidate_uuids: List[str],
        process_timeouts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Optional[int]]:
        """
        Kill all processes with the specified role from candidate UUID list.

        Filters candidate UUIDs by role metadata and terminates matching processes
        asynchronously for parallel shutdown within the role.

        Args:
            role: Process role to match (e.g., "application", "controller")
            candidate_uuids: List of process UUIDs to filter and potentially terminate
            process_timeouts: Dictionary mapping process UUIDs to timeout values
                            in seconds. Uses default timeout for unmapped UUIDs.

        Returns:
            Dictionary mapping terminated process UUIDs to their exit codes.
            Only includes processes matching the specified role.
        """
        pass

    @abstractmethod
    def get_process_stdout(self, uuid: str) -> Optional[str]:
        """
        Get stdout from process.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stdout content as string, None if not found
        """
        pass

    @abstractmethod
    def get_process_stderr(self, uuid: str) -> Optional[str]:
        """
        Get stderr from process.

        Args:
            uuid: Process UUID

        Returns:
            Accumulated stderr content as string, None if not found
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def validate_host_connection(
        self,
        host: str,
        auth_method: str,
        user: str,
    ) -> None:
        """
        Validate SSH connection to the specified host.

        Attempts to establish an SSH connection to the host and execute a
        simple command to verify connectivity. Used to validate access before
        starting processes.

        Args:
            host: Target hostname
            auth_method: Authentication method to use (implementation-specific)
            user: SSH username

        Raises:
            RuntimeError: If SSH connection or command execution fails
        """
        pass
