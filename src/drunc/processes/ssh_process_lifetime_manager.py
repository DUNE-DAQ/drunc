"""
Abstract base class for process lifetime management.

Defines the common interface for managing remote process lifecycles,
including process startup, monitoring, termination, and output capture.
"""

import logging
from abc import ABC, abstractmethod
from typing import Callable, List, Optional

from druncschema.process_manager_pb2 import BootRequest


class ProcessLifetimeManager(ABC):
    """
    Abstract base class for process lifetime management.

    Provides a common interface for starting, monitoring, and terminating
    processes on remote hosts via SSH. Concrete implementations may use
    different underlying SSH libraries (e.g., Paramiko, sh library).
    """

    @abstractmethod
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
        Initialise process lifetime manager.

        Args:
            disable_host_key_check: Disable SSH host key verification for all hosts
            disable_localhost_host_key_check: Disable SSH host key verification for localhost
            logger: Logger instance for real-time output logging
            on_process_exit: Optional callback function(uuid, exit_code, exception) invoked when process exits
        """
        pass

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
    def get_exit_code(self, uuid: str) -> Optional[int]:
        """
        Get process exit code.

        Args:
            uuid: Process UUID

        Returns:
            Exit code if process has terminated, None if still running or not found
        """
        pass

    @abstractmethod
    def terminate_process(self, uuid: str, timeout: float = 10.0) -> None:
        """
        Terminate process gracefully with optional timeout.

        Args:
            uuid: Process UUID to terminate
            timeout: Timeout for graceful termination in seconds
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
    def cleanup_process(self, uuid: str) -> None:
        """
        Clean up process resources.

        Terminates the process (if still running) and releases all associated resources.

        Args:
            uuid: Process UUID to clean up
        """
        pass

    @abstractmethod
    def cleanup_all(self) -> None:
        """
        Clean up all processes and resources.

        Terminates all managed processes and releases all associated resources.
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
