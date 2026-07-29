"""
Abstract Process Connection Manager

This module provides abstraction over different process execution methods (multiprocessing, SSH, etc.).
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from drunc.grpc_testing_tools.grpc_running_server_data import (
    RunningGrpcServer,
    TargetFunc,
)


class ProcessConnectionManager(ABC):
    """
    Abstract base class for managing process connections.

    Defines the interface that all process connection implementations must
    follow, whether using local multiprocessing or remote SSH execution.
    """

    def __init__(self, env_vars: Dict[str, str] | None = None) -> None:
        """
        Initialise process connection manager.

        Args:
            env_vars: Environment variables to set for all processes
        """
        self.env_vars = env_vars or {}
        self.process_handles: Dict[str, RunningGrpcServer] = {}

    @abstractmethod
    def create_process(
        self,
        process_id: str,
        target_func: TargetFunc,
        *args: object,
        **kwargs: object,
    ) -> RunningGrpcServer:
        """
        Create a new process handle for execution.

        Args:
            process_id: Unique identifier for the process
            target_func: Function to execute as the process
            *args: Positional arguments for target function
            **kwargs: Keyword arguments for target function

        Returns:
            RunningGrpcServer: Handle for managing the created process
        """
        pass

    @abstractmethod
    def start_process(self, handle: RunningGrpcServer) -> None:
        """
        Start execution of a process.

        Args:
            handle: RunningGrpcServer for the process to start

        Raises:
            RuntimeError: If process cannot be started
        """
        pass

    @abstractmethod
    def stop_process(self, handle: RunningGrpcServer, timeout: float = 10.0) -> None:
        """
        Stop a running process gracefully.

        Args:
            handle: RunningGrpcServer for the process to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If process cannot be stopped
        """
        pass

    @abstractmethod
    def is_process_alive(self, handle: RunningGrpcServer) -> bool:
        """
        Check if a process is currently running.

        Args:
            handle: RunningGrpcServer to check

        Returns:
            True if process is alive, False otherwise
        """
        pass

    @abstractmethod
    def wait_for_termination(
        self, handle: RunningGrpcServer, timeout: Optional[float] = None
    ) -> None:
        """
        Wait for a process to terminate.

        Args:
            handle: RunningGrpcServer to wait for
            timeout: Maximum time to wait (None for indefinite)
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up all resources and stop any remaining processes."""
        pass

    def get_process_handle(self, process_id: str) -> Optional[RunningGrpcServer]:
        """
        Retrieve a process handle by ID.

        Args:
            process_id: ID of the process to retrieve

        Returns:
            RunningGrpcServer if found, None otherwise
        """
        return self.process_handles.get(process_id)

    def list_process_ids(self) -> List[str]:
        """
        Get list of all managed process IDs.

        Returns:
            List of process IDs
        """
        return list(self.process_handles.keys())
