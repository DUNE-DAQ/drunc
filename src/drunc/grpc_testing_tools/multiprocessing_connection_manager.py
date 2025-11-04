"""
Multiprocessing Connection Manager

Manages gRPC server processes using Python's multiprocessing module for
local execution. Handles process creation, event coordination, and lifecycle
management for servers running as separate processes on the same machine.
"""

import multiprocessing
import os
from typing import Any, Dict, Optional

from drunc.grpc_testing_tools.process_connection_manager import (
    ProcessConnectionManager,
    RunningGrpcServer,
)


class MultiprocessingConnectionManager(ProcessConnectionManager):
    """
    Process connection manager using Python's multiprocessing module.

    Executes processes locally using multiprocessing.Process, suitable for
    single-machine testing scenarios where all gRPC servers run on localhost.
    Creates and manages ready/stop events for process coordination.
    """

    def __init__(self, env_vars: Dict[str, str] = None):
        """
        Initialise multiprocessing connection manager.

        Args:
            env_vars: Environment variables to set for child processes
        """
        super().__init__(env_vars)

    def create_process(
        self, process_id: str, target_func: Any, *args, **kwargs
    ) -> RunningGrpcServer:
        """
        Create a multiprocessing.Process handle with coordination events.

        Creates ready and stop events for process lifecycle coordination,
        wraps the target function to set environment variables, and prepares
        the process for execution. Events are appended to the argument list
        so the server functions can use them for signalling.

        Args:
            process_id: Unique identifier for the process
            target_func: Function to execute in the new process
            *args: Arguments to pass to target function
            **kwargs: Keyword arguments to pass to target function

        Returns:
            RunningGrpcServer containing the multiprocessing.Process and events
        """
        # Create coordination events for this process
        ready_event = multiprocessing.Event()
        stop_event = multiprocessing.Event()

        # Wrap target function to set environment variables
        def wrapped_target(*target_args, **target_kwargs):
            # Set environment variables in child process
            for key, value in self.env_vars.items():
                os.environ[key] = value
            return target_func(*target_args, **target_kwargs)

        # Append events to arguments so server functions can use them
        extended_args = args + (ready_event, stop_event)

        # Create process handle with wrapped function and extended arguments
        handle = RunningGrpcServer(process_id, wrapped_target, extended_args, kwargs)

        # Create multiprocessing.Process
        mp_process = multiprocessing.Process(
            target=wrapped_target, args=extended_args, kwargs=kwargs, name=process_id
        )

        handle.set_process(mp_process)

        # Store events in handle for access by server manager
        handle.ready_event = ready_event
        handle.stop_event = stop_event

        self.process_handles[process_id] = handle

        return handle

    def start_process(self, handle: RunningGrpcServer) -> None:
        """
        Start a multiprocessing.Process.

        Args:
            handle: RunningGrpcServer containing multiprocessing.Process

        Raises:
            RuntimeError: If process is already started or cannot start
        """
        if handle.started:
            raise RuntimeError(f"Process {handle.process_id} is already started")

        try:
            handle.process.start()
            handle.mark_started()
        except Exception as e:
            raise RuntimeError(f"Failed to start process {handle.process_id}: {e}")

    def stop_process(self, handle: RunningGrpcServer, timeout: float = 10.0) -> None:
        """
        Stop a multiprocessing.Process gracefully.

        Signals the stop event if present to allow graceful shutdown, then
        attempts termination. Forces kill if process doesn't terminate within
        the timeout period.

        Args:
            handle: RunningGrpcServer containing the process to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If process cannot be stopped
        """
        if not handle.started or not handle.process:
            return

        process = handle.process

        if not process.is_alive():
            return

        # Signal graceful shutdown if event exists
        if handle.stop_event:
            handle.stop_event.set()

        # Attempt graceful termination
        process.terminate()
        process.join(timeout=timeout)

        # Force kill if still alive
        if process.is_alive():
            process.kill()
            process.join(timeout=2.0)

    def is_process_alive(self, handle: RunningGrpcServer) -> bool:
        """
        Check if a multiprocessing.Process is alive.

        Args:
            handle: RunningGrpcServer to check

        Returns:
            True if process is running, False otherwise
        """
        if not handle.started or not handle.process:
            return False

        return handle.process.is_alive()

    def wait_for_termination(
        self, handle: RunningGrpcServer, timeout: Optional[float] = None
    ) -> None:
        """
        Wait for multiprocessing.Process to terminate.

        Args:
            handle: RunningGrpcServer to wait for
            timeout: Maximum time to wait
        """
        if handle.started and handle.process:
            handle.process.join(timeout=timeout)

    def cleanup(self) -> None:
        """Stop all managed processes and cleanup resources."""
        for handle in list(self.process_handles.values()):
            try:
                self.stop_process(handle)
            except Exception as e:
                print(f"Warning: Error stopping process {handle.process_id}: {e}")

        self.process_handles.clear()
