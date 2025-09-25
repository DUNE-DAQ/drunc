import multiprocessing
import os
from typing import Any, Dict, Optional

from drunc.tests.grpc.process_connection_manager import (
    ProcessConnectionManager,
    RunningGrpcServer,
)


class MultiprocessingConnectionManager(ProcessConnectionManager):
    """
    Process connection manager using Python's multiprocessing module.

    Executes processes locally using multiprocessing.Process, suitable for
    single-machine testing scenarios where all gRPC servers run on localhost.
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
        Create a multiprocessing.Process handle.

        Args:
            process_id: Unique identifier for the process
            target_func: Function to execute in the new process
            *args: Arguments to pass to target function
            **kwargs: Keyword arguments to pass to target function

        Returns:
            ProcessHandle containing the multiprocessing.Process
        """

        # Wrap target function to set environment variables
        def wrapped_target(*target_args, **target_kwargs):
            # Set environment variables in child process
            for key, value in self.env_vars.items():
                os.environ[key] = value
            return target_func(*target_args, **target_kwargs)

        handle = RunningGrpcServer(process_id, wrapped_target, args, kwargs)

        # Create multiprocessing.Process
        mp_process = multiprocessing.Process(
            target=wrapped_target, args=args, kwargs=kwargs, name=process_id
        )

        handle.set_process(mp_process)
        self.process_handles[process_id] = handle

        return handle

    def start_process(self, handle: RunningGrpcServer) -> None:
        """
        Start a multiprocessing.Process.

        Args:
            handle: ProcessHandle containing multiprocessing.Process

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

        Args:
            handle: ProcessHandle containing the process to stop
            timeout: Maximum time to wait for graceful shutdown

        Raises:
            RuntimeError: If process cannot be stopped
        """
        if not handle.started or not handle.process:
            return

        process = handle.process

        if not process.is_alive():
            return

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
            handle: ProcessHandle to check

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
            handle: ProcessHandle to wait for
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
