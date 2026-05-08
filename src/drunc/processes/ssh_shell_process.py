"""Runtime model for a process launched through the SSH shell lifetime manager."""

import signal as _signal
import threading
from typing import Optional

import sh

from drunc.processes.exit_status import ExitStatus, ExitStatusSource
from drunc.processes.process_metadata import ProcessMetadata


class RunningSSHProcess:
    """Holds runtime state for a process launched via SSH shell."""

    def __init__(self, process: sh.RunningCommand, hostname: str, user: str) -> None:
        """Initialise runtime state for a managed SSH-launched process.

        Args:
            process: Background SSH command handle returned by ``sh``.
            hostname: Target host used to launch and monitor the remote process.
            user: Remote user used for SSH operations.
        """
        self.process = process
        self.hostname = hostname
        self.user = user
        self.ssh_client_pid = getattr(self.process, "pid", None)
        self.remote_pid: Optional[int] = None
        self.exit_status: Optional[ExitStatus] = None
        self.pending_exit_status_source: Optional[ExitStatusSource] = None
        self.remote_monitoring_pid: Optional[int] = (
            None  # PID of watcher SSH process running remote monitoring
        )
        self.client_monitoring_pid: Optional[int] = (
            None  # PID of watcher SSH process in client monitoring mode
        )
        self._exit_status_lock = threading.Lock()

    def populate_from_metadata(self, metadata: ProcessMetadata) -> None:
        """Populate runtime fields from asynchronously read process metadata."""
        self.remote_pid = metadata.pid

    def consume_exit_status_source(self, default: ExitStatusSource) -> ExitStatusSource:
        if self.pending_exit_status_source is None:
            return default
        source = self.pending_exit_status_source
        self.pending_exit_status_source = None
        return source

    def finalise_exit(
        self, default_source: ExitStatusSource, raw_exit_code: Optional[int]
    ) -> ExitStatus:
        """Consume the pending exit source, build an ExitStatus, and store it."""
        source = self.consume_exit_status_source(default_source)
        self.exit_status = ExitStatus(source, raw_exit_code)
        return self.exit_status

    def finalise_exit_once(
        self, default_source: ExitStatusSource, raw_exit_code: Optional[int]
    ) -> tuple[ExitStatus, bool]:
        """Set exit status once across concurrent watcher threads."""
        with self._exit_status_lock:
            if self.exit_status is not None:
                return self.exit_status, False
            return self.finalise_exit(default_source, raw_exit_code), True

    def kill_client(self, signal_name: str = "KILL") -> None:
        """Send a signal to the local SSH client process group."""
        signal_name = signal_name.upper()
        if signal_name == "QUIT":
            self.process.signal_group(_signal.SIGQUIT)
        elif signal_name == "KILL":
            self.process.signal_group(_signal.SIGKILL)
        else:
            raise ValueError(f"Unsupported signal_name '{signal_name}'")
