from enum import Enum
from typing import Optional


class ExitStatusSource(Enum):
    CLIENT_MONITORING = "client_monitoring"
    REMOTE_MONITORING = "remote_monitoring"
    MANUAL_KILL_THROUGH_SSH_CLIENT = "manual_kill_through_ssh_client"
    MANUAL_KILL_THROUGH_REMOTE_PID = "manual_kill_through_remote_pid"


class ExitStatus:
    def __init__(
        self,
        source: ExitStatusSource,
        raw_exit_code: Optional[int],
    ) -> None:
        self._source = source
        self._raw_exit_code = raw_exit_code
        self._reported_exit_code, self._message_fragment = self._interpret()

    def _interpret(self) -> tuple[None | int, str]:
        if self._raw_exit_code is None:
            return None, "exit state could not be determined"

        if self._source is ExitStatusSource.CLIENT_MONITORING:
            return self._interpret_client_monitoring()

        if self._source is ExitStatusSource.REMOTE_MONITORING:
            return self._interpret_remote_monitoring()

        if self._source is ExitStatusSource.MANUAL_KILL_THROUGH_SSH_CLIENT:
            return self._interpret_manual_kill_through_ssh_client()

        if self._source is ExitStatusSource.MANUAL_KILL_THROUGH_REMOTE_PID:
            return self._interpret_manual_kill_through_remote_pid()

        return None, "Unrecognised exit status"

    def _interpret_client_monitoring(self) -> tuple[None | int, str]:
        if self._raw_exit_code == 255:
            return (
                None,
                "was terminated unexpectedly with SIGQUIT on the SSH client (SIGHUP on the server)",
            )

        if self._raw_exit_code == -9:
            return (
                None,
                "was terminated unexpectedly by a SIGKILL to the SSH client (SIGHUP on the server)",
            )

        return (
            self._raw_exit_code,
            f"was terminated unexpectedly with unusual SSH client exit code of {self._raw_exit_code}",
        )

    def _interpret_remote_monitoring(self) -> tuple[Optional[int], str]:
        return (
            self._raw_exit_code,
            "was terminated unexpectedly through the remote pid",
        )

    def _interpret_manual_kill_through_ssh_client(self) -> tuple[Optional[int], str]:
        if self._raw_exit_code == 255 or self._raw_exit_code == 0:
            return (
                None,
                "was terminated by the process manager through the SSH client",
            )

        if self._raw_exit_code == -9:
            return (
                None,
                "was terminated with a SIGKILL by the process manager through the SSH client",
            )

        return (
            self._raw_exit_code,
            f"was terminated by the process manager through the SSH client with an unusual exit code of: {self._raw_exit_code}",
        )

    def _interpret_manual_kill_through_remote_pid(self) -> tuple[Optional[int], str]:
        return (
            self._raw_exit_code,
            "was terminated by the process manager through the remote pid",
        )

    def get_source(self) -> ExitStatusSource:
        return self._source

    def get_reported_exit_code(self) -> Optional[int]:
        return self._reported_exit_code

    def get_process_manager_log_message(
        self,
        process_name: str,
        session: str,
        user: str,
    ) -> str:
        return (
            f"Process '{process_name}' (session: '{session}', user: '{user}') "
            f"{self._message_fragment}. "
            f"Reported exit code: {self._reported_exit_code}."
        )

    def __repr__(self) -> str:
        return (
            "ExitStatus("
            f"source={self._source.value!r}, "
            f"raw_exit_code={self._raw_exit_code!r}, "
            f"reported_exit_code={self._reported_exit_code!r}"
            ")"
        )
