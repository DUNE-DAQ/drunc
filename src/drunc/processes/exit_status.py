from enum import Enum
from typing import Optional


class ExitStatusSource(Enum):
    CLIENT_MONITORING = "client_monitoring"
    REMOTE_MONITORING = "remote_monitoring"
    MANUAL_KILL = "manual_kill"


class ExitStatus:
    def __init__(
        self,
        source: ExitStatusSource,
        raw_exit_code: Optional[int],
    ) -> None:
        self._source = source
        self._raw_exit_code = raw_exit_code
        self._reported_exit_code, self._message_fragment = self._interpret()

    def _interpret(self) -> tuple[Optional[int], str]:
        if self._raw_exit_code is None:
            return None, "exit state could not be determined"

        if self._source is ExitStatusSource.CLIENT_MONITORING:
            return self._interpret_client_monitoring()

        if self._source is ExitStatusSource.REMOTE_MONITORING:
            return self._interpret_remote_monitoring()

        return self._interpret_manual_kill()

    def _interpret_client_monitoring(self) -> tuple[Optional[int], str]:
        if self._raw_exit_code == 255:
            return (
                0,
                "was terminated cleanly after SSH client monitoring ended with status 255 following local SIGQUIT fallback",
            )

        if self._raw_exit_code == -9:
            return (
                -9,
                "lost its SSH client because that client was SIGKILLed externally while client monitoring was active",
            )

        return (
            self._raw_exit_code,
            f"exited while relying on SSH client monitoring with raw exit status {self._raw_exit_code}",
        )

    def _interpret_remote_monitoring(self) -> tuple[Optional[int], str]:
        return (
            self._raw_exit_code,
            f"exited while being monitored through the remote PID watcher with raw exit status {self._raw_exit_code}",
        )

    def _interpret_manual_kill(self) -> tuple[Optional[int], str]:
        if self._raw_exit_code == 255:
            return (
                0,
                "was terminated cleanly by the process manager; SSH client shutdown reported raw exit status 255 and is normalised to 0",
            )

        if self._raw_exit_code == 0:
            return 0, "was terminated cleanly by the process manager"

        return (
            self._raw_exit_code,
            f"was terminated by the process manager with raw exit status {self._raw_exit_code}",
        )

    def get_source(self) -> ExitStatusSource:
        return self._source

    def get_raw_exit_code(self) -> Optional[int]:
        return self._raw_exit_code

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
