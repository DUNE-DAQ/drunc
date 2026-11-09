"""Shared setup for process-manager integration tests."""

from dataclasses import dataclass, field

import integrationtest.resource_validation as resource_validation
from integrationtest.get_pytest_tmpdir import get_pytest_tmpdir

number_of_data_producers = 2
data_rate_slowdown_factor = 1
run_duration = 10
readout_window_time_before = 1000
readout_window_time_after = 1001

ignored_logfile_problems = {
    "connectionservice": [
        "Searching for connections matching uid_regex<errored_frames_q> and data_type Unknown"
    ],
    "SSH_SHELL_process_manager": [
        "was terminated unexpectedly through the remote pid by a SIGKILL",
    ],
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal 1",
        "Connection '.*' not found on the application registry",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

resource_validator = resource_validation.ResourceValidator()
resource_validator.cpu_count_needs(4, 8)
resource_validator.free_memory_needs(4, 6)
resource_validator.free_disk_space_needs(get_pytest_tmpdir(), 1)


@dataclass
class FsmCommandParams:
    """Describes a single FSM command to send in a dunerc command sequence, and
    the state/substate it is expected to leave the session in."""

    marker: str
    command: str
    expected_state: str
    non_controller_substate: str = "idle"
    command_args: list[str] = field(default_factory=list)
    run_number: int | None = None

    @property
    def done_marker(self) -> str:
        return f"{self.marker}_done"

    @property
    def full_command(self) -> str:
        return " ".join([self.command] + self.command_args)

    def to_command_block(self) -> str:
        return f"""
echo {self.marker}
{self.full_command}
echo {self.marker}_done
status
echo {self.marker}_status_done
"""
