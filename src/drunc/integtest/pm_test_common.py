"""Shared setup for process-manager integration tests."""

from dataclasses import dataclass, field

import integrationtest.data_classes as idc
import integrationtest.resource_validation as resource_validation
import integrationtest.utility_functions as utility_functions
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


def make_conf_dict(config_session_name: str):
    """Build the common generated DAQ configuration for a test session."""
    conf_dict = idc.integtest_params_for_generated_dunedaq_config()
    conf_dict.object_databases = [
        "config/daqsystemtest/integrationtest-objects.data.xml"
    ]
    conf_dict.dro_map_config.n_streams = number_of_data_producers
    conf_dict.op_env = "integtest"
    conf_dict.config_session_name = config_session_name
    conf_dict.tpg_enabled = False
    utility_functions.enable_fake_hsi_trigger(conf_dict, trigger_rate=1.0)
    conf_dict.config_substitutions.append(
        idc.attribute_substitution(obj_class="LatencyBuffer", updates={"size": 50000})
    )
    return conf_dict


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
status -w 140
echo {self.marker}_status_done
"""
