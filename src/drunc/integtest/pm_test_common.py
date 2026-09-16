"""Shared setup for process-manager integration tests."""

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
