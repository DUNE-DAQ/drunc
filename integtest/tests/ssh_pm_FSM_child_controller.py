import pytest
import urllib.request

import integrationtest.data_file_checks as data_file_checks
import integrationtest.log_file_checks as log_file_checks
import integrationtest.data_classes as data_classes

pytest_plugins = "integrationtest.integrationtest_drunc"

# Values that help determine the running conditions
number_of_data_producers = 2
data_rate_slowdown_factor = 1  # 10 for ProtoWIB/DuneWIB
run_duration = 10  # seconds
readout_window_time_before = 1000
readout_window_time_after = 1001

# Default values for validation parameters
expected_number_of_data_files = 1
check_for_logfile_errors = True
expected_event_count = run_duration
expected_event_count_tolerance = 2
wibeth_frag_params = {
    "fragment_type_description": "WIBEth",
    "fragment_type": "WIBEth",
    "expected_fragment_count": number_of_data_producers,
    "min_size_bytes": 7272,
    "max_size_bytes": 14472,
}
triggercandidate_frag_params = {
    "fragment_type_description": "Trigger Candidate",
    "fragment_type": "Trigger_Candidate",
    "expected_fragment_count": 1,
    "min_size_bytes": 128,
    "max_size_bytes": 216,
}
hsi_frag_params = {
    "fragment_type_description": "HSI",
    "fragment_type": "Hardware_Signal",
    "expected_fragment_count": 0,
    "min_size_bytes": 72,
    "max_size_bytes": 100,
}
ignored_logfile_problems = {
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal",
        "Connection '.*' not found on the application registry",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

# The next three variable declarations *must* be present as globals in the test
# file. They're read by the "fixtures" in conftest.py to determine how
# to run the config generation and nanorc

# The arguments to pass to the config generator, excluding the json
# output directory (the test framework handles that)

# CCM includes FSM, hosts; moduleconfs includes connections
object_databases = ["config/daqsystemtest/integrationtest-objects.data.xml"]

conf_dict = data_classes.drunc_config()
conf_dict.dro_map_config.n_streams = number_of_data_producers
conf_dict.op_env = "integtest"
conf_dict.session = "minimal"
conf_dict.tpg_enabled = False

# For testing, allow drunc to manage ConnectivityService (default is False, integrationtest manages Connectivity Service)
#conf_dict.drunc_connsvc = True
# For testing, specify connectivity service port (default is 0, a random port is chosen for the Connectivity Service)
#conf_dict.connsvc_port = 12345

substitution = data_classes.attribute_substitution(
    obj_id="random-tc-generator",
    obj_class="RandomTCMakerConf",
    updates={"trigger_rate_hz": 1},
)
conf_dict.config_substitutions.append(substitution)

confgen_arguments = {"MinimalSystem": conf_dict}

nanorc_command_list = (
    "boot conf --target root-controller/ru-controller/ru-det-conn-0 start --run-number 101 wait 1 enable-triggers wait ".split()
    + [str(run_duration)]
    +  "conf".split()
    + "disable-triggers wait 2 drain-dataflow wait 2 stop-trigger-sources stop scrap terminate".split()
)

# The tests themselves


def test_no_conf_error_in_child_controller(run_nanorc):
    assert run_nanorc.completed_process.returncode == 0

    logs = ""
    for logfile in run_nanorc.log_files:
        logs += logfile.read_text()
    
    assert "Got error from 'conf' to 'ru-det-conn-0'"  not in logs

