# 05-Aug-2026, KAB: the goal of this test is to validate and demonstrate the use of multiple
# user-specified applications running in the DAQ session that is part of this test.
#
# This integtest was created by copying the small_footprint_quick_test from the daqsystemtest
# repo and converting the assignment of the run control commands to make use of the new
# "daq_session_ingredients" special integtest variable.
#
import functools

import integrationtest.data_classes as idc
import integrationtest.data_file_checks as data_file_checks
import integrationtest.log_file_checks as log_file_checks
import integrationtest.resource_validation as resource_validation
import integrationtest.utility_functions as utility_functions
from daqconf.utils import find_free_port
from integrationtest.get_pytest_tmpdir import get_pytest_tmpdir

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

# Values that help determine the running conditions
number_of_data_producers = 1
run_duration = 20  # seconds

# Default values for validation parameters
expected_number_of_data_files = 1
check_for_logfile_errors = True
expected_event_count = run_duration
expected_event_count_tolerance = 2
wibeth_frag_params = {
    "fragment_type_description": "WIBEth",
    "fragment_type": "WIBEth",
    "expected_fragment_count": number_of_data_producers,
    "min_size_bytes": 14472,
    "max_size_bytes": 21672,
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
    "expected_fragment_count": 1,
    "min_size_bytes": 100,
    "max_size_bytes": 100,
}
ignored_logfile_problems = {
    "connectionservice": [
        "Searching for connections matching uid_regex<errored_frames_q> and data_type Unknown"
    ],
    "-controller": [
        "Worker with pid \\d+ was terminated due to signal 1",
        "Connection '.*' not found on the application registry",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

# Determine if this computer has enough resources for these tests
resource_validator = resource_validation.ResourceValidator()
resource_validator.cpu_count_needs(4, 8)  # 2 for each data source plus 2 more for everything else
resource_validator.free_memory_needs(4, 6)  # 33% more than what we observe being used ('free -h')
actual_output_path = get_pytest_tmpdir()
resource_validator.free_disk_space_needs(actual_output_path, 1)  # more than what we observe

# The arguments to pass to the config generator, excluding the json
# output directory (the test framework handles that)

conf_dict = idc.integtest_params_for_generated_dunedaq_config()
conf_dict.object_databases = ["config/daqsystemtest/integrationtest-objects.data.xml"]
conf_dict.dro_map_config.n_streams = number_of_data_producers
conf_dict.op_env = "integtest"
conf_dict.config_session_name = "smallfootprint"
conf_dict.tpg_enabled = False
utility_functions.enable_fake_hsi_trigger(conf_dict, trigger_rate=1.0)

conf_dict.config_substitutions.append(
    idc.attribute_substitution(obj_class="LatencyBuffer", updates={"size": 50000})
)

confgen_arguments = {"SmallFootprint": conf_dict}

# The commands to run in dunerc and the process manager shell
dunerc_commands_1 = (
    "boot conf start --run-number 101 wait 1 enable-triggers wait ".split()
    + [str(run_duration)] + ["disable-triggers"]
)
dunerc_commands_2 = (
    "drain-dataflow stop-trigger-sources stop wait 2 scrap terminate".split()
)
pmshell_command = ["ps"]

# Find a free network port to use for the process manager
pm_port = find_free_port(50020, 52000)

# The command lines that should be used to start the applications
procmsg_startup_commands = ["drunc-process-manager", "<proc_mgr_choice>", str(pm_port)]
pmapp = idc.DAQControlApplication("pm", procmsg_startup_commands)

pmshell_startup_commands = ["drunc-process-manager-shell", f"grpc://localhost:{pm_port}"]
pmshellapp = idc.DAQControlApplication("pmshell", pmshell_startup_commands)

drunc_startup_commands = ["drunc-unified-shell", f"grpc://localhost:{pm_port}", "<config_data_file>", "<config_session_name>", "<daq_session_name>"]
druncapp = idc.DAQControlApplication("drunc", drunc_startup_commands)

# Packaging up the commands into DAQCommandSets
cmd_set_1 = idc.DAQCommandSet("drunc", dunerc_commands_1, idc.CommandWaitParameters(style=idc.CommandWaitStyle.ECHO))
cmd_set_2 = idc.DAQCommandSet("pmshell", pmshell_command, idc.CommandWaitParameters(style=idc.CommandWaitStyle.TIME))
cmd_set_3 = idc.DAQCommandSet("drunc", dunerc_commands_2, idc.CommandWaitParameters(style=idc.CommandWaitStyle.ECHO))

# Putting everything together into a DAQSessionIngredients object
app_list = [ pmapp, pmshellapp, druncapp ]
cmd_set_list = [ cmd_set_1, cmd_set_2, cmd_set_3 ]
dsi = idc.DAQSessionIngredients(app_list, cmd_set_list)

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"MultiRCAppSession": dsi}


# The tests themselves


def test_dunerc_success(run_dunerc, caplog):
    # checks for run control success, problems during pytest setup, etc.
    utility_functions.basic_checks(run_dunerc, caplog, print_test_name=False)


def test_log_files(run_dunerc):
    if check_for_logfile_errors:
        # Check that there are no warnings or errors in the log files
        assert log_file_checks.logs_are_error_free(
            run_dunerc.log_files, True, True, ignored_logfile_problems,
            verbosity_helper=run_dunerc.verbosity_helper
        )


def test_data_files(run_dunerc):
    # Run some tests on the output data file
    assert len(run_dunerc.data_files) == expected_number_of_data_files

    fragment_check_list = [triggercandidate_frag_params, hsi_frag_params]
    fragment_check_list.append(wibeth_frag_params)  # WIBEth

    all_ok = True
    for idx in range(len(run_dunerc.data_files)):
        data_file = data_file_checks.DataFile(run_dunerc.data_files[idx], run_dunerc.verbosity_helper)
        all_ok &= data_file_checks.sanity_check(data_file)
        all_ok &= data_file_checks.check_file_attributes(data_file)
        all_ok &= data_file_checks.check_event_count(
            data_file, expected_event_count, expected_event_count_tolerance
        )
        for jdx in range(len(fragment_check_list)):
            all_ok &= data_file_checks.check_fragment_count(
                data_file, fragment_check_list[jdx]
            )
            all_ok &= data_file_checks.check_fragment_sizes(
                data_file, fragment_check_list[jdx]
            )
    assert all_ok
