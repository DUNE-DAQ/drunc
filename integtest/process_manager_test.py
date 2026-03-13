import getpass
import os
import re

import integrationtest.data_classes as data_classes
import integrationtest.log_file_checks as log_file_checks

pytest_plugins = "integrationtest.integrationtest_drunc"

# Values that help determine the running conditions
number_of_data_producers = 2
data_rate_slowdown_factor = 1  # 10 for ProtoWIB/DuneWIB
run_duration = 10  # seconds
readout_window_time_before = 1000
readout_window_time_after = 1001

check_for_logfile_errors = True

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
# conf_dict.drunc_connsvc = True
# For testing, specify connectivity service port (default is 0, a random port is chosen for the Connectivity Service)
# conf_dict.connsvc_port = 12345

substitution = data_classes.attribute_substitution(
    obj_id="random-tc-generator",
    obj_class="RandomTCMakerConf",
    updates={"trigger_rate_hz": 1},
)
conf_dict.config_substitutions.append(
    data_classes.attribute_substitution(
        obj_class="TCReadoutMap",
        obj_id="def-random-readout",
        updates={
            "time_before": readout_window_time_before,
            "time_after": readout_window_time_after,
        },
    )
)
conf_dict.config_substitutions.append(substitution)


confgen_arguments = {"MinimalSystem": conf_dict}
# The commands to run in nanorc, as a list
# NOTE THAT WE HAVE NOT TESTED FLUSH BECAUSE IT IS BROKEN
# see #821

dunerc_command_list = f"""
boot

echo testing_logs
logs --name unknown
logs --name root-controller --how-far 5
logs --name mlt --how-far 5

ps -u {getpass.getuser()}

restart -n root-controller
restart -n mlt
wait 5
kill -n mlt
wait 2
restart -n mlt
restart -n trg-controller
wait 5


flush
terminate

""".split()



def test_nanorc_success(run_dunerc):
    # print the name of the current test
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    match_obj = re.search(r".*\[(.+)-run_.*rc.*\d].*", current_test)
    if match_obj:
        current_test = match_obj.group(1)
    banner_line = re.sub(".", "=", current_test)
    print(banner_line)
    print(current_test)
    print(banner_line)

    # Check that nanorc completed correctly
    assert run_dunerc.completed_process.returncode == 0


def test_log_command(run_dunerc) -> None:
    test_str = "Bad query for logs: The process corresponding to the query doesn't exist"
    assert test_str in run_dunerc.completed_process.stdout 


def test_root_controller_logs(run_dunerc) -> None:
    """
    Verifies that:
    - the stdout contains a "root-controller logs" header line and a "root-controller end" footer line
    - there are exactly 5 lines between those two lines
    - among those 5 lines, the one from "drunc.controller.core.init_controller" ends with "Controller ready"
    """
    stdout = run_dunerc.completed_process.stdout
    assert isinstance(stdout, str)

    lines = stdout.splitlines()

    # 1) Find the header/footer lines
    header_idx = next(
        (i for i, line in enumerate(lines) if "root-controller logs" in line),
        None,
    )
    footer_idx = next(
        (i for i, line in enumerate(lines) if "root-controller end" in line),
        None,
    )

    assert header_idx is not None, "Did not find the 'root-controller logs' header line in stdout."
    assert footer_idx is not None, "Did not find the 'root-controller end' footer line in stdout."
    assert footer_idx > header_idx, "Footer appears before header in stdout."

    # 2) Check there are 5 lines between header and footer
    between = lines[header_idx + 1 : footer_idx]
    assert (
        len(between) == 5
    ), f"Expected exactly 5 lines between header and footer, found {len(between)}.\nBetween:\n" + "\n".join(
        between
    )

    # 3) Check the init_controller line ends with "Controller ready"
    # Example line:
    # [2026/03/13 08:17:47 UTC] INFO ... drunc.controller.core.init_controller ... Controller ready
    init_controller_ready_re = re.compile(
        r"drunc\.controller\.core\.init_controller.*Controller ready\s*$"
    )

    matches = [line for line in between if init_controller_ready_re.search(line)]
    assert (
        len(matches) >= 1
    ), "Did not find an init_controller line ending with 'Controller ready' within the 5 lines.\nBetween:\n" + "\n".join(
        between
    )



def test_log_files(run_dunerc):
    # Check that at least some of the expected log files are present
    assert any(
        f"{run_dunerc.session}_df-01" in str(logname)
        for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.session}_dfo" in str(logname) for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.session}_mlt" in str(logname) for logname in run_dunerc.log_files
    )
    assert any(
        f"{run_dunerc.session}_ru" in str(logname) for logname in run_dunerc.log_files
    )

    if check_for_logfile_errors:
        # Check that there are no warnings or errors in the log files
        assert log_file_checks.logs_are_error_free(
            [
                logname for logname in run_dunerc.log_files if "process_manager" in str(logname)
            ], True, True, ignored_logfile_problems
        )
