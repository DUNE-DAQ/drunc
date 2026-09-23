# The goal of this test is to test all functions in the process manager shell, when
# connected via a standalone process manager.

import functools
import getpass
import os
import re

import integrationtest.data_classes as idc
from daqconf.utils import find_free_port
from integ_test_utils import (
    strip_ansi,
)
from pm_test_common import make_conf_dict

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

conf_dict = make_conf_dict("rc_rcs")

confgen_arguments = {"SmallFootprint": conf_dict}


daq_session_name = "rc-rcs-test"

# The commands to run in dunerc and the process manager shell
dunerc_commands = """
    echo test_local_echo
    echo --server  test_server_echo
    """.split()

# Find a free network port to use for the process manager
rc_port = find_free_port(50020, 52000)

# The command lines that should be used to start the applications
procmsg_startup_commands = ["drunc-run-control", str(rc_port)]
rcapp = idc.DAQControlApplication("rc", procmsg_startup_commands)

rcshell_startup_commands = [
    "drunc-run-control-shell",
    f"grpc://localhost:{rc_port}",
]
rcshellapp = idc.DAQControlApplication("rcshell", rcshell_startup_commands)

# Packaging up the commands into DAQCommandSets
cmd_set = idc.DAQCommandSet(
    "rcshell",
    dunerc_commands,
    idc.CommandWaitParameters(style=idc.CommandWaitStyle.ECHO),
)

# Putting everything together into a DAQSessionIngredients object
app_list = [rcapp, rcshellapp]
cmd_set_list = [cmd_set]
dsi = idc.DAQSessionIngredients(app_list, cmd_set_list)

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"MultiRCAppSession": dsi}


# The tests themselves

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def test_dunerc_success(run_dunerc) -> None:
    """Checks that the drunc integration command sequence completes successfully."""
    # print the name of the current test
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    match_obj = re.search(r".*\[(.+)-run_.*rc.*\d].*", current_test)
    if match_obj:
        current_test = match_obj.group(1)
    banner_line = re.sub(".", "=", current_test)
    print(banner_line)
    print(current_test)
    print(banner_line)

    # Check that dunerc completed correctly
    assert run_dunerc.completed_processes["rcshell"].returncode == 0


#! We should have log files soon
# def test_log_files(run_dunerc) -> None:
#     """Checks that expected process-manager log files exist and are free of errors."""
#     # Check that at least some of the expected log files are present
#     logfile_types = ("df-01", "dfo", "mlt", "ru")
#     log_names = tuple(map(str, run_dunerc.log_files))
#     missing_logfiles = [
#         f"{daq_session_name}_{logfile_type}"
#         for logfile_type in logfile_types
#         if not any(
#             f"{daq_session_name}_{logfile_type}" in str(logname)
#             for logname in log_names
#         )
#     ]
#     assert not missing_logfiles, f"No logfile found for: {', '.join(missing_logfiles)}."

#     # Check that there are no warnings or errors in the log files
#     assert log_file_checks.logs_are_error_free(
#         [
#             logname
#             for logname in run_dunerc.log_files
#             if "process_manager" in str(logname)
#         ],
#         True,
#         True,
#         ignored_logfile_problems,
#     )


def test_echos(run_dunerc) -> None:
    lines_rc = strip_ansi(run_dunerc.completed_processes["rc"].stdout).splitlines()
    lines_rcs = strip_ansi(
        run_dunerc.completed_processes["rcshell"].stdout
    ).splitlines()

    assert any("test_local_echo" in line for line in lines_rcs), (
        "Did not find test local echo"
    )

    assert any("test_server_echo" in line for line in lines_rc), (
        "Did not find test server echo"
    )


def test_connections(run_dunerc) -> None:
    lines_rc = strip_ansi(run_dunerc.completed_processes["rc"].stdout).splitlines()
    lines_rcs = strip_ansi(
        run_dunerc.completed_processes["rcshell"].stdout
    ).splitlines()

    user_name = getpass.getuser()
    rc_connect = f"{user_name} connected from run_control_shell"
    rcs_connect = (
        f"{user_name} connected to the run control through a "
        f"drunc-run-control-shell via address localhost:{rc_port}"
    )

    assert any(rc_connect in line for line in lines_rc), (
        f"Did not find '{rc_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_rc)
    )

    assert any(rcs_connect in line for line in lines_rcs), (
        f"Did not find '{rcs_connect}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(lines_rcs)
    )
