# Basic lifecycle checks for the run-control shell connected to standalone
# run-control and process-manager servers.

import functools
import getpass
import os
import re

import integrationtest.data_classes as idc
import pytest
from daqconf.utils import find_free_port
from integ_test_utils import (
    _parse_table_from_index,
    assert_process_presence,
    assert_rows_have_valid_uuids,
    get_ps_table_after_echo,
    get_text_between_echo_markers,
    require_echo_marker_index,
    require_line_containing,
    strip_ansi,
)
from pm_test_common import make_conf_dict

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

conf_dict = make_conf_dict("rc_rcs")

confgen_arguments = {"SmallFootprint": conf_dict}


daq_session_name = "rc-rcs-test"


# Find a free network port to use for the process manager
rc_port = find_free_port(50020, 52000)
pm_port = find_free_port(50020, 52000)


# The commands to run in dunerc and the process manager shell
dunerc_commands = f"""
    echo test_local_echo
    echo --server  test_server_echo

    set-session-config --configuration-id local-1x1-config --configuration-file config/daqsystemtest/example-configs.data.xml --session-name rc-test

    connect-to-pm grpc://localhost:{pm_port}

    validate

    echo pre_boot
    ps
    boot
    wait 15
    echo post_boot
    ps
    echo post_boot_done

    echo test_logs
    logs --name unknown
    logs --name root-controller --how-far 5
    logs --name mlt --how-far 5
    echo test_logs_done


    echo test_terminate
    terminate
    echo test_terminate_done
    ps
    echo test_terminate_ps_done
    """.split()


# The command lines that should be used to start the applications
rc_startup_commands = ["drunc-run-control", str(rc_port)]
rcapp = idc.DAQControlApplication(
    "rc",
    rc_startup_commands,
    idc.KeyPhraseWaitParameters(
        search_phrase="communicating through",
        timeout_waiting_for_first_msg=5,
        wait_time_after_last_msg=5,
    ),
)

procmsg_startup_commands = ["drunc-process-manager", "<proc_mgr_choice>", str(pm_port)]
pmapp = idc.DAQControlApplication(
    "pm",
    procmsg_startup_commands,
    idc.KeyPhraseWaitParameters(
        search_phrase="communicating through",
        timeout_waiting_for_first_msg=5,
        wait_time_after_last_msg=5,
    ),
)

rcshell_startup_commands = [
    "drunc-run-control-shell",
    f"grpc://localhost:{rc_port}",
]
rcshellapp = idc.DAQControlApplication(
    "rcshell",
    rcshell_startup_commands,
    idc.KeyPhraseWaitParameters(search_phrase="Ready"),
)

# Packaging up the commands into DAQCommandSets
cmd_set = idc.DAQCommandSet(
    "rcshell",
    dunerc_commands,
    idc.EchoCommandWaitParameters(),
)

# Putting everything together into a DAQSessionIngredients object
app_list = [pmapp, rcapp, rcshellapp]
cmd_set_list = [cmd_set]
dsi = idc.DAQSessionIngredients(app_list, cmd_set_list)

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"MultiRCAppSession": dsi}


# The tests themselves


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


def test_boot(run_dunerc) -> None:
    """Check that boot starts live managed processes with valid UUIDs."""
    lines = strip_ansi(run_dunerc.completed_processes["rcshell"].stdout).splitlines()
    before_boot = get_text_between_echo_markers(lines, "pre_boot", "post_boot")
    assert "No processes running" in before_boot, (
        f"Expected an empty session before boot.\n{before_boot}"
    )

    boot_end = require_echo_marker_index(lines, "post_boot_done")
    rows = get_ps_table_after_echo(lines[:boot_end], "post_boot")
    assert rows, "Expected managed processes after boot."
    assert_rows_have_valid_uuids(rows)
    assert_process_presence(rows, "root-controller", context="after boot")
    assert_process_presence(rows, "mlt", context="after boot")
    for row in rows:
        assert row["status"] == "Alive", (
            f"Expected '{row['friendly_name']}' to be alive after boot: {row}"
        )


def test_unknown_log_command(run_dunerc) -> None:
    """Check that an unknown process produces the expected logs error."""
    lines = strip_ansi(run_dunerc.completed_processes["rcshell"].stdout).splitlines()
    output = get_text_between_echo_markers(lines, "test_logs", "test_logs_done")
    assert (
        "Bad query for logs: The process corresponding to the query doesn't exist"
        in output
    ), f"Expected an error for the unknown process log query.\n{output}"
    assert "Failed to serialize response!" not in output, (
        f"Expected a useful logs error rather than a serialisation failure.\n{output}"
    )


@pytest.mark.parametrize("process_name", ["root-controller", "mlt"])
def test_process_logs(run_dunerc, process_name: str) -> None:
    """Check that logs returns five lines, including controller readiness."""
    lines = strip_ansi(run_dunerc.completed_processes["rcshell"].stdout).splitlines()
    output = get_text_between_echo_markers(
        lines, "test_logs", "test_logs_done"
    ).splitlines()
    header = require_line_containing(
        output,
        f"{process_name} logs",
        error_message=f"Did not find the '{process_name} logs' header.",
    )
    footer = require_line_containing(
        output,
        f"{process_name} end",
        start_idx=header + 1,
        error_message=f"Did not find the '{process_name} end' footer.",
    )
    log_lines = [line for line in output[header + 1 : footer] if line.strip()]
    assert len(log_lines) == 5, (
        f"Expected five log lines for '{process_name}', got {len(log_lines)}.\n"
        + "\n".join(log_lines)
    )
    if process_name == "root-controller":
        assert any(
            re.search(
                r"drunc\.controller\.core\.init_controller.*Controller ready\s*$",
                line,
            )
            for line in log_lines
        ), "Did not find 'Controller ready' in the root-controller logs."


def test_terminate(run_dunerc) -> None:
    """Check that terminate stops every booted process and empties the session."""
    lines = strip_ansi(run_dunerc.completed_processes["rcshell"].stdout).splitlines()
    output = get_text_between_echo_markers(
        lines, "test_terminate", "test_terminate_done"
    ).splitlines()
    header = require_line_containing(
        output,
        "Terminated process",
        error_message="Did not find the terminated-process table.",
    )
    terminated_rows = _parse_table_from_index(output, header, "ps")
    assert terminated_rows, "Expected terminate to report stopped processes."
    assert_rows_have_valid_uuids(terminated_rows)
    boot_end = require_echo_marker_index(lines, "post_boot_done")
    booted_rows = get_ps_table_after_echo(lines[:boot_end], "post_boot")
    assert booted_rows, "Expected a non-empty post-boot process table."
    assert {row["uuid"] for row in terminated_rows} == {
        row["uuid"] for row in booted_rows
    }, "Terminated processes do not match the booted processes."
    for row in terminated_rows:
        assert row["status"] == "Dead", (
            f"Expected '{row['friendly_name']}' to be dead after terminate: {row}"
        )

    after_terminate = get_text_between_echo_markers(
        lines, "test_terminate_done", "test_terminate_ps_done"
    )
    assert "No processes running" in after_terminate, (
        f"Expected an empty session after terminate.\n{after_terminate}"
    )


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
