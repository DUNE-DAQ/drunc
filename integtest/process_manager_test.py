import getpass
import os
import re
from datetime import datetime

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

echo pre_boot
ps -u {getpass.getuser()}
boot
echo on_boot
ps -u {getpass.getuser()}


echo testing_logs
logs --name unknown
logs --name root-controller --how-far 5
logs --name mlt --how-far 5

echo test_wait
wait 10

echo pre_restart_mlt
restart -n mlt
restart -n root-controller
wait 5
echo post_restart_mlt


echo pre_kill_mlt
ps -u {getpass.getuser()}
kill -n mlt
wait 2
echo post_kill_mlt
ps -u {getpass.getuser()}


restart -n mlt
restart -n trg-controller
wait 5

echo ps_after_recovery
ps -u {getpass.getuser()}


flush
terminate

""".split()


UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    r"|^[0-9a-fA-F]{8}-[-0-9a-fA-F]*\u2026"  # truncated by Rich table column width
)
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def _parse_ps_table_from_index(
    lines: list[str], start_idx: int
) -> list[dict[str, str]]:
    table_rows: list[dict[str, str]] = []

    for line in lines[start_idx + 1 :]:
        stripped = line.strip()

        if stripped.startswith("└"):
            break

        if not stripped.startswith("│"):
            continue

        cells = [cell.strip() for cell in stripped.strip("│").split("│")]
        if len(cells) < 7:
            continue

        table_rows.append(
            {
                "session": cells[0],
                "friendly_name": cells[1],
                "user": cells[2],
                "host": cells[3],
                "uuid": cells[4],
                "alive": cells[5],
                "exit_code": cells[6],
            }
        )

    return table_rows


def get_ps_table_after_echo(stdout: str, echo_marker: str) -> list[dict[str, str]]:
    lines = strip_ansi(stdout).splitlines()

    echo_idx = next(
        (
            idx
            for idx, line in enumerate(lines)
            if "drunc.echo" in line and line.rstrip().endswith(echo_marker)
        ),
        None,
    )
    assert echo_idx is not None, (
        f"Could not find drunc.echo marker '{echo_marker}' in stdout."
    )

    table_start_idx = next(
        (
            idx
            for idx in range(echo_idx + 1, len(lines))
            if "Processes running" in lines[idx]
        ),
        None,
    )
    if table_start_idx is None:
        return []

    return _parse_ps_table_from_index(lines, table_start_idx)


def get_uuid_for_friendly_name(
    ps_table: list[dict[str, str]], friendly_name: str
) -> str:
    for row in ps_table:
        if row["friendly_name"].strip() == friendly_name:
            return row["uuid"]

    available_names = ", ".join(row["friendly_name"].strip() for row in ps_table)
    raise AssertionError(
        f"Could not find friendly name '{friendly_name}' in ps table. "
        f"Available names: {available_names}"
    )


def test_boot(run_dunerc) -> None:
    """Checks that boot starts the managed processes and exposes UUIDs in ps."""
    stdout = run_dunerc.completed_process.stdout

    ps_pre_boot = get_ps_table_after_echo(stdout, "pre_boot")
    ps_on_boot = get_ps_table_after_echo(stdout, "on_boot")

    assert not ps_pre_boot, (
        f"Expected ps table before boot to be empty, but found {len(ps_pre_boot)} row(s): "
        + ", ".join(row["friendly_name"] for row in ps_pre_boot)
    )

    assert ps_on_boot, (
        "Expected ps table after boot to contain processes, but it was empty."
    )
    for row in ps_on_boot:
        assert UUID_RE.match(row["uuid"]), (
            f"Expected a valid UUID for process '{row['friendly_name']}', got '{row['uuid']}'"
        )


def test_log_command(run_dunerc) -> None:
    """Checks that querying logs for an unknown process reports the expected error."""
    test_str = (
        "Bad query for logs: The process corresponding to the query doesn't exist"
    )
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

    assert header_idx is not None, (
        "Did not find the 'root-controller logs' header line in stdout."
    )
    assert footer_idx is not None, (
        "Did not find the 'root-controller end' footer line in stdout."
    )
    assert footer_idx > header_idx, "Footer appears before header in stdout."

    # 2) Check there are 5 lines between header and footer
    between = lines[header_idx + 1 : footer_idx]
    assert len(between) == 5, (
        f"Expected exactly 5 lines between header and footer, found {len(between)}.\nBetween:\n"
        + "\n".join(between)
    )

    # 3) Check the init_controller line ends with "Controller ready"
    # Example line:
    # [2026/03/13 08:17:47 UTC] INFO ... drunc.controller.core.init_controller ... Controller ready
    init_controller_ready_re = re.compile(
        r"drunc\.controller\.core\.init_controller.*Controller ready\s*$"
    )

    matches = [line for line in between if init_controller_ready_re.search(line)]
    assert len(matches) >= 1, (
        "Did not find an init_controller line ending with 'Controller ready' within the 5 lines.\nBetween:\n"
        + "\n".join(between)
    )


def test_wait_command_duration_from_logs(run_dunerc) -> None:
    """Checks that the wait command logs the expected duration and elapsed time."""
    stdout = run_dunerc.completed_process.stdout
    lines = strip_ansi(stdout).splitlines()

    echo_idx = next(
        (
            idx
            for idx, line in enumerate(lines)
            if "drunc.echo" in line and line.rstrip().endswith("test_wait")
        ),
        None,
    )
    assert echo_idx is not None, (
        "Could not find drunc.echo marker 'test_wait' in stdout."
    )

    running_pattern = re.compile(r"Command wait running for (\d+) seconds\.")
    ran_pattern = re.compile(r"Command wait ran for (\d+) seconds\.")
    timestamp_pattern = re.compile(r"^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) UTC\]")

    running_idx = next(
        (
            idx
            for idx in range(echo_idx + 1, len(lines))
            if running_pattern.search(lines[idx])
        ),
        None,
    )
    assert running_idx is not None, (
        "Did not find 'Command wait running for ... seconds.' after test_wait marker."
    )

    ran_idx = next(
        (
            idx
            for idx in range(running_idx + 1, len(lines))
            if ran_pattern.search(lines[idx])
        ),
        None,
    )
    assert ran_idx is not None, (
        "Did not find 'Command wait ran for ... seconds.' after wait start log."
    )

    running_match = running_pattern.search(lines[running_idx])
    ran_match = ran_pattern.search(lines[ran_idx])
    assert running_match is not None
    assert ran_match is not None

    expected_seconds = 10
    assert int(running_match.group(1)) == expected_seconds, (
        f"Expected wait start log to report {expected_seconds} seconds, got {running_match.group(1)}."
    )
    assert int(ran_match.group(1)) == expected_seconds, (
        f"Expected wait end log to report {expected_seconds} seconds, got {ran_match.group(1)}."
    )

    start_ts_match = timestamp_pattern.search(lines[running_idx])
    end_ts_match = timestamp_pattern.search(lines[ran_idx])
    assert start_ts_match is not None, (
        "Could not parse timestamp in wait start log line."
    )
    assert end_ts_match is not None, "Could not parse timestamp in wait end log line."

    start_ts = datetime.strptime(start_ts_match.group(1), "%Y/%m/%d %H:%M:%S")
    end_ts = datetime.strptime(end_ts_match.group(1), "%Y/%m/%d %H:%M:%S")
    elapsed_seconds = (end_ts - start_ts).total_seconds()

    tolerance_seconds = 1
    assert abs(elapsed_seconds - expected_seconds) <= tolerance_seconds, (
        f"Expected wait log timestamps to differ by {expected_seconds}±{tolerance_seconds} seconds, "
        f"got {elapsed_seconds} seconds."
    )


def test_restart_mlt_logs(run_dunerc) -> None:
    """Checks that restarting mlt produces the expected restart, exit, and boot logs."""
    stdout = run_dunerc.completed_process.stdout
    lines = strip_ansi(stdout).splitlines()

    echo_idx = next(
        (
            idx
            for idx, line in enumerate(lines)
            if "drunc.echo" in line and line.rstrip().endswith("pre_restart_mlt")
        ),
        None,
    )
    assert echo_idx is not None, (
        "Could not find drunc.echo marker 'pre_restart_mlt' in stdout."
    )

    post_restart_idx = next(
        (
            idx
            for idx, line in enumerate(lines)
            if idx > echo_idx
            and "drunc.echo" in line
            and line.rstrip().endswith("post_restart_mlt")
        ),
        None,
    )
    assert post_restart_idx is not None, (
        "Could not find drunc.echo marker 'post_restart_mlt' in stdout."
    )

    restart_lines = lines[echo_idx + 1 : post_restart_idx]
    restart_text = "\n".join(restart_lines)

    restart_request_match = re.search(
        r"process_manager restarting \['mlt'\] in session",
        restart_text,
    )
    assert restart_request_match is not None, (
        "Did not find the mlt restart request log line between restart markers."
    )

    #! Reinsert this in the future, but this log-based thing is super janky
    # graceful_termination_match = re.search(
    #     r"Remote process .*?terminated gracefully following SIGQUIT signal\.",
    #     restart_text[restart_request_match.end() :],
    #     re.DOTALL,
    # )
    # assert graceful_termination_match is not None, (
    #     "Did not find the graceful termination log line for mlt after restart request."
    # )

    # exit_code_search_text = restart_text[
    #     restart_request_match.end() + graceful_termination_match.end() :
    # ]
    # exit_code_match = re.search(
    #     r"Process 'mlt'.*?process exited\s+with exit code 0",
    #     exit_code_search_text,
    #     re.DOTALL,
    # )
    # assert exit_code_match is not None, (
    #     "Did not find the mlt exit-code log line after graceful termination."
    # )

    # booted_search_text = exit_code_search_text[exit_code_match.end() :]
    # booted_match = re.search(
    #     r"Booted 'mlt'.*?with UUID\s+([^\s\n]+)",
    #     booted_search_text,
    #     re.DOTALL,
    # )
    # assert booted_match is not None, (
    #     "Did not find the mlt boot log line after the restart exit log."
    # )

    # booted_uuid = booted_match.group(1)
    # assert UUID_RE.match(booted_uuid), (
    #     f"Expected the mlt boot log to contain a UUID, got: {booted_uuid}"
    # )


def test_kill_removes_mlt_from_ps_table(run_dunerc) -> None:
    """Checks that killing mlt removes it from the subsequent ps table."""
    stdout = run_dunerc.completed_process.stdout

    ps_before_kill = get_ps_table_after_echo(stdout, "pre_kill_mlt")
    ps_after_kill = get_ps_table_after_echo(stdout, "post_kill_mlt")

    mlt_before_kill = [
        row for row in ps_before_kill if row["friendly_name"].strip() == "mlt"
    ]
    mlt_after_kill = [
        row for row in ps_after_kill if row["friendly_name"].strip() == "mlt"
    ]

    assert mlt_before_kill, (
        "Expected to find 'mlt' in ps table before kill, but it was missing."
    )
    assert not mlt_after_kill, (
        "Expected 'mlt' to be absent from ps table after kill, but it is still present."
    )


def test_mlt_recovers_after_kill(run_dunerc) -> None:
    """Checks that mlt is present again after the recovery restart sequence."""
    stdout = run_dunerc.completed_process.stdout

    ps_after_recovery = get_ps_table_after_echo(stdout, "ps_after_recovery")

    mlt_after_recovery = [
        row for row in ps_after_recovery if row["friendly_name"].strip() == "mlt"
    ]
    assert mlt_after_recovery, (
        "Expected 'mlt' to be present in ps table after recovery, but it was missing."
    )


def test_nanorc_success(run_dunerc):
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

    # Check that nanorc completed correctly
    assert run_dunerc.completed_process.returncode == 0


def test_log_files(run_dunerc):
    """Checks that expected process-manager log files exist and are free of errors."""
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
                logname
                for logname in run_dunerc.log_files
                if "process_manager" in str(logname)
            ],
            True,
            True,
            ignored_logfile_problems,
        )
