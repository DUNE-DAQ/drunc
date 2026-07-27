import os
import re
from dataclasses import dataclass, field

import integrationtest.data_classes as data_classes
import integrationtest.log_file_checks as log_file_checks
import pytest
from integ_test_utils import (
    check_execution_report_success,
    check_status_table_states,
    check_status_table_substates,
    get_execution_report_after_echo,
    get_run_info_after_echo,
    get_status_table_after_echo,
    strip_ansi,
)

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
    "SSH_SHELL_process_manager": [
        "was terminated unexpectedly through the remote pid by a SIGKILL",
    ],
    "connectivity-service": [
        "errorlog: -",
    ],
}

conf_dict = data_classes.integtest_params_for_generated_dunedaq_config()
conf_dict.object_databases = ["config/daqsystemtest/integrationtest-objects.data.xml"]
conf_dict.dro_map_config.n_streams = number_of_data_producers
conf_dict.op_env = "integtest"
conf_dict.session = "minimal"
conf_dict.tpg_enabled = False

# For testing, allow drunc to manage ConnectivityService (default is False, integrationtest manages Connectivity Service)
conf_dict.drunc_connsvc = True
# For testing, specify connectivity service port (default is 0, a random port is chosen for the Connectivity Service)
# conf_dict.connsvc_port = 12345

conf_dict.config_substitutions.append(
    data_classes.attribute_substitution(
        obj_id="random-tc-generator",
        obj_class="RandomTCMakerConf",
        updates={"trigger_rate_hz": 1},
    )
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

confgen_arguments = {"MinimalSystem": conf_dict}


# ── FSM command dataclass ──────────────────────────────────────────────────────


@dataclass
class FsmCommandParams:
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


_FSM_COMMANDS = [
    FsmCommandParams("test_conf", "conf", "configured"),
    FsmCommandParams(
        "test_start", "start", "ready", command_args=["--run-number", "1"], run_number=1
    ),
    FsmCommandParams("test_enable_triggers", "enable-triggers", "running"),
    FsmCommandParams("test_disable_triggers", "disable-triggers", "ready"),
    FsmCommandParams("test_drain_dataflow", "drain-dataflow", "dataflow_drained"),
    FsmCommandParams(
        "test_stop_trigger_sources", "stop-trigger-sources", "trigger_sources_stopped"
    ),
    FsmCommandParams("test_stop", "stop", "configured"),
    FsmCommandParams("test_scrap", "scrap", "initial"),
]

_FSM_SEQUENCES = {
    "test_start_run": FsmCommandParams(
        "test_start_run",
        "start-run",
        "running",
        command_args=["--run-number", "2"],
        run_number=2,
    ),
    "test_shutdown": FsmCommandParams("test_shutdown", "shutdown", "initial"),
    "test_stop_run": FsmCommandParams("test_stop_run", "stop-run", "configured"),
}

# ── Command list ───────────────────────────────────────────────────────────────

dunerc_command_list = (
    """
boot
echo post_boot
status
echo post_boot_done
"""
    + "".join(p.to_command_block() for p in _FSM_COMMANDS)
    + _FSM_SEQUENCES["test_start_run"].to_command_block()
    + _FSM_SEQUENCES["test_stop_run"].to_command_block()
    + "start-run --run-number 3"
    + _FSM_SEQUENCES["test_stop_run"].to_command_block()
    + "\nterminate"
).split()

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def boot_status_table(run_dunerc):
    """Parse and cache the status table produced immediately after boot.

    Scoped to the module so every test in this file can compare against the
    same baseline without re-parsing stdout each time.
    """
    lines = strip_ansi(run_dunerc.completed_processes["drunc"].stdout).splitlines()
    return get_status_table_after_echo(lines, "post_boot")


# ── Helpers ────────────────────────────────────────────────────────────────────


def _check_command(
    lines: list[str],
    boot_status_table: list[dict[str, str]],
    params: FsmCommandParams,
) -> None:
    """Shared assertion logic for a drunc FSM command.

    Checks:
    - Execution report names match boot table, all rows successful.
    - Post-command status table has expected state/substates.
    - Run number if specified.
    """
    exec_report = get_execution_report_after_echo(lines, params.marker)
    assert exec_report, f"No execution report found after '{params.marker}' marker."

    boot_names = {row["name"] for row in boot_status_table}
    report_names = {row["name"] for row in exec_report}
    assert report_names == boot_names, (
        f"Execution report names do not match boot status table names.\n"
        f"  Only in report:     {report_names - boot_names}\n"
        f"  Only in boot table: {boot_names - report_names}"
    )
    check_execution_report_success(exec_report)

    status_table = get_status_table_after_echo(lines, params.done_marker)
    assert status_table, f"No status table found after '{params.done_marker}' marker."
    check_status_table_states(status_table, expected_state=params.expected_state)
    check_status_table_substates(
        status_table,
        controller_substate=params.expected_state,
        non_controller_substate=params.non_controller_substate,
    )

    if params.run_number is not None:
        run_info = get_run_info_after_echo(lines, params.done_marker)
        assert run_info, f"No Run Info table found after '{params.done_marker}' marker."
        assert run_info["Run number"] == str(params.run_number), (
            f"Expected run number '{params.run_number}', got '{run_info['Run number']}'."
        )


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_dunerc_success(run_dunerc) -> None:
    """Checks that the drunc integration command sequence completes successfully."""
    current_test = os.environ.get("PYTEST_CURRENT_TEST")
    match_obj = re.search(r".*\[(.+)-run_.*rc.*\d].*", current_test)
    if match_obj:
        current_test = match_obj.group(1)
    banner_line = re.sub(".", "=", current_test)
    print(banner_line)
    print(current_test)
    print(banner_line)

    assert run_dunerc.completed_processes["drunc"].returncode == 0


def test_log_files(run_dunerc) -> None:
    """Checks that expected process-manager log files exist and are free of errors."""
    for app_exension in ["_df-01", "_dfo", "_mlt", "_ru"]:
        assert any(
            f"{run_dunerc.daq_session_name}{app_exension}" in str(logname)
            for logname in run_dunerc.log_files
        ), f"Expected log file with extension '{app_exension}' not found."

    if check_for_logfile_errors:
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


@pytest.mark.parametrize("params", _FSM_COMMANDS, ids=lambda p: p.marker)
def test_fsm_command(run_dunerc, boot_status_table, params: FsmCommandParams) -> None:
    """Checks that each FSM command executes successfully and transitions all processes to the expected state."""
    lines = strip_ansi(run_dunerc.completed_processes["drunc"].stdout).splitlines()
    _check_command(lines, boot_status_table, params)
