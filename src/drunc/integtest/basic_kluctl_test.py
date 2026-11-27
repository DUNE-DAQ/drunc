# The goal of this test is to deploy the k8s process manager as a Pod with kluctl,
# and run the pm_pms_test process manager shell sequence against it.
#
# The kluctl project (.kluctl.yaml and deployment.yaml pointing at the
# process-manager-ci manifest) and the Kind cluster are expected to be set up
# outside of this test, e.g. by the drunc-pm-pod-k8s CI job. The test is skipped
# unless the following environment variables are set:
#   DRUNC_KLUCTL_PROJECT     path to the kluctl project directory
#   DRUNC_PM_POD_IMAGE       container image used for the process manager Pod
#   DRUNC_PM_POD_HOST_MOUNT  host path mounted into the Pod (must contain the workarea)
# Optional overrides:
#   DRUNC_KLUCTL_TARGET      kluctl target (default: ci)
#   DRUNC_PM_POD_WORKAREA    workarea used inside the Pod (default: $DBT_AREA_ROOT)
#   DRUNC_PM_POD_CONFIG      process manager configuration (default: the workarea's k8s.json)
#   DRUNC_PM_POD_ADDRESS     address the Pod is reachable at (default: $KIND_IP or the hostname)

import functools
import getpass
import os
import shlex
import shutil
import socket
import re

import integrationtest.data_classes as idc
import integrationtest.utility_functions as utility_functions
import pytest
from daqconf.utils import find_free_port
from integ_test_utils import (
    require_drunc,
    strip_ansi,
    require_line_containing,
)

from integ_test_utils import (
    _parse_table_from_index,
    assert_contains_between_markers,
    assert_match_contains_uuid,
    assert_process_presence,
    assert_rows_have_valid_uuids,
    find_line_index,
    get_column_for_friendly_name,
    get_lines_between_markers,
    get_ps_table_after_echo,
    get_text_between_echo_markers,
    require_echo_marker_index,
    
    require_pattern_match,
    require_pattern_match_index,
)

# Reuse the pm_pms_test command sequence and the checks that don't depend on the
# process manager running locally (its log files live inside the Pod, and it is
# not reachable through localhost).
from pm_pms_test import (
    daq_session_name,
    UUID_RE,
    test_boot_pms,
    test_unknown_log_command,
    test_wait_command_duration_from_logs,
)

dunerc_commands = f"""
    echo pre_boot
    echo --server  pre_boot
    ps
    boot <config_data_file> local-1x1-config {daq_session_name}
    wait 15
    echo post_boot
    echo --server  post_boot
    ps

    echo test_logs
    logs --name unknown
    logs --name root-controller --how-far 5
    logs --name mlt --how-far 5
    echo test_logs_done

    echo test_wait
    wait 10
    echo test_wait_done

    echo test_terminate
    echo --server  test_terminate
    terminate
    echo test_terminate_done
    echo --server  test_terminate_done

    """.split()

print = functools.partial(print, flush=True)  # always flush print() output

pytest_plugins = "integrationtest.integrationtest_drunc"

kluctl_project = os.environ.get("DRUNC_KLUCTL_PROJECT")
kluctl_target = os.environ.get("DRUNC_KLUCTL_TARGET", "ci")
pm_image = os.environ.get("DRUNC_PM_POD_IMAGE")
pm_host_mount = os.environ.get("DRUNC_PM_POD_HOST_MOUNT")
pm_workarea = os.environ.get(
    "DRUNC_PM_POD_WORKAREA", os.environ.get("DBT_AREA_ROOT", "")
)
pm_config = os.environ.get(
    "DRUNC_PM_POD_CONFIG",
    os.path.join(
        pm_workarea, "pythoncode/drunc/src/drunc/data/process_manager/k8s.json"
    ),
)
pm_address = os.environ.get(
    "DRUNC_PM_POD_ADDRESS", os.environ.get("KIND_IP", socket.gethostname())
)
pm_namespace = "kube-services"
pm_deployment = "deployment/drunc-process-manager"

missing_requirements = [
    name
    for name, value in (
        ("DRUNC_KLUCTL_PROJECT", kluctl_project),
        ("DRUNC_PM_POD_IMAGE", pm_image),
        ("DRUNC_PM_POD_HOST_MOUNT", pm_host_mount),
        ("DRUNC_PM_POD_WORKAREA or DBT_AREA_ROOT", pm_workarea),
        ("kluctl", shutil.which("kluctl")),
        ("kubectl", shutil.which("kubectl")),
    )
    if not value
]

pytestmark = [
    require_drunc,
    pytest.mark.skipif(
        bool(missing_requirements),
        reason="kluctl process manager deployment not available, missing: "
        + ", ".join(missing_requirements),
    ),
]

conf_dict = idc.integtest_params_for_predefined_dunedaq_config()
conf_dict.predefined_config_db = (
    os.environ.get("DAQSYSTEMTEST_SHARE", "")
    + "/config/daqsystemtest/example-configs.data.xml"
)
conf_dict.daq_session_name = daq_session_name

confgen_arguments = {"SmallFootprint": conf_dict}

# The process manager Pod uses hostNetwork, so this port is opened on the Kind node
pm_port = find_free_port(50020, 52000)

# Deploy with kluctl, wait for the rollout, then stream the Pod logs so the
# integtest framework can search them. The harness stops this process with
# SIGTERM at the end of the test, so stop streaming and exit cleanly on it.
kluctl_deploy = shlex.join(
    [
        "kluctl",
        "deploy",
        "-t",
        kluctl_target,
        "--yes",
        "-a",
        f"port={pm_port}",
        "-a",
        f"image={pm_image}",
        "-a",
        f"workarea={pm_workarea}",
        "-a",
        f"host_mount={pm_host_mount}",
        "-a",
        f"pm_config={pm_config}",
    ]
)
pm_deploy_script = f"""
set -e
cd {shlex.quote(kluctl_project or ".")}
{kluctl_deploy}
kubectl -n {pm_namespace} rollout status {pm_deployment} --timeout=300s
kubectl -n {pm_namespace} logs -f {pm_deployment} &
logs_pid=$!
trap 'kill $logs_pid 2>/dev/null; exit 0' TERM
wait $logs_pid
"""

pmapp = idc.DAQControlApplication(
    "pm",
    ["bash", "-c", pm_deploy_script],
    idc.KeyPhraseWaitParameters(
        search_phrase="communicating through",
        timeout_waiting_for_first_msg=300,
        wait_time_after_last_msg=300,
    ),
)

pmshellapp = idc.DAQControlApplication(
    "pmshell",
    ["drunc-process-manager-shell", f"grpc://{pm_address}:{pm_port}"],
    idc.KeyPhraseWaitParameters(search_phrase="Ready"),
)

cmd_set = idc.DAQCommandSet(
    "pmshell",
    dunerc_commands,
    idc.EchoCommandWaitParameters(),
)

dsi = idc.DAQSessionIngredients([pmapp, pmshellapp], [cmd_set])

# Declare the special variable that tells the integrationtest infrastructure what we want to run
daq_session_ingredients = {"KluctlPMSession": dsi}


def test_dunerc_success(run_dunerc, caplog) -> None:
    # checks for run control success, problems during pytest setup, etc.
    utility_functions.basic_checks(run_dunerc, caplog, print_test_name=False)


def test_connections(run_dunerc) -> None:
    lines_pm = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()
    lines_pms = strip_ansi(
        run_dunerc.completed_processes["pmshell"].stdout
    ).splitlines()

    user_name = getpass.getuser()
    pm_connect = f"{user_name} connected from process_manager_shell"
    pms_connect = (
        f"{user_name} connected to the process manager through a "
        f"drunc-process-manager-shell via address {pm_address}:{pm_port}"
    )

    assert any(pm_connect in line for line in lines_pm), (
        f"Did not find '{pm_connect}' in the process manager output.\nOutput:\n"
        + "\n".join(lines_pm)
    )
    assert any(pms_connect in line for line in lines_pms), (
        f"Did not find '{pms_connect}' in the process manager shell output.\nOutput:\n"
        + "\n".join(lines_pms)
    )


def test_root_controller_logs(run_dunerc) -> None:
    """
    Verifies that:
    - the stdout contains a "root-controller logs" header line and a "root-controller end" footer line
    - there are exactly 5 lines between those two lines
    - among those 5 lines, the one from "drunc.controller.core.init_controller" ends with "Controller ready"
    """
    lines = run_dunerc.completed_processes["pmshell"].stdout.splitlines()

    # 1) Find the header/footer lines
    header_idx = require_line_containing(
        lines,
        "root-controller logs",
        error_message="Did not find the 'root-controller logs' header line in stdout.",
    )
    footer_idx = require_line_containing(
        lines,
        "root-controller end",
        error_message="Did not find the 'root-controller end' footer line in stdout.",
    )
    assert footer_idx > header_idx, "Footer appears before header in stdout."

    # 2) Check there are 6 lines between header and footer
    # one more than the ssh compared to k8s
    # not sure why
    between = lines[header_idx + 1 : footer_idx]
    assert len(between) == 6, (
        f"Expected exactly 6 lines between header and footer, found {len(between)}.\nBetween:\n"
        + "\n".join(between)
    )

    # 3) Check one of the init_controller line ends with "Controller ready"
    # Example line:
    # [2026/03/13 08:17:47 UTC] INFO ... drunc.controller.core.init_controller ... Controller ready
    init_controller_ready_re = re.compile(
        r"drunc\.controller\.core\.init_controller.*Controller ready\s*$"
    )

    matches = [line for line in between if init_controller_ready_re.search(line)]
    assert len(matches) >= 1, (
        "Did not find an init_controller line ending with 'Controller ready' within the 6 lines.\nBetween:\n"
        + "\n".join(between)
    )

def test_terminate(run_dunerc) -> None:
    """Test terminate by checking both pm and pms shells"""
    lines_pms = strip_ansi(
        run_dunerc.completed_processes["pmshell"].stdout
    ).splitlines()
    lines_pm = strip_ansi(run_dunerc.completed_processes["pm"].stdout).splitlines()

    pre_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate",
        error_message="Did not find the 'pre_boot' header line in stdout.",
    )
    post_boot_idx_pm = require_line_containing(
        lines_pm,
        "test_terminate_done",
        error_message="Did not find the 'post_boot' footer line in stdout.",
    )

    pre_boot_idx_pms = require_line_containing(
        lines_pms,
        "test_terminate",
        error_message="Did not find the 'pre_boot' header line in stdout.",
    )

    between_pm = lines_pm[pre_boot_idx_pm + 1 : post_boot_idx_pm]
    shutdown_re = "--- Termination of role 'root-controller' complete ---"
    assert any(shutdown_re in line for line in between_pm), (
        f"Did not find '{shutdown_re}' between pre_boot and post_boot.\nBetween:\n"
        + "\n".join(between_pm)
    )

    # TODO: This bit here is grabbing functions from the integ test utils. Maybe it can be better optimised?
    table_start_idx = find_line_index(
        lines_pms,
        lambda line: "Terminated process" in line,
        start_idx=pre_boot_idx_pms + 1,
    )

    assert table_start_idx is not None, "cannot fine terminated process table"

    #! AI SAYS THIS SHOULD BE PMS, SHOULD DOUBLE CHECK THIS
    terminated_table = _parse_table_from_index(lines_pm, table_start_idx, "ps")
    for row in terminated_table:
        assert UUID_RE.match(row["uuid"]), (
            f"Expected a valid UUID for process '{row['friendly_name']}', got '{row['uuid']}'"
        )



def test_boot_pm(run_dunerc) -> None:
    """Checks that boot starts in the pm. More lightweight, checks if root-controller boots"""
    
    """Shared boot-check logic: verifies markers/message, and optionally the root-controller
    boot message (pm) or a populated, UUID-valid ps table (pmshell)."""
    
    process_name = "pm"
    pre_marker = "pre_boot"
    post_marker = "post_boot"
    expected_message = "sent boot with arguments"

    
    lines = strip_ansi(run_dunerc.completed_processes[process_name].stdout).splitlines()

    if expected_message:
        assert_contains_between_markers(
            lines, pre_marker, post_marker, expected_message
        )
    else:
        get_lines_between_markers(lines, pre_marker, post_marker)

    check_root_controller_boot = (
        f"Drunc controller 'root-controller' is fully ready."
    )
    assert_contains_between_markers(
        lines, pre_marker, post_marker, check_root_controller_boot
    )

