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

import integrationtest.data_classes as idc
import integrationtest.utility_functions as utility_functions
import pytest
from daqconf.utils import find_free_port
from integ_test_utils import (
    require_drunc,
    strip_ansi,
)

# Reuse the pm_pms_test command sequence and the checks that don't depend on the
# process manager running locally (its log files live inside the Pod, and it is
# not reachable through localhost).
from pm_pms_test import (
    daq_session_name,
    dunerc_commands,
    test_boot_pm,
    test_boot_pms,
    test_flush,
    test_kill_removes_mlt_from_ps_table,
    test_mlt_recovers_after_kill,
    test_restart_mlt_logs_pm,
    test_restart_mlt_logs_pms,
    test_root_controller_logs,
    test_terminate,
    test_unknown_log_command,
    test_wait_command_duration_from_logs,
)

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
