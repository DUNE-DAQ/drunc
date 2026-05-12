from pathlib import Path

import pytest

from tests.processes.test_ssh_process_lifetime_manager_common import (
    boot_processes_and_kill_individually,
    boot_processes_and_terminate_all_different_role_deep_nested,
    boot_processes_and_terminate_all_different_role_flat,
    boot_processes_and_terminate_all_same_role,
)


@pytest.mark.paramiko
def test_ssh_multi_process_lifecycle_paramiko(ssh_manager_paramiko):
    """
    Test lifecycle of 3 concurrent SSH processes using Paramiko.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    boot_processes_and_kill_individually(ssh_manager_paramiko, Path(__file__))


@pytest.mark.paramiko
def test_ssh_terminate_all_same_role_paramiko(ssh_manager_paramiko):
    """
    Test batch termination of processes sharing the same role using Paramiko.

    Executes 3 processes with identical roles via SSH, verifies log output,
    terminates all processes simultaneously, and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_same_role(ssh_manager_paramiko, Path(__file__))


@pytest.mark.paramiko
def test_ssh_terminate_all_different_role_flat_paramiko(
    ssh_manager_paramiko, process_configs_flat
):
    """
    Test priority-based termination of processes with different roles (flat) using Paramiko.

    Executes processes with varying role priorities via SSH, verifies log output,
    terminates all processes using role-based shutdown, verifies termination order,
    and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_different_role_flat(
        ssh_manager_paramiko, Path(__file__), process_configs_flat
    )


@pytest.mark.paramiko
def test_ssh_terminate_all_different_role_deep_nested_paramiko(
    ssh_manager_paramiko, process_configs_deep_nested
):
    """
    Test role classification and priority-based termination for deeply nested processes using Paramiko.

    Exercises role classification for applications at arbitrary depth under "0." prefix,
    and verifies they terminate before infrastructure-applications processes.
    """
    boot_processes_and_terminate_all_different_role_deep_nested(
        ssh_manager_paramiko, Path(__file__), process_configs_deep_nested
    )
