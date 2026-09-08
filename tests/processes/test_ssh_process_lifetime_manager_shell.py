import getpass
import os
from pathlib import Path

from tests.processes.test_ssh_process_lifetime_manager_common import (
    boot_processes_and_kill_individually,
    boot_processes_and_terminate_all_different_role_deep_nested,
    boot_processes_and_terminate_all_different_role_flat,
    boot_processes_and_terminate_all_same_role,
)


def test_ssh_process_execution_directory_access_check_shell(
    tmp_path, ssh_manager_shell
):
    """The write-access preflight should work through the real SSH shell path."""

    platform = os.uname().sysname.lower()
    is_macos = "darwin" in platform
    env = os.environ.copy()
    env.pop("DISPLAY", None)
    ssh_arguments = ssh_manager_shell._build_ssh_arguments(
        "localhost", f"{getpass.getuser()}@localhost"
    )

    ssh_manager_shell._check_process_execution_directory_access(
        ssh_arguments, str(tmp_path), env, is_macos
    )

    assert not (tmp_path / ".write_test").exists()


def test_ssh_multi_process_lifecycle_shell(ssh_manager_shell):
    """
    Test lifecycle of 3 concurrent SSH processes using shell.

    Executes 3 processes via SSH, verifies log output, terminates all
    processes, and confirms complete cleanup.
    """
    boot_processes_and_kill_individually(ssh_manager_shell, Path(__file__))


def test_ssh_terminate_all_same_role_shell(ssh_manager_shell):
    """
    Test batch termination of processes sharing the same role using shell.

    Executes 3 processes with identical roles via SSH, verifies log output,
    terminates all processes simultaneously, and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_same_role(ssh_manager_shell, Path(__file__))


def test_ssh_terminate_all_different_role_flat_shell(
    ssh_manager_shell, process_configs_flat
):
    """
    Test priority-based termination of processes with different roles (flat) using shell.

    Executes processes with varying role priorities via SSH, verifies log output,
    terminates all processes using role-based shutdown, verifies termination order,
    and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_different_role_flat(
        ssh_manager_shell, Path(__file__), process_configs_flat
    )


def test_ssh_terminate_all_different_role_deep_nested_shell(
    ssh_manager_shell, process_configs_deep_nested
):
    """
    Test role classification and priority-based termination for deeply nested processes.

    Exercises role classification for applications at arbitrary depth under "0." prefix,
    and verifies they terminate before infrastructure-applications processes.
    """
    boot_processes_and_terminate_all_different_role_deep_nested(
        ssh_manager_shell, Path(__file__), process_configs_deep_nested
    )
