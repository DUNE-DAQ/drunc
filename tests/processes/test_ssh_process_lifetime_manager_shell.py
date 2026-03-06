from pathlib import Path

from tests.processes.test_ssh_process_lifetime_manager_common import (
    boot_processes_and_kill_individually,
    boot_processes_and_terminate_all_different_role,
    boot_processes_and_terminate_all_same_role,
)


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


def test_ssh_terminate_all_different_role_shell(ssh_manager_shell):
    """
    Test priority-based termination of processes with different roles using shell.

    Executes processes with varying role priorities via SSH, verifies log output,
    terminates all processes using role-based shutdown, verifies termination order,
    and confirms complete cleanup.
    """
    boot_processes_and_terminate_all_different_role(ssh_manager_shell, Path(__file__))
