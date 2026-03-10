"""
SSH Doctor Application
Validate the ability to SSH onto all of the hosts required by the configuration
<configuration> session <session> applications.
"""

import logging
import sys
from pathlib import Path

import click
import conffwk
from daqpytools.logging.levels import logging_log_levels

from drunc.process_manager.oks_parser import collect_apps
from drunc.processes.ssh_process_lifetime_manager_paramiko import (
    SSHProcessLifetimeManagerParamiko,
)

# from drunc.process_manager.ssh_process_manager import on_parent_exit
from drunc.utils.utils import get_logger

kPublicKeyAuth = "publickey"
kKerberosAuth = "gssapi-with-mic"
authentication_methods: list[str] = [kPublicKeyAuth, kKerberosAuth]
log = get_logger("ssh_doctor", rich_handler=True)


def test_host_connection(host: str, test_auth: str) -> bool:
    """
    Test SSH connection to a specific host using the given authentication method.

    Args:
        host (str): The hostname or IP address of the target host.
        test_auth (str): The authentication method to use ('publickey',
            'gssapi-with-mic', or None for both).

    Returns:
        bool: True if the SSH connection is successful, False otherwise.
    """

    ssh_manager = SSHProcessLifetimeManagerParamiko(disable_host_key_check=True)

    try:
        ssh_manager.validate_host_connection(host=host, auth_method=test_auth)
        return True
    except Exception as e:
        logging.error(f"Failed to validate SSH connection for {host}: {e}")
        return False


def parse_configuration(configuration: str, session_name: str) -> list[str]:
    """
    Parse configurations and collect unique hosts from applications in the given
    configuration and session.

    Args:
        configuration (str): The configuration file name.
        session_name (str): The session name defined within the given configuration (e.g.
            'local-1x1-config'). The command validates SSH access for hosts required by
            applications in this session.

    Returns:
        list[str]: A list of hostnames to test SSH connections to.
    """

    # Load configuration and session, collect applications
    if not Path(configuration).exists():
        log.error(f"Configuration file '{configuration}' does not exist.")
        sys.exit(1)

    db = conffwk.Configuration(f"oksconflibs:{configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)
    apps = collect_apps(
        config_filename=configuration,
        session_name=session_name,
        session_dal_obj=session_dal,
        segment_obj=session_dal.segment,
        env={},
        tree_prefix=[],
    )

    # Gather unique hosts
    hosts = set()
    for app in apps:
        hosts.add(app["host"])

    return sorted(hosts)


def print_results(results: dict[str, dict[str, bool]]) -> None:
    """
    Print the results of SSH connection tests.

    Args:
        results (dict): A dictionary containing the results of SSH connection tests.
            Formatted as {host: {auth_method: success_bool}}.

    Returns:
        None
    """
    for host, auth_methods in results.items():
        all_valid_connections = False
        if all(auth_methods.values()):
            all_valid_connections = True
        log.info(
            f"\tHost: {host} - Overall Status: {'[green]✔[/green]' if all_valid_connections else '[red]✘[/red]'}"
        )
        for auth_method, successful_connection in auth_methods.items():
            status = "[green]✔[/green]" if successful_connection else "[red]✘[/red]"
            if successful_connection:
                log.info(f"\t\t{auth_method} {status}")
            else:
                log.error(
                    f"\t{host if not all_valid_connections else ''} {auth_method} {status}"
                )
    return


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def main(log_level: str):
    """
    Validate the ability to SSH onto all of the hosts required by the configuration
    <configuration> session <session> applications.\n\n

    This command groups SSH connection validation using commands as:\n
        drunc ssh-doctor check-session my_config.oks my_session\n
        drunc ssh-doctor check-host myhost.example.com\n\n

    Set the log level to error to see only failed connections.\n
    """
    log.setLevel(logging_log_levels[log_level.upper()])


@main.command()
@click.argument("configuration", type=str, nargs=1)
@click.argument("session", type=str, nargs=1)
def check_session(configuration: str, session: str) -> None:
    """
    Validate SSH connectivity for a configuration/session.\n\n

    Validates the ability to SSH onto all of the hosts required by the
    configuration <configuration> session <session> applications.\n\n

    Args:\n
        configuration (str): The configuration file name.\n

        session (str): The session name defined within the given configuration (e.g.
            'local-1x1-config'). The command validates SSH access for hosts required by
            applications in this session.\n

    Returns:\n
        None

    Raises:\n
        None
    """

    # Determine the list of hosts from the configuration and session
    hosts: list[str] = parse_configuration(configuration, session)

    if hosts:
        log.info(f"Validating SSH connection to {hosts}")
    else:
        log.error(
            f"No hosts found in configuration '{configuration}' session '{session}'"
        )
        return

    # Test SSH connections for each host
    host_results: dict[str, dict[str, bool]] = {}  # host -> auth_method -> result
    for host in hosts:
        log.debug(
            f"Testing connection to '{host}' with {', '.join(authentication_methods)} "
            "authentications"
        )
        host_results[host] = {}
        for authentication_method in authentication_methods:
            host_results[host][authentication_method] = test_host_connection(
                host, authentication_method
            )

    print_results(host_results)
    return


@main.command()
@click.argument("host", type=str, nargs=1)
def check_host(host: str) -> None:
    """
    Validate SSH connectivity to a specific host.

    Args:\n
        host (str): The hostname or IP address of the target host.

    Returns:\n
        None

    Raises:\n
        None
    """

    log.info(
        f"Testing SSH connection to '{host}' with {', '.join(authentication_methods)} authentications"
    )

    # Test SSH connections for each authentication method
    host_results: dict[str, bool] = {}  # auth_method -> result
    for authentication_method in authentication_methods:
        host_results[authentication_method] = test_host_connection(
            host, authentication_method
        )

    # Format and print the results
    results: dict[str, dict[str, bool]] = {}  # host -> auth_method -> result
    results[host] = host_results
    print_results(results)
    return
