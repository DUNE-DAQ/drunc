"""
SSH validator application.

This script validates the ability to SSH onto all of the hosts required by the
configuration <configuration> session <session> applications.
"""

import getpass
import logging
from pathlib import Path

import click
import conffwk

from drunc.process_manager.oks_parser import collect_apps
from drunc.processes.ssh_process_lifetime_manager import SSHProcessLifetimeManager
from drunc.utils.utils import create_logger_handler, get_logger, log_levels


def validate_ssh_connection(
    configuration: str, session_name: str, log_level: str
) -> None:
    """
    Validate SSH connection to all hosts required by the applications in the given
    configuration and session.

    Args:
        configuration (str): The configuration file name.
        session_name (str): The session name.
        log_level (str): The log level to use.

    Returns:
        None

    Raises:
        None
    """

    # Set up logging
    log: logging.Logger = get_logger("validate_ssh_connection")
    create_logger_handler(rich_handler=True)
    log.setLevel(log_levels[log_level.upper()])

    # Validate that the ssh configuration has been defined
    ssh_config_path: Path = Path("~/.ssh/config").expanduser()
    if not ssh_config_path.exists():
        raise FileNotFoundError(
            f"SSH configuration file not found at {ssh_config_path}. This is required "
            "for using the SSH process manager."
        )

    # Load configuration and session
    db = conffwk.Configuration(f"oksconflibs:{configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)

    # Collect unique hosts from applications
    hosts = set()

    # Parse the configuration to collect applications. This will determine what hosts
    # ssh access is required for.
    apps = collect_apps(
        config_filename=configuration,
        session_name=session_name,
        db=db,
        session_obj=session_dal,
        segment_obj=session_dal.segment,
        env={},
        tree_prefix=[],
    )

    # Gather unique hosts
    for app in apps:
        hosts.add(app["host"])

    log.info(f"Validating SSH connection to {len(hosts)} host(s)")

    # Attempt SSH connection to each host
    ssh_manager = SSHProcessLifetimeManager(
        disable_host_key_check=True,
        logger=log,
    )
    for host in hosts:
        log.info(f"Trying to SSH onto host [green]{host}[/green]")
        try:
            user = getpass.getuser()
            ssh_manager.validate_host_connection(host=host)
            log.info(
                f"SSH connection established successfully for user {user} on host [green]{host}[/green]"
            )
        except Exception as e:
            log.error(
                f"SSH connection failed for user {user} on host [red]{host}[/red]"
            )
            log.exception(e)


@click.command(
    short_help="Validate SSH connectivity for a configuration/session.",
    help=(
        "Validate the ability to SSH onto all of the hosts required by the "
        "configuration <configuration> session <session> applications.\n\n"
        "After running the command, you will see a list of hosts with either a check "
        "mark or a cross mark.\n\n"
        "Arguments:\n\n"
        "  configuration: The configuration name or identifier used by the oksconflibs"
        "backend (e.g. 'myconfig'). This determines which configuration file is loaded "
        "to collect applications and hosts.\n\n"
        "  session: The session name defined within the given configuration (e.g. "
        "'default'). The command validates SSH access for hosts required by "
        "applications in this session.\n\n"
        "Example usage: drunc ssh-validator <configuration> <session>\n"
    ),
)
@click.argument("configuration", type=str, nargs=1)
@click.argument("session", type=str, nargs=1)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(list(log_levels.keys()), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def main(configuration: str, session: str, log_level: str) -> None:
    """
    Entrypoint for the ssh-validator CLI command.


    Args:
        configuration (str): The configuration file name.
        session (str): The session name.
        log_level (str): The log level to use.
    """
    validate_ssh_connection(configuration, session, log_level)
