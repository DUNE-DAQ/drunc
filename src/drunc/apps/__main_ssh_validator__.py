import getpass
import logging
import signal
from pathlib import Path

import click
import conffwk
from sh import Command

from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.ssh_process_manager import on_parent_exit
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
    ssh = Command("/usr/bin/ssh")
    for host in hosts:
        log.info(f"Trying to SSH onto host [green]{host}[/green]")

        # Construct SSH command
        user_host = f"{getpass.getuser()}@{host}"
        ssh_args = [
            user_host,
            "-tt",
            "-o StrictHostKeyChecking=no",
            f'echo "{user_host} established SSH successfully";',
        ]
        log.debug(f"SSH command: /usr/bin/ssh {' '.join(ssh_args)}")

        # Attempt SSH connection
        try:
            ssh(
                *ssh_args,
                _bg=False,
                _bg_exc=False,
                _new_session=True,
                _preexec_fn=on_parent_exit(signal.SIGTERM),
                _err_to_out=True,
            )
            log.info(
                f"SSH connection established successfully on host [green]{user_host}[/green]"
            )
        except Exception as e:
            log.error(f"Failed to SSH onto host [red]{user_host}[/red]")
            log.exception(e)


@click.command()
@click.argument("configuration", type=str, nargs=1)
@click.argument("session", type=str, nargs=1)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def main(configuration: str, session: str, log_level: str) -> None:
    """
    The script validates the ability to SSH onto all of the hosts required by the
    configuration <configuration> session <session> applications.

    After running the command, you will see a list of hosts with either a check mark or
    a cross mark:

    Example usage:
        drunc ssh-validator <configuration> <session>

    Args:
        configuration (str): The configuration file name.
        session (str): The session name.
        log_level (str): The log level to use.

    Returns:
        None

    Raises:
        None
    """
    validate_ssh_connection(configuration, session, log_level)
