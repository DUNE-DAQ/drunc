import getpass
import signal

import click
import conffwk
from daqpytools.logging import logging_log_levels
from sh import Command

from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.ssh_process_manager_paramiko_client import on_parent_exit
from drunc.utils.utils import get_logger


def validate_ssh_connection(configuration: str, session_name: str, log_level: str):
    log = get_logger("validate_ssh_connection", rich_handler=True)

    db = conffwk.Configuration(f"oksconflibs:{configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)

    hosts = set()

    apps = collect_apps(
        config_filename=configuration,
        session_name=session_name,
        session_dal_obj=session_dal,
        segment_obj=session_dal.segment,
        env={},
        tree_prefix=[],
    )

    for app in apps:
        hosts.add(app["host"])

    log.info(f"Validating SSH connection to {len(hosts)} host(s)")

    ssh = Command("/usr/bin/ssh")

    for host in hosts:
        log.info(f"Trying to SSH onto host [green]{host}[/green]")

        user_host = f"{getpass.getuser()}@{host}"
        ssh_args = [
            user_host,
            "-tt",
            "-o StrictHostKeyChecking=no",
            f'echo "{user_host} established SSH successfully";',
        ]
        log.debug(f"SSH command: /usr/bin/ssh {' '.join(ssh_args)}")

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
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def main(configuration: str, session: str, log_level: str) -> None:
    """The script validates the ability to SSH onto all of the hosts required by the configuration <configuration> session <session> applications."""
    validate_ssh_connection(configuration, session, log_level)
