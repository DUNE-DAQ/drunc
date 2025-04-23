import getpass
import logging
import signal

import click
import conffwk
from rich import print
from rich.logging import RichHandler
from sh import Command

from drunc.process_manager.oks_parser import collect_apps
from drunc.process_manager.ssh_process_manager import on_parent_exit
from drunc_core.utils.utils import (
    log_levels,
)

kDefaultAuth='default'
kPublicKeyAuth='publickey'
kKerberosAuth='gssapi-with-mic'

def test_host_connection(host: str, preferred_auth:str=kDefaultAuth) -> bool:
    ssh = Command("/usr/bin/ssh")

    print(f"[blue]{host}[/blue] \[{preferred_auth}]: ", end='')

    user_host = f"{getpass.getuser()}@{host}"
    ssh_args = [
        user_host,
        "-tt",
        "-o StrictHostKeyChecking=no",
    ]+([f"-o PreferredAuthentications={preferred_auth} "] if preferred_auth!=kDefaultAuth else [])+[
        f'echo "{user_host} established SSH successfully";',
    ]
    logging.debug(f"SSH command: /usr/bin/ssh {' '.join(ssh_args)}")

    try:
        ssh(
            *ssh_args,
            _bg=False,
            _bg_exc=False,
            _new_session=True,
            _preexec_fn=on_parent_exit(signal.SIGTERM),
            _err_to_out=True,
        )
        print(":white_check_mark:")
    except Exception as e:
        print(":x:")

        # print(f"Failed to SSH onto host [red]{user_host}[/red]")
        # print(e)
        return e

    return True

def test_session_ssh_connections(configuration: str, session_name: str, log_level: str, preferred_auth=kDefaultAuth):

    # log = logging.getLogger()
    db = conffwk.Configuration(f"oksconflibs:{configuration}")
    session_dal = db.get_dal(class_name="Session", uid=session_name)

    hosts = set()

    apps = collect_apps(
        config_filename=configuration,
        session_name=session_name,
        db=db,
        session_obj=session_dal,
        segment_obj=session_dal.segment,
        env={},
        tree_prefix=[],
    )

    for app in apps:
        hosts.add(app["host"])

    for host in hosts:
        test_host_connection(host, preferred_auth)
    print()


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="WARNING",
    help="Set the log level",
)
def main(log_level :  str):
    FORMAT = "%(message)s"
    logging.basicConfig(
        level=logging.WARNING, format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
    )
    logging.getLogger('sh').setLevel(log_level)

@main.command()
@click.argument("configuration", type=str, nargs=1)
@click.argument("session", type=str, nargs=1)
def check_session(configuration: str, session: str) -> None:
    """The script validates the ability to SSH onto all of the hosts required by the configuration <configuration> session <session> applications."""
    auths = [kDefaultAuth, kPublicKeyAuth, kKerberosAuth]
    results = {}
    for auth in auths:

        print('-'*80)
        print(f"Testing SSH connection to '{session}' host(s) " + (f"enforcing '{auth}' authentication" if auth != kDefaultAuth else "with default authentication"))
        print()

        results[auth] = test_session_ssh_connections(configuration, session, auth)
    print()

    print(results)

    print()

@main.command()
@click.argument("host", type=str, nargs=1)
def check_host(host):

    auths = [kDefaultAuth, kPublicKeyAuth, kKerberosAuth]

    print('-'*80)
    print(f"Testing SSH connection to '{host}' with {', '.join(auths)} authentications")
    print()

    results = {}
    for auth in auths:

        results[auth] = test_host_connection(host, auth)

    print()

    print(results)

    print()
