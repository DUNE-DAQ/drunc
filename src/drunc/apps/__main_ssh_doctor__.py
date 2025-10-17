import logging

import click
import conffwk
from rich import print
from rich.logging import RichHandler

from drunc.process_manager.oks_parser import collect_apps
from drunc.processes.ssh_process_lifetime_manager import SSHProcessLifetimeManager

# from drunc.process_manager.ssh_process_manager import on_parent_exit
from drunc.utils.utils import log_levels

kPublicKeyAuth = "publickey"
kKerberosAuth = "gssapi-with-mic"
authentication_methods: list[str] = [kPublicKeyAuth, kKerberosAuth]


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

    ssh_manager = SSHProcessLifetimeManager(disable_host_key_check=True)

    try:
        ssh_manager.validate_host_connection(host=host, auth_methods=test_auth)
        return True
    except Exception as e:
        logging.error(f"Failed to validate SSH connection for {host}: {e}")
        return False


def test_session_ssh_connections(configuration: str, session_name: str, test_auth: str):
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
        test_host_connection(host, test_auth)
    print()


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(log_levels.keys(), case_sensitive=False),
    default="WARNING",
    help="Set the log level",
)
def main(log_level: str):
    """
    Validate the ability to SSH onto all of the hosts required by the configuration
    <configuration> session <session> applications.\n\n

    This command groups SSH connection validation using commands as:\n
        drunc ssh-doctor check-session my_config.oks my_session\n
        drunc ssh-doctor check-host myhost.example.com\n
    """

    FORMAT = "%(message)s"
    logging.basicConfig(
        level=logging.WARNING, format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
    )
    logging.getLogger("sh").setLevel(log_level)


@main.command()
@click.argument("configuration", type=str, nargs=1)
@click.argument("session", type=str, nargs=1)
def check_session(configuration: str, session: str) -> None:
    """
    Validate SSH connectivity for a configuration/session.

    Validates the ability to SSH onto all of the hosts required by the
    configuration <configuration> session <session> applications.

    Args:
        configuration (str): The configuration name or identifier used by the
            oksconflibs backend (e.g. 'myconfig'). This determines which configuration
            file is loaded to collect applications and hosts.

        session (str): The session name defined within the given configuration (e.g.
            'local-1x1-config'). The command validates SSH access for hosts required by
            applications in this session.
    """

    results = {}
    for authentication_method in authentication_methods:
        print("-" * 80)
        print(
            f"Testing SSH connection to '{session}' host(s) "
            f"enforcing '{authentication_method}' authentication"
        )
        print()

        results[authentication_method] = test_session_ssh_connections(
            configuration, session, authentication_method
        )
    print()

    print(results)

    print()


@main.command()
@click.argument("host", type=str, nargs=1)
def check_host(host):
    """
    Validate SSH connectivity to a specific host.

    Args:\n
        host (str): The hostname or IP address of the target host.

    Returns:
        None

    Raises:
        None
    """

    print("-" * 80)
    print(
        f"Testing SSH connection to '{host}' with {', '.join(authentication_methods)} authentications"
    )
    print()

    results = {}
    for authentication_method in authentication_methods:
        results[authentication_method] = test_host_connection(
            host, authentication_method
        )

    print()

    print(results)

    print()
