"""
Module for SSH configurator main application.

Sets up the SSH configurations based on provided parameters. Currently this configures
the SSH client configuration file (~/.ssh/config) to facilitate passwordless SSH
access to np0* hosts and localhost using public key authentication.
"""

import fnmatch
import getpass
from pathlib import Path

import click
import paramiko
from daqpytools.logging.levels import logging_log_levels
from jinja2 import Template

import drunc as _drunc
from drunc.utils.utils import get_logger

log = get_logger("ssh_configurator", rich_handler=True)

kPublicKeyAuth = "publickey"
kKerberosAuth = "gssapi-with-mic"
authentication_methods: list[str] = [kPublicKeyAuth, kKerberosAuth]

np0x_servers = [
    "np04-srv-001",
    "np04-srv-002",
    "np04-srv-003",
    "np04-srv-004",
    "np04-srv-005",
    "np04-srv-011",
    "np04-srv-012",
    "np04-srv-013",
    "np04-srv-014",
    "np04-srv-015",
    "np04-srv-016",
    "np04-srv-018",
    "np04-srv-019",
    "np04-srv-021",
    "np04-srv-022",
    "np04-srv-024",
    "np04-srv-025",
    "np04-srv-026",
    "np04-srv-028",
    "np04-srv-029",
    "np04-srv-030",
    "np04-srv-031",
    "np02-srv-002",
    "np02-srv-003",
    "np02-srv-004",
    "np02-srv-005",
]

hosts_to_access = np0x_servers + ["localhost"]


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(logging_log_levels.keys(), case_sensitive=False),
    default="INFO",
    help="Set the log level",
)
def main(log_level: str) -> None:
    """
    Configures SSH access for the specified hosts.

    This uses a template SSH configuration file to set up passwordless SSH access using
    both publickey and GSSAPI authentication methods for np0* hosts and localhost.

    Args:
        log_level (str): The log level to use for logging output.

    Returns:
        None
    """

    # Set the log level
    log.setLevel(logging_log_levels[log_level.upper()])

    # Determine SSH configuration file path
    ssh_configuration_path: Path = Path("~/.ssh/config").expanduser().resolve()
    populate_ssh_template: bool = False
    ssh_config_exists: bool = False

    if not ssh_configuration_path.exists():
        log.warning(
            "SSH configuration file not found. Will create a new one from template."
        )
        populate_ssh_template = True
    else:
        log.info(f"SSH configuration file found at {ssh_configuration_path}")
        log.info("Parsing existing SSH configuration file.")

        ssh_config = paramiko.SSHConfig()
        with ssh_configuration_path.open("r") as ssh_config_file:
            # Check if the file is empty - empty config files will get parsed as
            # containing the configuration for all hosts (with wildcard *)
            if ssh_config_file.read().strip() == "":
                log.warning(
                    "SSH configuration file is empty. Will populate from template."
                )
                populate_ssh_template = True
            else:
                ssh_config_file.seek(0)  # Reset file pointer to beginning
                ssh_config.parse(ssh_config_file)
                ssh_config_exists = True
            log.info("Parsed existing SSH configuration file.")

    if ssh_config_exists:
        # Validate that there is a host entry for each of the np0* servers and localhost
        configured_hosts = ssh_config.get_hostnames()
        log.debug(f"Configured hosts/patterns: {configured_hosts}")
        matched_hosts = []

        if not configured_hosts:
            log.warning("No hosts configured for SSH access.")
            populate_ssh_template = True
        else:
            log.info(
                f"Found SSH configurations for hosts and patterns: {configured_hosts}"
            )

            # Check each required host against existing configurations
            for host in hosts_to_access:
                matched = False
                for pattern in configured_hosts:
                    # Respect SSH negation patterns like "!badhost"
                    if pattern.startswith("!"):
                        if fnmatch.fnmatchcase(host, pattern[1:]):
                            log.error(
                                f"{host} is excluded by SSH configuration pattern: "
                                f"{pattern}. Fix the underlying problem and then re-run."
                            )
                            return
                        continue

                    # Support wildcard patterns like "np0*"
                    if fnmatch.fnmatchcase(host, pattern):
                        matched_hosts.append(host)
                        matched = True
                        break

                if not matched:
                    log.warning(f"No SSH configuration found for host: {host}")
                    populate_ssh_template = True
                    break

        # Determine if all required hosts are configured
        if set(hosts_to_access).issubset(set(matched_hosts)):
            log.info(
                f"All required hosts: {(', '.join(sorted(set(matched_hosts))))} are "
                "already configured in the SSH configuration file."
            )
            return

    if not populate_ssh_template:
        log.error("Logic flaw above.")
        return

    # Prompt the user to confirm whether to populate/create the SSH config
    if populate_ssh_template:
        prompt = (
            f"Populate (create/extend) SSH config at {ssh_configuration_path}? "
            "This will extend the existing file if present."
        )

    if not click.confirm(prompt, default=False):
        log.info("SSH configuration templating declined. Exiting.")
        return

    # Ensure we will populate (confirmed by user)
    populate_ssh_template = True

    # Populate SSH configuration from template
    log.info("Populating SSH configuration file from template.")
    template_path = (
        Path(_drunc.__file__).resolve().parent
        / "data"
        / "template_ssh_config"
        / "template.jinja"
    )
    if not template_path.exists():
        log.error(f"SSH configuration template file not found at {template_path}")
        return

    # Populate the template
    template_variables = {"USERNAME": getpass.getuser()}
    template = Template(template_path.read_text())
    result = template.render(**template_variables)

    if not ssh_configuration_path.parent.exists():
        ssh_configuration_path.parent.mkdir(parents=True, exist_ok=True)

    with ssh_configuration_path.open("w") as ssh_config_file:
        ssh_config_file.write(result)

    log.info(f"Populated SSH configuration file at {ssh_configuration_path}")
    log.info("Rerun this command to ensure that all hosts are properly configured.")


if __name__ == "__main__":
    main()
