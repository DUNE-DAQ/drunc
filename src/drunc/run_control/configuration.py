"""
Specify the configuration handler for the run control server
"""

from importlib import resources

import click

from drunc.configuration_types import CONFIGURATION_TYPES
from drunc.utils.utils import load_json_to_dict


class RunControlServerConfHandler:
    pass


def import_config_json_to_dict(
    config_type: str, configuration_file: str
) -> dict[str, str | int | float | bool]:
    """
    Import the configuration from a JSON file into a dictionary.

    Args:
        configuration_file (str): Path to the configuration JSON file.
        config_type (str): Type of configuration to import (e.g., "process_manager", "run_control").

    Returns:
        dict[str, str | int | float | bool]: A dictionary containing the configuration settings.

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
    """
    # Validate thet type of configuration being imported
    if config_type not in CONFIGURATION_TYPES:
        raise ValueError(
            f"Invalid configuration type '{config_type}'. Must be one of {CONFIGURATION_TYPES}."
        )

    # Use importlib.resources to access the configuration file within the package data
    resource = resources.files("drunc.data") / config_type / configuration_file
    with resources.as_file(resource) as file_path:
        if not file_path.exists():
            raise FileNotFoundError(
                f"Configuration file '{configuration_file}' not found in package data."
            )

        # Import the data from the JSON file into a dictionary
        configuration = load_json_to_dict(resource)
        return configuration


def get_run_control_server_configuration(
    configuration_file: str,
    port_override: int | None,
    log_path_override: str | None,
    override_logs: bool,
    log_level_override: str | None,
) -> dict[str, str | int | float | bool]:
    """
    Retrieve the run control server configuration from a JSON file and apply overrides.

    Args:
        configuration (click.Path | None): Path to the configuration JSON file.
        port_override (int | None): Optional override for the server port.
        log_path_override (str | None): Optional override for the log path.
        override_logs (bool): Flag to indicate whether to override existing logs.
        log_level (str | None): Optional override for the log level.

    Returns:
        dict[str, str | int | float | bool]: A dictionary containing the final configuration settings

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
    """
    # Import the config parameters from JSON file into a dictionary
    configuration = import_config_json_to_dict("run_control", configuration_file)

    # Apply overrides to the configuration dictionary
    if port_override is not None:
        configuration["port"] = port_override

    if log_path_override is not None:
        configuration["log_path"] = log_path_override

    if override_logs:
        configuration["override_logs"] = override_logs

    if log_level_override is not None:
        configuration["log_level"] = log_level_override

    return configuration


def is_run_control_server_valid() -> bool:
    # log_path = get_log_path(
    #     user=getpass.getuser(),
    #     session_name=getattr(pmch, "pm_type", pmch.type).name,
    #     application_name=appName,
    #     override_logs=override_logs,
    #     app_log_path=log_path,
    # )
    return True


def parse_conf_url(url: str | click.Path) -> dict:
    pass
