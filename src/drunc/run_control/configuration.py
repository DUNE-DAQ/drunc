"""
Specify the configuration handler for the run control server
"""

import click


class RunControlServerConfHandler:
    pass


def get_run_control_server_configuration() -> dict[str, str | int | float | bool]:
    pass


def validate_run_control_server_config() -> bool:
    # log_path = get_log_path(
    #     user=getpass.getuser(),
    #     session_name=getattr(pmch, "pm_type", pmch.type).name,
    #     application_name=appName,
    #     override_logs=override_logs,
    #     app_log_path=log_path,
    # )

    pass


def parse_conf_url(url: str | click.Path) -> dict:
    pass
