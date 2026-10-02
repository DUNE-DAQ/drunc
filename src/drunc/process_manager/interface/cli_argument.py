from typing import Callable

import click
from click.core import Context, Parameter
from click.decorators import FC

from drunc.process_manager.utils import generate_process_query


def validate_conf_string(
    ctx: Context, param: Parameter, boot_configuration: str
) -> str:
    """
    Validate the boot configuration string.

    Args:
        ctx (Context): The Click context.
        param (Parameter): The Click parameter.
        boot_configuration (str): The boot configuration string to validate.

    Returns:
        str: The validated boot configuration string.
    """
    return boot_configuration


def add_query_options_no_session(
    at_least_one: bool, all_processes_by_default: bool = False
) -> Callable[[FC], FC]:
    """
    Decorator to add query options to a click command.

    Args:
        at_least_one (bool): If True, at least one query option must be provided.
        all_processes_by_default (bool): If True, all processes will be selected by
            default if no query options are provided.

    Returns:
        function: A decorator function that adds query options to a click command.
    """

    def wrapper(f1: FC) -> FC:
        f2 = click.option(
            "-n",
            "--name",
            type=str,
            default=None,
            multiple=True,
            help="Select the process of a particular names",
        )(f1)
        f3 = click.option(
            "-u",
            "--user",
            type=str,
            default=None,
            help="Select the process of a particular user",
        )(f2)
        f4 = click.option(
            "--uuid",
            type=str,
            default=None,
            multiple=True,
            help="Select the process of a particular UUIDs",
        )(f3)
        return generate_process_query(f4, at_least_one, all_processes_by_default)

    return wrapper


def add_query_options(
    at_least_one: bool, all_processes_by_default: bool = False
) -> Callable[[FC], FC]:
    """
    Decorator to add query options to a click command, including session selection.

    Args:
        at_least_one (bool): If True, at least one query option must be provided.
        all_processes_by_default (bool): If True, all processes will be selected by
            default if no query options are provided.

    Returns:
        function: A decorator function that adds query options to a click command.
    """

    def wrapper(f0: FC) -> FC:
        f1 = click.option(
            "-s",
            "--session",
            type=str,
            default=None,
            help="Select the processes on a particular session",
        )(f0)
        return add_query_options_no_session(at_least_one, all_processes_by_default)(f1)

    return wrapper
