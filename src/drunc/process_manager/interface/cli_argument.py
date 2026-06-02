from collections.abc import Callable
from typing import ParamSpec, cast

import click
from click import Context
from click.core import Parameter

from drunc.process_manager.utils import generate_process_query

P = ParamSpec("P")


def validate_conf_string(
    ctx: Context,
    param: Parameter,
    boot_configuration: str,
) -> str:
    del ctx, param
    return boot_configuration


def add_query_options(
    at_least_one: bool, all_processes_by_default: bool = False
) -> Callable[[Callable[P, object]], Callable[P, object]]:
    def wrapper(f0: Callable[P, object]) -> Callable[P, object]:
        f1 = click.option(
            "-s",
            "--session",
            type=str,
            default=None,
            help="Select the processes on a particular session",
        )(f0)
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
        return cast(
            Callable[P, object],
            generate_process_query(f4, at_least_one, all_processes_by_default),
        )

    return wrapper
