import click

from drunc.process_manager.utils import generate_process_query


def validate_conf_string(ctx, param, boot_configuration):
    return boot_configuration


def add_query_options(at_least_one: bool, all_processes_by_default: bool = False):
    def wrapper(f0):
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
        f5 = click.option(
            "--crash",
            is_flag=True,
            default=False,
            help="Simulate a crash: send SIGKILL without any cleanup, leaving the process manager in an unexpected-death state.",
        )(f4)
        return generate_process_query(f5, at_least_one, all_processes_by_default)

    return wrapper
