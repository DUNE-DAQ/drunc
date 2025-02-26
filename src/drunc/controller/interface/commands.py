from time import sleep

import click
from druncschema.controller_pb2 import FSMCommand

from drunc.controller.interface.context import ControllerContext
from drunc.controller.interface.shell_utils import controller_setup, print_status_table
from drunc.utils.utils import get_logger

logger_params = {"logger_name": "controller.interface", "rich_handler": True}


@click.command("list-transitions")
@click.option(
    "--all", is_flag=True, help="List all transitions (available and unavailable)"
)
@click.pass_obj
def list_transitions(obj: ControllerContext, all: bool) -> None:
    log = get_logger(**logger_params)
    desc = obj.get_driver("controller").describe_fsm("all-transitions" if all else None)
    if not desc:
        log.error("Could not get the list of commands available")
        return

    if all:
        log.info(
            f"\nAvailable transitions on '{desc.name}' are ([underline]some may not be accessible now, use list-transition without --all to see what transitions can be issued now[/]):"
        )
    else:
        log.info(f"\nCurrently available controller transitions on '{desc.name}' are:")

    for c in desc.data.commands:
        log.info(f" - [yellow]{c.name.replace('_', '-').lower()}[/]")

    log.info("\nUse [yellow]help <command>[/] for more information on a command.\n")


@click.command("wait")
@click.argument("sleep_time", type=int, default=1)
@click.pass_obj
def wait(obj: ControllerContext, sleep_time: int) -> None:
    log = get_logger(**logger_params)
    log.info(f"Command [green]wait[/green] running for {sleep_time} seconds.")
    sleep(sleep_time)  # seconds
    log.info(f"Command [green]wait[/green] ran for {sleep_time} seconds.")


@click.command("status")
@click.pass_obj
def status(obj: ControllerContext) -> None:
    statuses = obj.get_driver(
        "controller"
    ).status()  # Get the dynamic system information
    descriptions = obj.get_driver(
        "controller"
    ).describe()  # Get the static system information
    print_status_table(obj, statuses, descriptions)


@click.command("connect")
@click.argument("controller_address", type=str)
@click.pass_obj
def connect(obj: ControllerContext, controller_address: str) -> None:
    log = get_logger(**logger_params)
    log.info(f"Connecting this shell to the controller at {controller_address}")
    obj.set_controller_driver(controller_address)
    controller_setup(obj, controller_address)


@click.command("take-control")
@click.pass_obj
def take_control(obj: ControllerContext) -> None:
    obj.get_driver("controller").take_control().data


@click.command("surrender-control")
@click.pass_obj
def surrender_control(obj: ControllerContext) -> None:
    obj.get_driver("controller").surrender_control().data


@click.command("who-am-i")
@click.pass_obj
def who_am_i(obj: ControllerContext) -> None:
    log = get_logger(**logger_params)
    log.info(obj.get_token().user_name)


@click.command("who-is-in-charge")
@click.pass_obj
def who_is_in_charge(obj: ControllerContext) -> None:
    who = obj.get_driver("controller").who_is_in_charge().data
    if who:
        log = get_logger(**logger_params)
        log.info(who.text)


@click.command("include")
@click.pass_obj
def include(obj: ControllerContext) -> None:
    data = FSMCommand(command_name="include")
    result = obj.get_driver("controller").include(arguments=data).data
    if not result:
        return
    log = get_logger(**logger_params)
    log.info(result.text)


@click.command("exclude")
@click.pass_obj
def exclude(obj: ControllerContext) -> None:
    data = FSMCommand(command_name="exclude")
    result = obj.get_driver("controller").exclude(arguments=data).data
    if not result:
        return
    log = get_logger(**logger_params)
    log.info(result.text)
