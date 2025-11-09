import json
from time import sleep

import click

from drunc.controller.interface.context import ControllerContext
from drunc.controller.interface.shell_utils import controller_setup, get_status_table
from drunc.utils.utils import get_logger

logger_params = {"logger_name": "controller.interface", "rich_handler": True}


@click.command("list-transitions")
@click.option(
    "--all",
    is_flag=True,
    show_default=True,
    default=False,
    help="List all transitions (available and unavailable)",
)
@click.option("--target", type=str, help="The target to address", default="")
@click.pass_obj
def list_transitions(obj: ControllerContext, all: bool, target: str) -> None:
    log = get_logger(**logger_params)
    print("Logging")
    log.critical(f"Have logger with handlers: {log.handlers}")
    desc = obj.get_driver("controller").describe_fsm(
        target=target,
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=False,
        key="all-transitions" if all else None,
    )
    if not desc:
        log.error("Could not get the list of commands available")
        return

    if all:
        log.info(
            f"\nAvailable transitions on '{desc.name}' are ([underline]some may not be accessible now, use list-transition without --all to see what transitions can be issued now[/]):"
        )
    else:
        log.info(f"\nCurrently available controller transitions on '{desc.name}' are:")

    for c in desc.description.commands:
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
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def status(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    statuses = obj.get_driver("controller").status(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    )  # Get the dynamic system information
    descriptions = obj.get_driver("controller").describe(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    )  # Get the static system information
    t = get_status_table(statuses, descriptions)
    obj.print(t)
    obj.print_status_summary()


@click.command("recompute-status")
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def recompute_status(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    statuses = obj.get_driver("controller").recompute_status(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    )
    descriptions = obj.get_driver("controller").describe(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    )
    t = get_status_table(statuses, descriptions)
    obj.print(t)
    obj.print_status_summary()


@click.command("connect")
@click.argument("controller_address", type=str)
@click.option("-f", "--force", is_flag=True, help="Confirm the disconnect")
@click.pass_obj
def connect(obj: ControllerContext, controller_address: str, force: bool) -> None:
    log = get_logger(**logger_params)

    if obj.has_driver("controller"):
        driver = obj.get_driver("controller")
        log.info(f"Already connected to a controller ({driver.name}@{driver.address})")
        if not force:
            click.confirm("Do you want to disconnect from it before?", abort=True)
        log.info("Disconnecting...")
        obj.delete_driver("controller")

    log.info(f"Connecting this shell to the controller at {controller_address}...")

    if controller_address.startswith("grpc://"):
        controller_address = controller_address.replace("grpc://", "")

    obj.set_controller_driver(controller_address)
    controller_setup(obj, controller_address)


@click.command("disconnect")
@click.option("-f", "--force", is_flag=True, help="Confirm the disconnect")
@click.pass_obj
def disconnect(obj: ControllerContext, force: bool):
    log = get_logger(**logger_params)

    if not obj.has_driver("controller"):
        log.info("You are not connected to any controller.")
        return

    driver = obj.get_driver("controller")

    if not force:
        log.info(
            f"""
[red]You are about to disconnect from the {driver.name} controller.[/red]

To reconnect to it, you will need to issue the following command:

[yellow]connect {driver.address}[/yellow]

To get the address of another controller, abort now and issue the command:

[yellow]status[/yellow]

You can also find the controller address on the connectivity service.
""",
            extra={"markup": True},
        )
        click.confirm(
            "Are you sure you want to disconnect from the controller?", abort=True
        )

    obj.delete_driver("controller")


@click.command("take-control")
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def take_control(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    obj.get_driver("controller").take_control(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    ).data


@click.command("surrender-control")
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def surrender_control(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    obj.get_driver("controller").surrender_control(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    ).data


@click.command("who-am-i")
@click.pass_obj
def who_am_i(obj: ControllerContext) -> None:
    log = get_logger(**logger_params)
    log.info(obj.get_token().user_name)


@click.command("who-is-in-charge")
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def who_is_in_charge(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    who = (
        obj.get_driver("controller")
        .who_is_in_charge(
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        .data
    )
    if who:
        log = get_logger(**logger_params)
        log.info(who.text)  ## TODO create a table of who is in charge


@click.command("include")
@click.option("--target", type=str, help="The target to address", default="")
@click.pass_obj
def include(
    obj: ControllerContext,
    target: str,
) -> None:
    result = (
        obj.get_driver("controller")
        .include(
            target=target,
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        )
        .data
    )
    if not result:
        return
    log = get_logger(**logger_params)
    log.info(result.text)


@click.command("exclude")
@click.option("--target", type=str, help="The target to address", default="")
@click.pass_obj
def exclude(
    obj: ControllerContext,
    target: str,
) -> None:
    result = (
        obj.get_driver("controller")
        .exclude(
            target=target,
            execute_along_path=False,
            execute_on_all_subsequent_children_in_path=True,
        )
        .data
    )
    if not result:
        return
    log = get_logger(**logger_params)
    log.info(result.text)


@click.command("expert-command")
@click.option(
    "-s",
    "--string",
    is_flag=True,
    show_default=True,
    help="Read the command directly from the command line, else you need to write a file and provide its path",
)
@click.argument("command", type=str)
@click.option("--target", type=str, help="The target to address", default="")
@click.pass_obj
def expert_command(
    obj: ControllerContext,
    command: str,
    string: bool,
    target: str,
) -> None:
    data = dict()
    log = get_logger(**logger_params)
    try:
        if string:
            data = json.loads(command)
        else:
            with open(command, "r") as f:
                data = json.load(f)

    except FileNotFoundError:
        log.error(f"File not found: {command}")
        return

    except json.JSONDecodeError as e:
        log.error(f"JSON decode error: {e}")
        return

    result = obj.get_driver("controller").expert_command(
        target=target,
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=True,
        json_string=json.dumps(data),
    )

    def print_result(result, prefix=""):
        if not hasattr(result, "data"):
            log.info(
                f"{prefix}[yellow]{result.name}[/yellow] [red]NO RESPONSE (no data)[/red]"
            )
        elif result.data.DESCRIPTOR.name == "PlainText":
            log.info(
                f"{prefix}[yellow]{result.name}[/yellow] [green]{result.data.text}[/green]"
            )
        elif result.data.DESCRIPTOR.name == "Stacktrace":
            for i in reversed(range(len(result.data.text))):
                error = result.data.text[i]
                if error != "":
                    break
            log.info(
                f"{prefix}[yellow]{result.name}[/yellow] [red]ERROR: {error}[/red]"
            )
        else:
            log.info(
                f"{prefix}[yellow]{result.name}[/yellow] [red]NO RESPONSE (data format not understood: {result.data.DESCRIPTOR.name})[/red]"
            )

        for child in result.children:
            print_result(child, prefix + "    ")

    print_result(result)


@click.command("to_error")
@click.option("--target", type=str, help="The target to address", default="")
@click.option(
    "--execute-along-path/--dont-execute-along-path",
    is_flag=True,
    show_default=True,
    help="Execute the command along the path",
    default=True,
)
@click.option(
    "--execute-on-all-subsequent-children-in-path/--dont-execute-on-all-subsequent-children-in-path",
    is_flag=True,
    show_default=True,
    help="Execute the command on all subsequent children in the path",
    default=True,
)
@click.pass_obj
def to_error(
    obj: ControllerContext,
    target: str,
    execute_along_path: bool,
    execute_on_all_subsequent_children_in_path: bool,
) -> None:
    obj.get_driver("controller").to_error(
        target=target,
        execute_along_path=execute_along_path,
        execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
    )
