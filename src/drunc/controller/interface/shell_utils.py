import datetime
import logging
import os
import socket
import sys
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from urllib.parse import urlparse

import click
import grpc
from druncschema.controller_pb2 import (
    Argument,
    DescribeResponse,
    FSMCommand,
    FSMCommandDescription,
    FSMResponseFlag,
    Status,
    StatusResponse,
)
from druncschema.description_pb2 import Description
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg
from druncschema.request_response_pb2 import ResponseFlag
from rich.console import ConsoleRenderable, Group, RichCast
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.unified_shell.context import UnifiedShellContext, UnifiedShellMode
from drunc.utils.grpc_utils import (
    ServerTimeout,
    ServerUnreachable,
    pack_to_any,
    unpack_any,
)
from drunc.utils.utils import format_name_for_cli, get_logger


@dataclass(slots=True)
class StatusDescriptionPair:
    status: StatusResponse | None = None
    description: DescribeResponse | None = None


def match_children(
    statuses: Sequence[StatusResponse], descriptions: Sequence[DescribeResponse]
) -> dict[str, StatusDescriptionPair]:
    children: dict[str, StatusDescriptionPair] = {}
    for status in statuses:
        pair = children.setdefault(status.name, StatusDescriptionPair())
        pair.status = status
    for description in descriptions:
        pair = children.setdefault(description.name, StatusDescriptionPair())
        pair.description = description
    return children


def get_status_table(
    status_response: StatusResponse, describe_response: DescribeResponse
):
    status = status_response.status
    description = describe_response.description

    t = Table(
        title=(
            f"[dark_green]{description.session}[/dark_green] status"
            if description is not None
            else "[dark_green]status[/dark_green]"
        )
    )
    t.add_column("Name")
    t.add_column("Info")
    t.add_column("State")
    t.add_column("Substate")
    t.add_column("In error")
    t.add_column("Included")
    t.add_column("Endpoint")

    def add_status_to_table(
        table: Table,
        status_response: StatusResponse,
        describe_response: DescribeResponse,
        prefix: str,
    ):
        status = status_response.status
        description = describe_response.description
        if status is None or description is None:
            return

        def update_endpoint(endpoint: str) -> str:
            """
            Parses endpoint to a human readable hostname

            Args:
            endpoint: Process URI

            Returns:
            str: URI with human readable hostname
            """
            if not endpoint:
                return ""

            ip_address = urlparse(endpoint).hostname
            if not ip_address:
                return ""
            hostname, _, _ = socket.gethostbyaddr(ip_address)
            return endpoint.replace(ip_address, hostname)

        table.add_row(
            prefix + status_response.name,
            description.info,
            status.state,
            status.sub_state,
            format_bool(status.in_error, false_is_good=True),
            format_bool(status.included),
            update_endpoint(description.endpoint),
        )

        children = match_children(status_response.children, describe_response.children)
        children_list = sorted(list(children.keys()))

        for child in children_list:
            child_status = children[child].status
            child_describe = children[child].description
            if child_status is None or child_describe is None:
                raise DruncShellException(
                    f"No matching status and description for child '{child}'"
                )
            add_status_to_table(t, child_status, child_describe, prefix + "  ")

    add_status_to_table(t, status_response, describe_response, "")

    def add_runinfo_to_table(table: Table, status: Status):
        table.add_row("Run number", str(status.run_info.run_number))
        table.add_row("Run type", status.run_info.run_type)
        table.add_row(
            "Start time",
            datetime.datetime.fromtimestamp(status.run_info.run_time_at_start).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )
        table.add_row(
            "Duration",
            str(datetime.timedelta(seconds=status.run_info.run_time_since_start)),
        )
        table.add_row("Trigger rate", f"{status.run_info.trigger_rate:.4f} Hz")
        table.add_row(
            "Data storage disabled", str(status.run_info.disable_data_storage)
        )
        table.add_row("Config file", status.run_info.run_config_file)
        table.add_row("Config ID", status.run_info.run_config_name)

    if status.HasField("run_info"):
        runinfo_table = Table(
            title="Run Info",
            show_header=False,
        )
        runinfo_table.add_column()
        runinfo_table.add_column()
        add_runinfo_to_table(runinfo_table, status)
        return Group(t, runinfo_table)

    return t


class StatusTableUpdater(Progress):
    def __init__(self, ctx, refresh_per_second=2, *args, **kwargs) -> None:
        self.ctx = ctx
        self.update_table()
        super().__init__(*args, refresh_per_second=refresh_per_second, **kwargs)

    def update_table(self):
        statuses = self.ctx.get_driver("controller").status()
        descriptions = self.ctx.get_driver("controller").describe()
        self.table = get_status_table(statuses, descriptions)

    def get_renderable(self) -> ConsoleRenderable | RichCast | str:
        renderable = Group(self.table, *self.get_renderables())
        return renderable


def controller_cleanup_wrapper(ctx):
    def controller_cleanup():
        log = logging.getLogger("controller.shell_utils")
        # remove the shell from the controller broadcast list
        dead = False
        who = ""

        try:
            who = ctx.get_driver("controller").who_is_in_charge().data
        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            log.error("Could not understand who is in charge from the controller.")
            log.error(e)
            who = "no_one"

        if dead:
            log.error("Controller is dead. Exiting.")
            return

        if who == ctx.get_token().user_name and ctx.took_control:
            log.info("You are in control. Surrendering control.")
            try:
                ctx.get_driver("controller").surrender_control()
            except Exception as e:
                log.error("Could not surrender control.")
                log.error(e)
            log.info("Control surrendered.")
        ctx.terminate()

    return controller_cleanup


def controller_setup(ctx, controller_address):
    log = logging.getLogger("controller.shell_utils")
    if not hasattr(ctx, "took_control"):
        raise DruncSetupException(
            "This context is not compatible with a controller, you need to add a 'took_control' bool member"
        )

    desc = Description()

    timeout = 60

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
        console=ctx._console,
    ) as progress:
        waiting = progress.add_task(
            "[yellow]Trying to talk to the root controller...", total=timeout
        )

        stored_exception = None

        start_time = time.time()
        while time.time() - start_time < timeout:
            progress.update(waiting, completed=time.time() - start_time)

            try:
                desc = ctx.get_driver("controller").describe().description
                stored_exception = None
                break
            except ServerUnreachable as e:
                stored_exception = e
                time.sleep(1)

            except Exception as e:
                ctx.critical("Could not get the controller's status")
                ctx.critical(e)
                ctx.critical("Exiting.")
                ctx.terminate()
                raise e

    if stored_exception is not None:
        raise stored_exception

    log.info(
        f"{controller_address} is '{desc.name}.{desc.session}' (name.session), starting listening..."
    )
    ctx.get_driver("controller").name = f"{desc.name}.{desc.session}"
    if desc.HasField("broadcast"):
        ctx.start_listening_controller(desc.broadcast)

    log.debug("Connected to the controller")

    # 60s for everyone to show up on the connectivity service, and 10s to come out of initialising state
    timeout = 60 + 10

    time_start = time.time()
    state = ctx.get_driver("controller").status().status.state.lower()
    with StatusTableUpdater(ctx) as updater:
        task = updater.add_task("Waiting on tree initialisation...", total=timeout)
        while time.time() - time_start < timeout and state == "initialising":
            state = ctx.get_driver("controller").status().status.state.lower()
            updater.update(task, completed=time.time() - time_start)
            updater.update_table()
            time.sleep(0.5)

        updater.update_table()

    if state == "initialising":
        log.error("Controller did not initialise in time")
        return

    log.debug(f"Taking control of the controller as {ctx.get_token()}")
    try:
        ret = ctx.get_driver("controller").take_control()
        from druncschema.request_response_pb2 import ResponseFlag

        if ret.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            log.debug("You are in control.")
            ctx.took_control = True
        else:
            log.debug("You are NOT in control.")
            ctx.took_control = False

    except Exception as e:
        log.error("You are NOT in control.")
        ctx.took_control = False
        raise e

    return desc


def search_fsm_command(command_name: str, command_list: list[FSMCommand]):
    for command in command_list:
        if command_name == command.name:
            return command
    return None


class ArgumentException(DruncShellException):
    pass


class MissingArgument(ArgumentException):
    def __init__(self, argument_name, argument_type):
        message = f'Missing argument: "{argument_name}" of type "{argument_type}"'
        super(MissingArgument, self).__init__(message)


class DuplicateArgument(ArgumentException):
    def __init__(self, argument_name):
        message = f'Duplicate argument: "{argument_name}"'
        super(DuplicateArgument, self).__init__(message)


class InvalidArgumentType(ArgumentException):
    def __init__(self, argument_name, value, expected_type):
        message = f'Argument: "{argument_name}" ({value}) does not have the expected type {expected_type}'
        super(InvalidArgumentType, self).__init__(message)


class UnhandledArgumentType(ArgumentException):
    def __init__(self, argument_name, argument_type):
        message = f'Unhandled argument type for argument: "{argument_name}" Type: {argument_type}'
        super(UnhandledArgumentType, self).__init__(message)


class UnhandledArguments(ArgumentException):
    def __init__(self, arguments_and_values):
        message = (
            f"These arguments are not handled by this command: {arguments_and_values}"
        )
        super(UnhandledArguments, self).__init__(message)


def format_bool(b, format=["dark_green", "red"], false_is_good=False):
    index_true = 0 if not false_is_good else 1
    index_false = 1 if not false_is_good else 0
    return f"[{format[index_true]}]Yes[/]" if b else f"[{format[index_false]}]No[/]"


def tree_prefix(i, n):
    first_one = "└── "
    first_many = "├── "
    next = "├── "
    last = "└── "
    if i == 0 and n == 1:
        return first_one
    elif i == 0:
        return first_many
    elif i == n - 1:
        return last
    else:
        return next


def validate_and_format_fsm_arguments(
    arguments: dict, command_arguments: list[Argument]
):
    out_dict = {}

    arguments_left = arguments
    # If the argument dict is empty, don't bother trying to read it
    if not arguments:
        return out_dict

    for argument_desc in command_arguments:
        aname = argument_desc.name
        atype = Argument.Type.Name(argument_desc.type)
        adefa = argument_desc.default_value

        if aname in out_dict:
            raise DuplicateArgument(aname)

        if (
            argument_desc.presence == Argument.Presence.MANDATORY
            and aname not in arguments
        ):
            raise MissingArgument(aname, atype)

        value = arguments.get(aname)
        if value is None:
            out_dict[aname] = adefa
            continue

        if value:
            del arguments_left[aname]

        match argument_desc.type:
            case Argument.Type.INT:
                try:
                    value = int(value)
                except Exception as e:
                    raise InvalidArgumentType(aname, value, atype) from e
                value = int_msg(value=value)
            case Argument.Type.FLOAT:
                try:
                    value = float(value)
                except Exception as e:
                    raise InvalidArgumentType(aname, value, atype) from e
                value = float_msg(value=value)
            case Argument.Type.STRING:
                value = string_msg(value=value)
            case Argument.Type.BOOL:
                bvalue = value  # .lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly']
                try:
                    value = bool_msg(value=bvalue)
                except Exception as e:
                    raise InvalidArgumentType(aname, value, atype) from e
            case _:
                try:
                    pretty_type = Argument.Type.Name(argument_desc.type)
                except:
                    pretty_type = argument_desc.type
                raise UnhandledArgumentType(argument_desc.name, pretty_type)
        out_dict[aname] = pack_to_any(value)

    # if arguments_left:
    #     raise UnhandledArguments(arguments_left)
    return out_dict


def run_one_fsm_command(
    controller_name: str,
    transition_name: str,
    obj: UnifiedShellContext,
    target: str,
    **kwargs,
) -> None:
    """
    Run one FSM command on the controller

    Args:
        controller_name (str): Name of the controller
        transition_name (str): Name of the transition to run
        obj (UnifiedShellContext): Unified shell context
        target (str): Target to run the command on
        **kwargs: Arguments to the command

    Returns:
        None

    Raises:
        ArgumentException: If there is an issue with the arguments
        ServerTimeout: If the server times out
    """
    log = get_logger("controller.shell_utils")
    log.info(
        f"Running transition '{transition_name}' on controller '{controller_name}', targeting: '{target if target else controller_name}'"
    )

    # If running in batch or semibatch mode, and error is detected, exit
    if (
        obj.running_mode in [UnifiedShellMode.BATCH, UnifiedShellMode.SEMIBATCH]
        and obj.get_driver("controller").status().status.in_error
    ):
        obj.get_driver("controller").status()
        log.error(
            "Running in batch mode, and because error state is detected, exiting."
        )
        sys.exit(1)

    execute_along_path = False
    execute_on_all_subsequent_children_in_path = True

    execute_on_root_controller = False
    if target == "":
        execute_on_root_controller = True
    elif target == controller_name:
        execute_on_root_controller = True

    if execute_on_root_controller:
        fsm_description = (
            obj.get_driver("controller")
            .describe_fsm(
                target=controller_name,
                execute_along_path=True,
                execute_on_all_subsequent_children_in_path=False,
            )
            .description
        )

        command_desc = search_fsm_command(transition_name, fsm_description.commands)

        if command_desc is None:
            log.error(
                f'Command "{transition_name}" does not exist, or is not accessible right now'
            )
            return
    else:

        class DummyCommand:
            pass

        command_desc = DummyCommand()
        command_desc.arguments = []

    try:
        formated_args = validate_and_format_fsm_arguments(
            kwargs, command_desc.arguments
        )
        data = FSMCommand(
            command_name=transition_name,
            arguments=formated_args,
        )

        timeout = 60
        time_start = time.time()
        result = None

        with ThreadPoolExecutor() as executor:
            future = executor.submit(
                obj.get_driver("controller").execute_fsm_command,
                command=data,
                target=target,
                execute_along_path=execute_along_path,
                execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
                timeout=timeout,
            )

            with StatusTableUpdater(obj) as updater:
                task = updater.add_task(
                    f"Waiting for [yellow]{transition_name}[/yellow] to complete...",
                    total=timeout,
                )
                while time.time() - time_start < timeout and not future.done():
                    updater.update(task, completed=time.time() - time_start)
                    updater.update_table()
                    time.sleep(0.5)

                updater.update_table()

            result = future.result(timeout=1)

    except ArgumentException as ae:
        log.exception(
            str(ae)
        )  # TODO: Manually raise exception, see if the str declaration is needed with rich handling
        return
    except ServerTimeout as e:
        log.error(e)
        log.error(
            "The command timed out, unfortunately this means the server is in undefined state, and [red]your best option at this stage is to [bold]terminate[/bold] and [bold]boot[/bold][/]."
        )
        log.error(
            "Alternatively, if you are patient, you can try to wait a bit longer and send [yellow]'status'[/yellow] to check if the command ends up being executed (you may want to check the logs of the controller and application with the [yellow]'logs'[/yellow] command)."
        )
        return

    if not result:
        return

    t = Table(title=f"{transition_name} execution report")
    t.add_column("Name")
    t.add_column("Command execution")
    t.add_column("FSM transition")

    def bool_to_success(flag_message, message_type):
        flag = message_type.Name(flag_message).replace("_", " ").title()
        success = False

        if (
            message_type == FSMResponseFlag
            and flag_message == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY
        ):
            success = True
        if (
            message_type == ResponseFlag
            and flag_message == ResponseFlag.EXECUTED_SUCCESSFULLY
        ):
            success = True

        return f"[dark_green]{flag}[/]" if success else f"[red]{flag}[/]"

    def add_to_table(table, response, prefix=""):
        executed_command = response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY

        table.add_row(
            prefix + response.name,
            bool_to_success(response.flag, message_type=ResponseFlag),
            (
                bool_to_success(response.fsm_flag, message_type=FSMResponseFlag)
                if executed_command
                else "[red]NA[/]"
            ),
        )
        for child_response in response.children:
            add_to_table(table, child_response, "  " + prefix)

    add_to_table(t, result)
    obj.print(t)  # rich tables require console printing

    statuses = obj.get_driver("controller").status()
    descriptions = obj.get_driver("controller").describe()
    t = get_status_table(statuses, descriptions)
    obj.print(t)
    obj.print_status_summary()


def generate_fsm_command(ctx, transition: FSMCommandDescription, controller_name: str):
    log = get_logger("controller.shell_utils")
    cmd = partial(run_one_fsm_command, controller_name, transition.name)
    cmd = click.pass_obj(cmd)
    cmd = click.option(
        "--target",
        type=str,
        help="The target to address",
        default="",
    )(cmd)

    for argument in transition.arguments:
        atype = None
        if argument.type == Argument.Type.STRING:
            atype = str
            default_value = (
                unpack_any(argument.default_value, string_msg)
                if argument.HasField("default_value")
                else None
            )
            # choices = [unpack_any(choice, string_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type == Argument.Type.INT:
            atype = int
            default_value = (
                unpack_any(argument.default_value, int_msg)
                if argument.HasField("default_value")
                else None
            )
            # choices = [unpack_any(choice, int_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type == Argument.Type.FLOAT:
            atype = float
            default_value = (
                unpack_any(argument.default_value, float_msg)
                if argument.HasField("default_value")
                else None
            )
            # choices = [unpack_any(choice, float_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type == Argument.Type.BOOL:
            atype = bool
            default_value = (
                unpack_any(argument.default_value, bool_msg)
                if argument.HasField("default_value")
                else None
            )
            # choices = [unpack_any(choice, bool_msg).value for choice in argument.choices] if argument.choices else None
        else:
            raise Exception(f"Unhandled argument type '{argument.type}'")

        argument_name = f"--{argument.name.lower().replace('_', '-')}"
        default_value = atype(default_value.value) if default_value else None

        def grab_default_value_from_env(argument_name):
            env_var = f"DRUNC_{argument_name}_DEFAULT".upper().replace("-", "_")
            env_var_value = os.getenv(env_var, None)
            if env_var_value:
                log.info(
                    f"Using environment variable '{env_var}' as default value for argument '--{argument.name.lower().replace('_', '-')}' of {transition.name.replace('_', '-').lower()} ('{env_var_value}')"
                )
                return atype(env_var_value)
            return None

        env_default_value = grab_default_value_from_env(argument.name)
        if env_default_value is not None:
            default_value = env_default_value

        cmd = click.option(
            f"{argument_name}",
            type=atype,
            default=default_value,
            show_default=True,
            required=argument.presence == Argument.Presence.MANDATORY
            and default_value is None,
            help=argument.help,
        )(cmd)

    cmd_name = format_name_for_cli(transition.name)

    cmd = click.command(
        name=cmd_name,
        help=f"Execute the transition {transition.name} on the controller {controller_name}",
    )(cmd)

    return cmd, cmd_name
