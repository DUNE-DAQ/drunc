import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import click
import grpc
from druncschema.controller_pb2 import (
    Argument,
    FSMCommand,
    FSMCommandDescription,
    FSMResponseFlag,
    Status,
)
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg
from druncschema.request_response_pb2 import Description, ResponseFlag
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
from drunc.utils.grpc_utils import (
    ServerTimeout,
    ServerUnreachable,
    pack_to_any,
    unpack_any,
)
from drunc.utils.shell_utils import DecodedResponse
from drunc.utils.utils import get_logger


def generate_none_status() -> Status:
    return Status(
        state="none",
        sub_state="none",
        in_error=False,
        included=False,
    )


def generate_none_description() -> Description:
    return Description(
        type="none",
        name="none",
        endpoint="none",
        commands=[],
        broadcast=None,
    )


def check_message_type(message, expected_type: str) -> None:
    if message is None:
        return False

    if message.data is None:
        return False

    if message.data.DESCRIPTOR.name != expected_type:
        return False
    return True


def match_children(statuses: list, descriptions: list) -> defaultdict:
    children = defaultdict(dict)
    for status in statuses:
        children[status.name].update({"status": status})

    for description in descriptions:
        children[description.name].update({"description": description})

    for child in children.values():
        if "status" not in child:
            child["status"] = None
        if "description" not in child:
            child["description"] = None
    return children


def get_status_table(status: DecodedResponse, description: DecodedResponse):
    t = Table(
        title=f"[dark_green]{description.data.session}[/dark_green] status"
        if description.data
        else "[dark_green]status[/dark_green]"
    )
    t.add_column("Name")
    t.add_column("Info")
    t.add_column("State")
    t.add_column("Substate")
    t.add_column("In error")
    t.add_column("Included")
    t.add_column("Endpoint")

    def add_status_to_table(table, status, description, prefix):
        valid_description = check_message_type(description, "Description")
        valid_status = check_message_type(status, "Status")

        if not valid_description or not valid_status:
            return

        NA = "[red]NA[/]"
        table.add_row(
            prefix + status.name if valid_status else NA,
            description.data.info if valid_description else NA,
            status.data.state if valid_status else NA,
            status.data.sub_state if valid_status else NA,
            format_bool(status.data.in_error, false_is_good=True)
            if valid_status
            else NA,
            format_bool(status.data.included) if valid_status else NA,
            description.data.endpoint if valid_description else NA,
        )

        children = match_children(status.children, description.children)
        children_list = sorted(list(children.keys()))
        for child in children_list:
            add_status_to_table(
                t,
                children[child]["status"],
                children[child]["description"],
                prefix=prefix + "  ",
            )

    add_status_to_table(t, status, description, prefix="")
    return t


class StatusTableUpdater(Progress):
    def __init__(self, ctx, refresh_per_second=2, *args, **kwargs) -> None:
        self.ctx = ctx
        self.update_table()
        super().__init__(*args, refresh_per_second=refresh_per_second, **kwargs)

    def update_table(self):
        self.table = get_status_table(
            self.ctx.get_driver("controller").status(),
            self.ctx.get_driver("controller").describe(),
        )

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
                desc = ctx.get_driver("controller").describe().data
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

    timeout = (
        60 + 10
    )  # 60s for everyone to show up on the connectivity service, and 10s to come out of initialising state

    time_start = time.time()
    controller_status = ctx.get_driver("controller").status().data.state.lower()
    with StatusTableUpdater(ctx) as updater:
        task = updater.add_task("Waiting on tree initialisation...", total=timeout)
        while (
            time.time() - time_start < timeout and controller_status == "initialising"
        ):
            controller_status = ctx.get_driver("controller").status().data.state.lower()
            updater.update(task, completed=time.time() - time_start)
            time.sleep(0.2)

    if controller_status == "initialising":
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
    controller_name,
    transition_name,
    obj,
    target,
    **kwargs,
):
    log = get_logger("controller.shell_utils")
    log.info(
        f"Running transition '{transition_name}' on controller '{controller_name}', targeting: '{target if target else controller_name}'"
    )

    execute_along_path = False
    execute_on_all_subsequent_children_in_path = True

    execute_on_root_controller = False
    if target == "":
        execute_on_root_controller = True
    elif target == controller_name:
        execute_on_root_controller = True
    elif target == "/" + controller_name:
        execute_on_root_controller = True

    if execute_on_root_controller:
        fsm_description = (
            obj.get_driver("controller")
            .describe_fsm(
                target=controller_name,
                execute_along_path=True,
                execute_on_all_subsequent_children_in_path=False,
            )
            .data
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
                arguments=data,
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
                    time.sleep(0.5)

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
            bool_to_success(response.data.flag, message_type=FSMResponseFlag)
            if executed_command
            else "[red]NA[/]",
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
        cmd = click.option(
            f"{argument_name}",
            type=atype,
            default=atype(default_value.value)
            if argument.presence != Argument.Presence.MANDATORY
            else None,
            show_default=True,
            required=argument.presence == Argument.Presence.MANDATORY,
            help=argument.help,
        )(cmd)

    cmd = click.command(
        name=transition.name.replace("_", "-").lower(),
        help=f"Execute the transition {transition.name} on the controller {controller_name}",
    )(cmd)

    return cmd, transition.name.replace("_", "-").lower()
