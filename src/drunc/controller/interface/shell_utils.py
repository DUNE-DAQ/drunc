import logging
from functools import partial

import click
from druncschema.controller_pb2 import (
    Argument,
    FSMCommand,
    FSMCommandDescription,
    FSMResponseFlag,
    Status,
)
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg
from druncschema.request_response_pb2 import Description, ResponseFlag
from rich.table import Table

from drunc.exceptions import DruncSetupException, DruncShellException
from drunc.utils.grpc_utils import pack_to_any, unpack_any
from drunc.utils.shell_utils import DecodedResponse


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
    if message.data is None:
        return False

    if message.data.DESCRIPTOR.name != expected_type:
        return False
    return True


def match_children(statuses: list, descriptions: list) -> dict:
    children = {}
    for status in statuses:
        children[status.name] = {"status": status}

    for description in descriptions:
        children[description.name].update({"description": description})

    return children


def print_status_table(obj, status: DecodedResponse, description: DecodedResponse):
    from rich.table import Table

    t = Table(title=f"[dark_green]{description.data.session}[/dark_green] status")
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

        table.add_row(
            prefix + status.name,
            description.data.info if valid_description else "[red]unavailable[/]",
            status.data.state if valid_status else "[red]unavailable[/]",
            status.data.sub_state if valid_status else "[red]unavailable[/]",
            format_bool(status.data.in_error, false_is_good=True)
            if valid_status
            else "[red]unavailable[/]",
            format_bool(status.data.included)
            if valid_status
            else "[red]unavailable[/]",
            description.data.endpoint if valid_description else "[red]unavailable[/]",
        )
        for child in match_children(status.children, description.children).values():
            add_status_to_table(
                t, child["status"], child["description"], prefix=prefix + "  "
            )

    add_status_to_table(t, status, description, prefix="")
    obj.print(t)
    obj.print_status_summary()


def controller_cleanup_wrapper(ctx):
    def controller_cleanup():
        log = logging.getLogger("controller.shell_utils")
        # remove the shell from the controller broadcast list
        dead = False
        import grpc

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

    from druncschema.request_response_pb2 import Description

    desc = Description()

    timeout = 60

    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )

    from drunc.utils.grpc_utils import ServerUnreachable

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
        console=ctx._console,
    ) as progress:
        waiting = progress.add_task(
            "[yellow]Trying to talk to the controller...", total=timeout
        )

        stored_exception = None
        import time

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


def run_one_fsm_command(controller_name, transition_name, obj, **kwargs):
    log = logging.getLogger("controller.shell_utils")
    log.info(
        f"Running transition '{transition_name}' on controller '{controller_name}'"
    )
    fsm_description = obj.get_driver("controller").describe_fsm().data
    command_desc = search_fsm_command(transition_name, fsm_description.commands)

    if command_desc is None:
        log.error(
            f'Command "{transition_name}" does not exist, or is not accessible right now'
        )
        return

    try:
        formated_args = validate_and_format_fsm_arguments(
            kwargs, command_desc.arguments
        )
        data = FSMCommand(
            command_name=transition_name,
            arguments=formated_args,
        )
        result = obj.get_driver("controller").execute_fsm_command(
            arguments=data,
        )
    except ArgumentException as ae:
        log.exception(
            str(ae)
        )  # TODO: Manually raise exception, see if the str declaration is needed with rich handling
        return

    if not result:
        return
    t = Table(title=f"{transition_name} execution report")
    t.add_column("Name")
    t.add_column("Command execution")
    t.add_column("FSM transition")

    def bool_to_success(flag_message, FSM):
        flag = False
        if FSM and flag_message == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY:
            flag = True
        if not FSM and flag_message == ResponseFlag.EXECUTED_SUCCESSFULLY:
            flag = True
        return "[dark_green]success[/]" if flag else "[red]failed[/]"

    def add_to_table(table, response, prefix=""):
        table.add_row(
            prefix + response.name,
            bool_to_success(response.flag, FSM=False),
            bool_to_success(response.data.flag, FSM=True)
            if response.flag == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY
            else "[red]failed[/]",
        )
        for child_response in response.children:
            add_to_table(table, child_response, "  " + prefix)

    add_to_table(t, result)
    obj.print(t)  # rich tables require console printing

    statuses = obj.get_driver("controller").status()
    descriptions = obj.get_driver("controller").describe()
    print_status_table(obj, statuses, descriptions)


def generate_fsm_command(ctx, transition: FSMCommandDescription, controller_name: str):
    cmd = partial(run_one_fsm_command, controller_name, transition.name)
    cmd = click.pass_obj(cmd)
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
            required=argument.presence == Argument.Presence.MANDATORY,
            help=argument.help,
        )(cmd)

    cmd = click.command(
        name=transition.name.replace("_", "-").lower(),
        help=f"Execute the transition {transition.name} on the controller {controller_name}",
    )(cmd)

    return cmd, transition.name.replace("_", "-").lower()
