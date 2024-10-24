from rich import print
from druncschema.controller_pb2 import FSMCommandsDescription

import logging
log = logging.getLogger('controller_shell_utils')

def controller_cleanup_wrapper(ctx):
    def controller_cleanup():
        # remove the shell from the controller broadcast list
        dead = False
        import grpc
        who = ''

        from drunc.utils.grpc_utils import unpack_any
        try:
            who = ctx.get_driver('controller').who_is_in_charge().data

        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            log.error('Could not understand who is in charge from the controller.')
            log.error(e)
            who = 'no_one'

        if dead:
            log.error('Controller is dead. Exiting.')
            return

        if who == ctx.get_token().user_name and ctx.took_control:
            log.info('You are in control. Surrendering control.')
            try:
                ctx.get_driver('controller').surrender_control()
            except Exception as e:
                log.error('Could not surrender control.')
                log.error(e)
            log.info('Control surrendered.')
        ctx.terminate()
    return controller_cleanup


def controller_setup(ctx, controller_address, timeout=60):
    if not hasattr(ctx, 'took_control'):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('This context is not compatible with a controller, you need to add a \'took_control\' bool member')


    from druncschema.request_response_pb2 import Description
    desc = Description()

    from drunc.utils.grpc_utils import ServerUnreachable
    stored_exception = None

    if timeout == 0: # we break directly
        try:
            desc = ctx.get_driver('controller').describe().data
        except Exception as e:
            ctx.critical(f'Could not connect to the process manager, use --log-level DEBUG for more information')
            ctx.debug(f'Error: {e}')
            ctx.print(f'\nIf you are sure that the controller exists, on the NP04 cluster, this error usually happens when you have the web proxy enabled. Try to disable it by doing:\n\n[yellow]source ~np04daq/bin/web_proxy.sh -u[/]\n')
            raise e

    else:
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, TimeElapsedColumn

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=ctx._console,
        ) as progress:

            waiting = progress.add_task("[yellow]Trying to talk to the top controller...", total=timeout)

            import time
            start_time = time.time()
            while time.time()-start_time < timeout:
                progress.update(waiting, completed=time.time()-start_time)

                try:
                    desc = ctx.get_driver('controller').describe().data
                    stored_exception = None
                    break
                except ServerUnreachable as e:
                    stored_exception = e
                    time.sleep(1)

                except Exception as e:
                    ctx.critical('Could not get the controller\'s status')
                    ctx.critical(e)
                    ctx.critical('Exiting.')
                    ctx.terminate()
                    raise e

    if stored_exception is not None:
        raise stored_exception

    ctx.info(f'{controller_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
    if desc.HasField('broadcast'):
        ctx.start_listening_controller(desc.broadcast)

    ctx.print('Connected to the controller')

    children = ctx.get_driver('controller').ls().data
    ctx.print(f'{desc.name}.{desc.session}\'s children :family:: {children.text}')

    ctx.info(f'Taking control of the controller as {ctx.get_token()}')
    try:
        ret = ctx.get_driver('controller').take_control()
        from druncschema.request_response_pb2 import ResponseFlag

        if ret.flag == ResponseFlag.EXECUTED_SUCCESSFULLY:
            ctx.info('You are in control.')
            ctx.took_control = True
        else:
            ctx.warn(f'You are NOT in control.')
            ctx.took_control = False


    except Exception as e:
        ctx.warn('You are NOT in control.')
        ctx.took_control = False
        raise e

    return desc


from drunc.controller.interface.context import ControllerContext
from druncschema.controller_pb2 import FSMCommand
def search_fsm_command(command_name:str, command_list:list[FSMCommand]):
    for command in command_list:
        if command_name == command.name:
            return command
    return None

from drunc.exceptions import DruncShellException
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
        message = f'These arguments are not handled by this command: {arguments_and_values}'
        super(UnhandledArguments, self).__init__(message)

from druncschema.controller_pb2 import Argument
def validate_and_format_fsm_arguments(arguments:dict, command_arguments:list[Argument]):
    from druncschema.generic_pb2 import int_msg, float_msg, string_msg, bool_msg
    from drunc.utils.grpc_utils import pack_to_any
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

        if argument_desc.presence == Argument.Presence.MANDATORY and not aname in arguments:
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
                bvalue = value#.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly']

                try:
                    value = bool_msg(value=bvalue)
                except Exception as e:
                    raise InvalidArgumentType(aname, value, atype) from e


            case _:
                try:
                    pretty_type = Argument.Type.Name(argument_desc.type)
                except:
                    pretty_type = argument_desc.type
                raise UnhandledArgumentType(argument_desc.name,  pretty_type)


        out_dict[aname] = pack_to_any(value)

    # if arguments_left:
    #     raise UnhandledArguments(arguments_left)

    return out_dict


def format_bool(b, format=['dark_green', 'bold white on red'], false_is_good = False):
    index_true = 0 if not false_is_good else 1
    index_false = 1 if not false_is_good else 0

    return f'[{format[index_true]}]Yes[/]' if b else f'[{format[index_false]}]No[/]'

def tree_prefix(i, n):
    first_one = "└── "
    first_many = "├── "
    next = "├── "
    last = "└── "
    first_column = ''
    if i==0 and n == 1:
        return first_one
    elif i==0:
        return first_many
    elif i == n-1:
        return last
    else:
        return next


def run_one_fsm_command(controller_name, transition_name, obj, **kwargs):
    obj.print(f"Running transition \'{transition_name}\' on controller \'{controller_name}\'")
    from druncschema.controller_pb2 import FSMCommand

    description = obj.get_driver('controller').describe_fsm().data

    from drunc.controller.interface.shell_utils import search_fsm_command, validate_and_format_fsm_arguments, ArgumentException

    command_desc = search_fsm_command(transition_name, description.commands)

    if command_desc is None:
        obj.error(f'Command "{transition_name}" does not exist, or is not accessible right now')
        return

    try:
        formated_args = validate_and_format_fsm_arguments(kwargs, command_desc.arguments)
        data = FSMCommand(
            command_name = transition_name,
            arguments = formated_args,
        )
        result = obj.get_driver('controller').execute_fsm_command(
            arguments = data,
        )
    except ArgumentException as ae:
        obj.print(str(ae))
        return

    if not result: return

    from drunc.controller.interface.shell_utils import format_bool, tree_prefix
    from drunc.utils.grpc_utils import unpack_any
    from druncschema.controller_pb2 import FSMResponseFlag, FSMCommandResponse

    from rich.table import Table
    t = Table(title=f'{transition_name} execution report')
    t.add_column('Name')
    t.add_column('Command execution')
    t.add_column('FSM transition')

    from druncschema.request_response_pb2 import ResponseFlag
    def bool_to_success(flag_message, FSM):
        flag = False
        if FSM and flag_message == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY:
            flag = True
        if not FSM and flag_message == ResponseFlag.EXECUTED_SUCCESSFULLY:
            flag = True
        return "success" if flag else "failed"

    def add_to_table(table, response, prefix=''):
        table.add_row(
            prefix+response.name,
            bool_to_success(response.flag, FSM=False),
            bool_to_success(response.data.flag, FSM=True) if response.flag == FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY else "failed",
        )
        for child_response in response.children:
            add_to_table(table, child_response, "  "+prefix)

    add_to_table(t, result)
    obj.print(t)


from druncschema.controller_pb2 import FSMCommandDescription

def generate_fsm_command(ctx, transition:FSMCommandDescription, controller_name:str):
    import click

    from functools import partial
    cmd = partial(run_one_fsm_command, controller_name, transition.name)

    cmd = click.pass_obj(cmd)

    from druncschema.controller_pb2 import Argument
    from drunc.utils.grpc_utils import unpack_any
    from druncschema.generic_pb2 import int_msg, float_msg, string_msg, bool_msg

    for argument in transition.arguments:
        atype = None
        if argument.type == Argument.Type.STRING:
            atype = str
            default_value = unpack_any(argument.default_value, string_msg) if argument.HasField('default_value') else None
            # choices = [unpack_any(choice, string_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type ==  Argument.Type.INT:
            atype = int
            default_value = unpack_any(argument.default_value, int_msg)    if argument.HasField('default_value') else None
            # choices = [unpack_any(choice, int_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type == Argument.Type.FLOAT:
            atype = float
            default_value = unpack_any(argument.default_value, float_msg)  if argument.HasField('default_value') else None
            # choices = [unpack_any(choice, float_msg).value for choice in argument.choices] if argument.choices else None
        elif argument.type == Argument.Type.BOOL:
            atype = bool
            default_value = unpack_any(argument.default_value, bool_msg)   if argument.HasField('default_value') else None
            # choices = [unpack_any(choice, bool_msg).value for choice in argument.choices] if argument.choices else None
        else:
            raise Exception(f'Unhandled argument type \'{argument.type}\'')

        argument_name = f'--{argument.name.lower().replace("_", "-")}'
        cmd = click.option(
            f'{argument_name}',
            type=atype,
            default = atype(default_value.value) if argument.presence != Argument.Presence.MANDATORY else None,
            required= argument.presence == Argument.Presence.MANDATORY,
            help=argument.help,
        )(cmd)

    cmd = click.command(
        name = transition.name.replace('_', '-').lower(),
        help = f'Execute the transition {transition.name} on the controller {controller_name}'
    )(cmd)

    return cmd, transition.name.replace('_', '-').lower()