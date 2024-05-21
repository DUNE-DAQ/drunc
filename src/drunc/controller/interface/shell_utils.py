
def controller_cleanup_wrapper(ctx):
    def controller_cleanup():
        # remove the shell from the controller broadcast list
        dead = False
        import grpc
        who = ''
        from drunc.utils.grpc_utils import unpack_any
        try:
            who = ctx.get_driver('controller').who_is_in_charge(rethrow=True).text

        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            ctx.error('Could not understand who is in charge from the controller.')
            ctx.error(e)
            who = 'no_one'

        if dead:
            ctx.error('Controller is dead. Exiting.')
            return

        if who == ctx.get_token().user_name and ctx.took_control:
            ctx.info('You are in control. Surrendering control.')
            try:
                ctx.get_driver('controller').surrender_control(rethrow=True)
            except Exception as e:
                ctx.error('Could not surrender control.')
                ctx.error(e)
            ctx.info('Control surrendered.')
        ctx.terminate()
    return controller_cleanup


def controller_setup(ctx, controller_address):
    if not hasattr(ctx, 'took_control'):
        from drunc.exceptions import DruncSetupException
        raise DruncSetupException('This context is not compatible with a controller, you need to add a \'took_control\' bool member')


    from druncschema.request_response_pb2 import Description
    desc = Description()

    timeout = 60

    from drunc.utils.grpc_utils import ServerUnreachable
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, TimeElapsedColumn

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
        console=ctx._console,
    ) as progress:

        waiting = progress.add_task("[yellow]timeout", total=timeout)

        stored_exception = None
        import time
        start_time = time.time()
        while time.time()-start_time < timeout:
            progress.update(waiting, completed=time.time()-start_time)

            try:
                desc = ctx.get_driver('controller').describe(rethrow=True).data
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

    children = ctx.get_driver('controller').ls(rethrow=False).data
    ctx.print(f'{desc.name}.{desc.session}\'s children :family:: {children.text}')

    ctx.info(f'Taking control of the controller as {ctx.get_token()}')
    try:
        ret = ctx.get_driver('controller').take_control(rethrow=True)
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



def search_fsm_command(command_name, command_list):
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


def validate_and_format_fsm_arguments(arguments, arguments_desc):
    from druncschema.controller_pb2 import Argument
    from druncschema.generic_pb2 import int_msg, float_msg, string_msg, bool_msg
    from drunc.utils.grpc_utils import pack_to_any
    out_dict = {}

    arguments_left = arguments

    for argument_desc in arguments_desc:
        aname = argument_desc.name
        atype = Argument.Type.Name(argument_desc.type)
        adefa = argument_desc.default_value

        if aname in out_dict:
            raise DuplicateArgument(aname)

        if argument_desc.presence == Argument.Presence.MANDATORY and not aname in arguments:
            raise MissingArgument(aname, atype)

        value = arguments.get(aname)
        if not value:
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
                bvalue = value.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly']

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

    if arguments_left:
        raise UnhandledArguments(arguments_left)

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
