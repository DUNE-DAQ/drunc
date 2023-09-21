def search_fsm_command(command_name, command_list):
    for command in command_list:
        if command_name == command.name:
            return command
    return None


class ArgumentException(Exception):
    pass

class MissingArgument(ArgumentException):
    def __init__(self, argument_name, argument_type):
        message = f'Missing argument: "{argument_name}" of type "{argument_type}"'
        super(MissingArgument, self).__init__(message)

class DuplicateArgument(ArgumentException):
    def __init__(self, argument_name):
        message = f'Duplicate argument: "{argument_name}"'
        super(MissingArgument, self).__init__(message)

class InvalidArgumentType(ArgumentException):
    def __init__(self, argument_name, value, expected_type):
        message = f'Argument: "{argument_name}" ({value}) does not have the expected type {expected_type}'
        super(InvalidArgumentType, self).__init__(message)

class UnhandledArgumentType(ArgumentException):
    def __init__(self, argument_name, argument_type):
        message = f'Unhandled argument type for argument: "{argument_name}" Type: {argument_type}'
        super(MissingArgument, self).__init__(message)

class UnhandledArguments(ArgumentException):
    def __init__(self, arguments_and_values):
        message = f'These arguments are not handled by this command: {arguments_and_values}'
        super(UnhandledArguments, self).__init__(message)


def validate_and_format_fsm_arguments(arguments, arguments_desc):
    from druncschema.controller_pb2 import Argument
    from druncschema.generic_pb2 import int_msg, float_msg, string_msg
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


            case _:
                raise UnhandledArgumentType(argument_desc.name, argument_desc.type)


        out_dict[aname] = pack_to_any(value)

    if arguments_left:
        raise UnhandledArguments(arguments_left)

    return out_dict
