
def convert_fsm_transition(transitions):
    from druncschema.controller_pb2 import FSMCommandsDescription, FSMCommandDescription
    desc = FSMCommandsDescription()
    for t in transitions:
        desc.commands.append(
            FSMCommandDescription(
                name = t.name,
                data_type = ['controller_pb2.FSMCommand'],
                help = t.help,
                return_type = 'controller_pb2.FSMCommandResponse',
                arguments = t.arguments
            )
        )
    return desc

def decode_fsm_arguments(arguments, arguments_format):
    from drunc.utils.grpc_utils import unpack_any
    import drunc.fsm.fsm_errors as fsme
    from druncschema.generic_pb2 import int_msg, float_msg, string_msg, bool_msg
    from druncschema.controller_pb2 import Argument

    def get_argument(name, arguments):
        for n, k in arguments.items():
            if n == name:
                return k
        return None


    out_dict = {}
    for arg in arguments_format:
        arg_value = get_argument(arg.name, arguments)

        if arg.presence == Argument.Presence.MANDATORY and arg_value is None:
            raise fsme.MissingArgument(arg.name, '')

        if arg_value is None:
            arg_value = arg.default_value

        match arg.type:
            case Argument.Type.INT:
                out_dict[arg.name] = unpack_any(arg_value, int_msg).value
            case Argument.Type.FLOAT:
                out_dict[arg.name] = unpack_any(arg_value, float_msg).value
            case Argument.Type.STRING:
                out_dict[arg.name] = unpack_any(arg_value, string_msg).value
            case Argument.Type.BOOL:
                out_dict[arg.name] = unpack_any(arg_value, bool_msg).value
            case _:
                raise RuntimeError(f'Unhandled argument type {arg.type}')
    import logging
    l = logging.getLogger('decode_fsm_arguments')
    l.debug(f'Parsed FSM arguments: {out_dict}')
    return out_dict