
def convert_fsm_arguments(args):
    from druncschema.controller_pb2 import Argument
    # if not signature:
    # signature = [<Parameter "some_int: int">,
    #              <Parameter "some_str: str">,
    #              <Parameter "some_float: float">,
    # ]
    retr = []
    from inspect import Parameter

    for p in args:
        default_value = ''

        t = Argument.Type.INT
        from druncschema.generic_pb2 import string_msg, float_msg, int_msg
        from drunc.utils.grpc_utils import pack_to_any

        if p.annotation is str:
            t = Argument.Type.STRING

            if p.default != Parameter.empty:
                default_value = pack_to_any(string_msg(value = p.default))

        elif p.annotation is float:
            t = Argument.Type.FLOAT

            if p.default != Parameter.empty:
                default_value = pack_to_any(float_msg(value = p.default))

        elif p.annotation is int:
            t = Argument.Type.INT

            if p.default != Parameter.empty:
                default_value = pack_to_any(int_msg(value = p.default))
        else:
            raise RuntimeError(f'Annotation {p.annotation} is not handled.')

        a = Argument(
            name = p.name,
            presence = Argument.Presence.MANDATORY if p.default == Parameter.empty else Argument.Presence.OPTIONAL,
            type = t,
            help = '',
        )

        if default_value:
            a.default_value.CopyFrom(default_value)

        retr += [a]
        print(a)
    return retr


def convert_fsm_transition(fsm):
    from druncschema.controller_pb2 import FSMCommandsDescription, FSMCommandDescription
    desc = FSMCommandsDescription()
    for t in fsm.get_executable_transitions():
        desc.commands.append(
            FSMCommandDescription(
                name = t,
                data_type = ['controller_pb2.FSMCommand'],
                help = t,
                return_type = 'controller_pb2.FSMCommandResponse',
                arguments = convert_fsm_arguments(fsm.get_transition_arguments(t))
            )
        )
    return desc

