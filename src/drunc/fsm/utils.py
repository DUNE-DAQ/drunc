from typing import MutableMapping

from druncschema.controller_pb2 import (
    Argument,
    FSMCommandDescription,
    FSMCommandsDescription,
)
from druncschema.generic_pb2 import bool_msg, float_msg, int_msg, string_msg
from google.protobuf.any_pb2 import Any

import drunc.fsm.exceptions as fsme
from drunc.fsm.transition import Transition
from drunc.utils.grpc_utils import unpack_any


def convert_fsm_transition(transitions: list[Transition]) -> FSMCommandsDescription:
    """
    Converts a list of FSM transitions to a FSMCommandsDescription.

    Args:
        transitions (list[Transition]): A list of FSM transitions.

    Returns:
        FSMCommandsDescription: A FSMCommandsDescription containing the converted 
            transitions.
    
    Raises:
        None.
    """
    commands_description = FSMCommandsDescription()
    for t in transitions: # type: Transition
        commands_description.commands.append(
            FSMCommandDescription(
                name=t.name,
                data_type=["controller_pb2.FSMCommand"],
                help=t.help,
                return_type="controller_pb2.ExecuteFSMCommandResponse",
                arguments=t.arguments,
            )
        )
    return commands_description


def decode_fsm_arguments(arguments: MutableMapping[str, Any], arguments_format: list[Argument]) -> dict[str, str | int | float | bool]:
    """
    Decodes the arguments of a FSM command.

    Note there is separate logic to validate whether the required arguments are all 
    present at the click.core.Command level, this is a safeguard to support the multiple
    drunc operating modes.

    Args:
        arguments (MutableMapping[str, Any]): The arguments to decode.
        arguments_format (list[Argument]): The format of the arguments.

    Returns:
        dict[str, str | int | float | bool]: The decoded arguments.

    Raises:
        fsme.MissingArgument: If a mandatory argument is missing.
        fsme.UnhandledArgumentType: If an argument type is not handled.
    """

    def get_argument(name, arguments):
        for n, k in arguments.items():
            if n == name:
                return k
        return None

    out_dict = {}
    for arg in arguments_format:
        arg_value = get_argument(arg.name, arguments)

        if arg.presence == Argument.Presence.MANDATORY and arg_value is None:
            raise fsme.MissingArgument(arg.name, "")

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
                raise fsme.UnhandledArgumentType(arg.type)
    return out_dict
