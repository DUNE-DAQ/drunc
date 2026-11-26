from typing import MutableMapping, Optional

from druncschema.controller_pb2 import (
    Argument,
    FSMCommandDescription,
    FSMCommandsDescription,
    ScalarArgument,
)

# prevent confusion with typing.Any
import drunc.fsm.exceptions as fsme
from drunc.fsm.transition import Transition


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
    for t in transitions:
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


def decode_fsm_arguments(
    arguments: MutableMapping[str, ScalarArgument], arguments_format: list[Argument]
) -> dict[str, str | int | float | bool]:
    """
    Decodes the arguments of a FSM command.

    Note there is separate logic to validate whether the required arguments are all
    present at the click.core.Command level, this is a safeguard to support the multiple
    drunc operating modes.

    Args:
        arguments (MutableMapping[str, PbAny]): The arguments to decode.
        arguments_format (list[Argument]): The format of the arguments.

    Returns:
        dict[str, str | int | float | bool]: The decoded arguments.

    Raises:
        fsme.MissingArgument: If a mandatory argument is missing.
        fsme.UnhandledArgumentType: If an argument type is not handled.
    """

    # Added explicit type hints to the nested function
    def get_argument(
        name: str, args: MutableMapping[str, ScalarArgument]
    ) -> Optional[ScalarArgument]:
        for n, k in args.items():
            if n == name:
                return k
        return None
    
    # Map each Argument.Type to the corresponding ScalarArgument oneof field
    field_names = {
        Argument.Type.INT: "int_val",
        Argument.Type.FLOAT: "float_val",
        Argument.Type.STRING: "string_val",
        Argument.Type.BOOL: "bool_val",
    }

    # Explicitly type out_dict
    out_dict: dict[str, str | int | float | bool] = {}

    for arg in arguments_format:
        arg_value: Optional[ScalarArgument] = get_argument(arg.name, arguments)

        if arg.presence == Argument.Presence.MANDATORY and arg_value is None:
            raise fsme.MissingArgument(arg.name, "")

        if arg.type not in field_names:
            raise fsme.UnhandledArgumentType(str(arg.type))

        if arg_value is None:
            # default_value is also a ScalarArgument, no unpacking needed
            arg_value = arg.default_value

        out_dict[arg.name] = getattr(arg_value, field_names[arg.type])

    return out_dict