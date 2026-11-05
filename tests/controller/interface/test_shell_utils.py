import pytest
from druncschema.controller_pb2 import Argument

from drunc.controller.interface import shell_utils


@pytest.mark.parametrize(
    ("argument_type", "field", "input_value", "expected"),
    [
        (Argument.Type.INT, "int_val", "42", 42),
        (Argument.Type.FLOAT, "float_val", "1.25", 1.25),
        (Argument.Type.STRING, "string_val", "hello", "hello"),
        (Argument.Type.BOOL, "bool_val", "false", False),
        (Argument.Type.BOOL, "bool_val", "yes", True),
    ],
)

def test_argument_type_mapping(argument_type, field, input_value, expected):
    """Convert input type into the right ScalarArgument field."""

    argument = Argument(
        name="value",
        type=argument_type,
        presence=Argument.Presence.OPTIONAL,
    )

    result = shell_utils.validate_and_format_fsm_arguments(
        {"value": input_value}, [argument]
    )

    assert getattr(result["value"], field) == expected


def test_validate_and_format_fsm_arguments_false_bool():
    """Check the string 'false' is treated as False, not as string."""
    argument = Argument(
        name="enabled",
        type=Argument.Type.BOOL,
        presence=Argument.Presence.OPTIONAL,
    )

    result = shell_utils.validate_and_format_fsm_arguments(
        {"enabled": "false"}, [argument]
    )

    assert result["enabled"].bool_val is False


def test_validate_and_format_fsm_arguments_missing_argument():
    """Check that a missing mandatory argument raises an exception."""
    argument = Argument(
        name="count",
        type=Argument.Type.INT,
        presence=Argument.Presence.MANDATORY,
    )

    with pytest.raises(shell_utils.MissingArgument):
        shell_utils.validate_and_format_fsm_arguments({}, [argument])
