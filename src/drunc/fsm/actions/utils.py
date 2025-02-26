import json

from drunc.fsm.exceptions import (
    DotDruncJsonIncorrectFormat,
    DotDruncJsonNotFound,
    InvalidRunType,
)
from drunc.utils.utils import expand_path


def validate_run_type(run_type: str) -> str:
    """
    Validate the run type
    :param run_type: the run type
    :return: the validated run type
    """
    RUN_TYPES = ["PROD", "TEST"]
    if run_type not in RUN_TYPES:
        raise InvalidRunType(
            f"Invalid run type: '{run_type}'. Must be one of {RUN_TYPES}"
        )
    return run_type


def get_dotdrunc_json(path: str = "~/.drunc.json"):
    try:
        f = open(expand_path(path))
        dotdrunc = json.load(f)
    except FileNotFoundError:
        raise DotDruncJsonNotFound(f"dotdrunc file not found: '{path}'")
    except json.JSONDecodeError as exc:
        raise DotDruncJsonIncorrectFormat(
            f"dotdrunc file is not a valid JSON: '{path}'"
        ) from exc

    expected_keys = [
        "run_registry_configuration",
        "run_number_configuration",
        "elisa_configuration",
    ]

    if not all(key in dotdrunc for key in expected_keys):
        raise DotDruncJsonIncorrectFormat(
            f"dotdrunc file is missing some expected keys: {expected_keys}"
        )

    return dotdrunc
