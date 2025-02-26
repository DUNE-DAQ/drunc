import tempfile

import pytest

from drunc.exceptions import DruncException
from drunc.fsm.actions.utils import get_dotdrunc_json, validate_run_type
from drunc.fsm.exceptions import DotDruncJsonIncorrectFormat, DotDruncJsonNotFound


def test_get_dotdrunc_json():
    dotdrunc = get_dotdrunc_json()
    assert dotdrunc is not None

    with pytest.raises(DotDruncJsonNotFound):
        get_dotdrunc_json("nonexistent_path")

    with tempfile.NamedTemporaryFile(delete=True) as f:
        f.write(b'{"test"; "test"}')
        f.flush()

        with pytest.raises(DotDruncJsonIncorrectFormat):
            get_dotdrunc_json(f.name)


def test_validate_run_type():
    assert validate_run_type("PROD") == "PROD"
    assert validate_run_type("TEST") == "TEST"
    with pytest.raises(DruncException):
        validate_run_type("INVALID")
