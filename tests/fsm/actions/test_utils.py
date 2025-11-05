import json
import os
import tempfile

import pytest

from drunc.exceptions import DruncException
from drunc.fsm.actions.utils import get_dotdrunc_json, validate_run_type
from drunc.fsm.exceptions import DotDruncJsonIncorrectFormat, DotDruncJsonNotFound

dotdrunc_json = {
    "run_registry_configuration": {
        "socket": "http://bananas:1234",
        "user": "jcvandamme",
        "password": "karate",
    },
    "run_number_configuration": {
        "socket": "http://bananas:1234",
        "user": "jcvandamme",
        "password": "karate",
    },
    "elisa_configuration": {
        "some-detector": {
            "socket": "http://bananas:1234",
            "user": "jcvandamme",
            "password": "karate",
        },
    },
}


def test_get_dotdrunc_json():
    with tempfile.NamedTemporaryFile(delete=True, mode="w") as f:
        f.write(json.dumps(dotdrunc_json))
        f.flush()

        dotdrunc = get_dotdrunc_json(f.name)
        assert dotdrunc is not None

    if os.path.exists(os.path.expanduser("~/.drunc.json")):
        dotdrunc = get_dotdrunc_json()
        assert dotdrunc is not None

    with pytest.raises(DotDruncJsonNotFound):
        get_dotdrunc_json("nonexistent_path")

    with tempfile.NamedTemporaryFile(delete=True, mode="w") as f:
        f.write('{"test": "test"}')
        f.flush()

        with pytest.raises(DotDruncJsonIncorrectFormat):
            get_dotdrunc_json(f.name)


def test_validate_run_type():
    assert validate_run_type("PROD") == "PROD"
    assert validate_run_type("TEST") == "TEST"
    with pytest.raises(DruncException):
        validate_run_type("INVALID")
