import requests

from drunc.process_manager.oks_parser import collect_variables


class DummyVariable:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def className(self):
        return "Variable"


class DummyVariableSet:
    def __init__(self, contains):
        self.contains = contains

    def className(self):
        return "VariableSet"


def test_collect_variables_flattens_nested_variable_sets():
    env = {}
    variables = [
        DummyVariableSet(
            [
                DummyVariable("APP_NAME", "demo"),
                DummyVariableSet(
                    [
                        DummyVariable("HOST", "localhost"),
                        DummyVariable("PORT", "1234"),
                    ]
                ),
            ]
        )
    ]

    collect_variables(variables, env)

    assert env == {
        "APP_NAME": "demo",
        "HOST": "localhost",
        "PORT": "1234",
    }


def test_connectivity_service_started(one_controller_running):
    session_dal = one_controller_running[1]

    r = requests.get(
        f"http://localhost:{session_dal.connectivity_service.service.port}", timeout=2
    )

    r.raise_for_status()
    assert r.status_code == 200, "Connectivity service did not start in time"
