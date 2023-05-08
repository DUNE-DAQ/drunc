import pytest
import os
import json
from fake_controller import FakeController

#Some command sequences for the controller to run
fsm_loop = ["boot", "conf", "start", "enable_triggers", "disable_triggers", 
            "drain_dataflow", "stop_trigger_sources", "stop", "scrap", "terminate"]
repeat = ["boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate"]
sequences = ["boot", "start_run", "stop_run", "shutdown"]
#A list of config files that the test should be run on
filelist = ["fsm_configuration.json", "fake_controller.json"]

@pytest.fixture(params = filelist)
def make_controller():
    this_dir = os.path.dirname(__file__)
    path = os.path.join(this_dir, '..', '..', '..', 'data', 'fsm')
    filename = "fsm_configuration.json"
    filepath = path + '/' + filename
    print(filepath)
    f = open(filepath, 'r')
    config = json.loads(f.read())
    f.close()
    return FakeController(config)

@pytest.fixture(params = [fsm_loop, repeat, sequences])
def run_commands(make_controller, request):
    err_list = []
    for cmd in request.param:
        try:
            make_controller.do_command(cmd, None)
        except Exception as e:
            err_list.append(e)
    return err_list

def test_runtime(run_commands):
    assert run_commands == []