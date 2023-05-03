import pytest
from fake_controller import FakeController

#Some command sequences for the controller to run
fsm_loop = ["boot", "conf", "start", "enable_triggers", "disable_triggers", 
            "drain_dataflow", "stop_trigger_sources", "stop", "scrap", "terminate"]
repeat = ["boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate"]
sequences = ["boot", "start_run", "stop_run", "shutdown"]

@pytest.fixture
def make_controller():
    this_dir = os.path.dirname(__file__)
    path = os.path.join(this_dir, '..', '..', '..', 'data', 'fsm')
    filename = "asdsa"
    f = open(filename, 'r')
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