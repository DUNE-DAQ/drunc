import pytest
import os
import json
from .fake_controller import FakeController

#Some command sequences for the controller to run
fsm_loop = ["boot", "conf", "start", "enable_triggers", "disable_triggers", 
            "drain_dataflow", "stop_trigger_sources", "stop", "scrap", "terminate"]
repeat = ["boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate", "boot", "terminate"]
sequences = ["boot", "start_run", "stop_run", "shutdown"]
#A list of config files that the test should be run on
filelist = ["fsm_configuration", "no_interfaces"]

#Sets of arguments are defined here to avoid repetition
argsets = {
    "no_args":    {"tr": {}},
    "fsm_conf":   {"tr": {"a_number":77}, "pre_test-interface": {}},
    "noint_conf": {"tr": {"a_number":77}},
    "fsm_start":  {"tr": {}, "post_run-number": {}, "post_logbook": {"message": "hello!"}},
    "fsm_stop":   {"tr": {}, "post_logbook": {}}
}

path = os.path.join(os.environ['DRUNC_DIR'], 'data', 'fsm')
args_filepath = path + '/fsm_test_args.json'
f = open(args_filepath, 'r')
all_args_data = json.loads(f.read())
f.close()

@pytest.fixture(params = filelist)
def make_controller(request):
    '''Generate a controller for each config provided.'''
    conf_filename = f"{request.param}.json"
    conf_filepath = path + '/' + conf_filename
    f = open(conf_filepath, 'r')
    config = json.loads(f.read())
    f.close()
    return FakeController(config), request.param

@pytest.fixture(params = [fsm_loop, repeat, sequences])
def run_commands(make_controller, request):
    err_list = []
    controller = make_controller[0]
    this_conf = make_controller[1]
    config_args_data = all_args_data[this_conf]
    for cmd in request.param:
        try:
            if cmd in config_args_data:
                to_send = config_args_data[cmd]
                if type(to_send) == str:                            #If the value for the listed command is a string...
                    controller.do_command(cmd, argsets[to_send])    #...send the relevent data.
                else:                                               #Otherwise, it must be a sequence.
                    #Take the dict and replace the names of the argsets with the actual values.
                    sequence_data = {k:argsets[v] for (k,v) in to_send.items()} 
                    controller.do_command(cmd, sequence_data)
            else:
                controller.do_command(cmd, argsets['no_args'])
        except Exception as e:
            err_list.append(e)
    return err_list

def test_runtime(run_commands):
    assert run_commands == []