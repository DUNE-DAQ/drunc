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

#We define the argument dicts up here to avoid repetiton
no_args   =  {'tr': {}}
fsm_conf  =  {'tr': {'a_number':77}, 'pre_test-interface': {}}
noint_conf = {'tr': {'a_number':77}}
fsm_start =  {'tr': {}, 'post_run-number': {}, 'post_logbook': {'message': "hello!"}}
fsm_stop  =  {'tr': {}, 'post_logbook': {}}
#This dict maps commands to the arguments that should be provided.
#The sequences make this very big, so there is hopefully a better way of doing it.
arglist = {
    'fsm_configuration': {
        'conf':                     fsm_conf,
        'start':                    fsm_start,
        'stop':                     fsm_stop,
        'start_run': {
            'conf':                 fsm_conf,
            'start':                fsm_start,
            'enable_triggers':      no_args
        },
        'stop_run': {
            'disable_triggers':     no_args,
            'drain_dataflow':       no_args,
            'stop_trigger_sources': no_args,
            'stop':                 fsm_stop
        },
        'shutdown': {
            'disable_triggers':     no_args,
            'drain_dataflow':       no_args,
            'stop_trigger_sources': no_args,
            'stop':                 fsm_stop,
            'scrap':                no_args,
            'terminate':            no_args
        }
    },
    'no_interfaces': {
        'conf': noint_conf,
        'start_run': {
            'conf':                 noint_conf,
            'start':                no_args,
            'enable_triggers':      no_args
        },
        'stop_run': {
            'disable_triggers':     no_args,
            'drain_dataflow':       no_args,
            'stop_trigger_sources': no_args,
            'stop':                 no_args
            },
        'shutdown': {
            'disable_triggers':     no_args,
            'drain_dataflow':       no_args,
            'stop_trigger_sources': no_args,
            'stop':                 no_args,
            'scrap':                no_args,
            'terminate':            no_args
        }
    }
}

@pytest.fixture(params = filelist)
def make_controller(request):
    '''Generate a controller for each config provided.'''
    this_dir = os.path.dirname(__file__)
    path = os.path.join(this_dir, '..', '..', '..', 'data', 'fsm')
    filename = f"{request.param}.json"
    filepath = path + '/' + filename
    print(filepath)
    f = open(filepath, 'r')
    config = json.loads(f.read())
    f.close()
    return FakeController(config), request.param

@pytest.fixture(params = [fsm_loop, repeat, sequences])
def run_commands(make_controller, request):
    err_list = []
    controller = make_controller[0]
    this_conf = make_controller[1]
    for cmd in request.param:
        try:
            if cmd in arglist[this_conf]:
                controller.do_command(cmd, arglist[this_conf][cmd])
            else:
                controller.do_command(cmd, no_args)
        except Exception as e:
            err_list.append(e)
    return err_list

def test_runtime(run_commands):
    assert run_commands == []