import os
import sys
top_dir = os.environ['DRUNC_DIR']
fsm_dir = os.path.join(top_dir, 'src', 'drunc', 'fsm')
sys.path.append(fsm_dir)
from fsm_core import FSM

class FakeController:
    def __init__(self, config):
        self.name = "controller"
        self.fsm = FSM(configuration=config)
        methods = [attr for attr in dir(self) if callable(getattr(self, attr))] #Get every callable attribute (i.e methods)
        unmangled= [m for m in methods if m[0] != '_']                          #Filters out methods starting with a _
        unwanted = ["do_command", "get_state"]
        cmds = [c for c in unmangled if c not in unwanted]                      #Filters out non-FSM methods
        for command in cmds:
            self.fsm.register_transition(command, getattr(self, command))       #Passes every command to the FSM
    
    def do_command(self, transition, transition_data):
        self.fsm.execute_transition(transition, transition_data)

    def get_state(self):
        return self.fsm.get_current_state()

    def boot(self, data):
        pass

    def conf(self, data, a_number:int):
        pass

    def start(self, data):
        pass
    
    def enable_triggers(self, data):
        pass
    
    def disable_triggers(self, data):
        pass
    
    def drain_dataflow(self, data):
        pass
    
    def stop_trigger_sources(self, data):
        pass
    
    def stop(self, data):
        pass

    def scrap(self, data):
        pass

    def terminate(self, data):
        pass

    def abort(self, data):
        pass