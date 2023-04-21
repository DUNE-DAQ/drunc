import sys
import os
import json

this_dir = os.path.dirname(__file__)
fsm_dir = os.path.join(this_dir, '..', 'fsm')
sys.path.append(fsm_dir)
from fsm_core import FSM

class FakeController:
    def __init__(self, config):
        self.name = "controller"
        self.fsm = FSM(configuration=config)
        self.fsm.register_transition("boot", self.boot)
        self.fsm.register_transition("conf", self.conf)
        self.fsm.register_transition("scrap", self.scrap)
        self.fsm.register_transition("terminate", self.terminate)
    
    def do_command(self, transition, data):
        self.fsm.execute_transition(transition, data)

    def get_state(self):
        return self.fsm.get_current_state()

    def boot(self):
        pass

    def conf(self):
        pass

    def scrap(self):
        pass

    def terminate(self):
        pass

    

def main():
    filename = sys.argv[1]
    f = open(filename, 'r')
    config = json.loads(f.read())
    controller = FakeController(config)
    commands = ["boot", "conf", "scrap", "terminate"]

    print(f"Current state is {controller.get_state()}.")
    for c in commands:
        controller.do_command(c, None)
        print(f"Current state is {controller.get_state()}.")


if __name__ == '__main__':
    main()