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
        cmd = ["boot", "conf", "start", "scrap", "terminate"]
        for command in cmd:
            self.fsm.register_transition(command, getattr(self, command))
    
    def do_command(self, transition, data):
        self.fsm.execute_transition(transition, data)

    def get_state(self):
        return self.fsm.get_current_state()

    def boot(self, data):
        pass

    def conf(self, data):
        pass

    def start(self, data):
        pass

    def scrap(self, data):
        pass

    def terminate(self, data):
        pass

def main():
    filename = sys.argv[1]
    f = open(filename, 'r')
    config = json.loads(f.read())
    controller = FakeController(config)
    commands = ["boot", "conf", "start"]

    print(f"Current state is {controller.get_state()}")
    for c in commands:
        print(f"Trying {c}")
        try:
            controller.do_command(c, None)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()