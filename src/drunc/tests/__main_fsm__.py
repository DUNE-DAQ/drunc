import sys
import os
import json
from .fake_controller import FakeController

def main():
    filename = sys.argv[1]
    f = open(filename, 'r')
    config = json.loads(f.read())
    f.close()
    controller = FakeController(config)
    commands = sys.argv[2:]     #Args should be drunc-fsm-tests, then the config filename, then a list of commands

    for c in commands:
        try:
            print(f"Trying {c}")
            controller.do_command(c, None)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    main()