import sys
import os
import json
from .fake_controller import FakeController

def request_args(controller, name, i_name):
    interface = controller.fsm.config.interfaces[i_name]
    sig = interface.get_transition_arguments(name)          #The signature of the function
    p_list = []
    for p in sig.parameters.keys():
        if p not in ("self", "data"):
            p_list.append(p)
    if p_list == []:
        return None
    print(f"Enter the following parameters for {name} of {i_name}:")
    print(*p_list)
    inp = input("|DRUNC> ")
    return dict(zip(p_list, inp.split()))

def main():
    #Args should be drunc-fsm-tests, then the config filename
    filename = sys.argv[1]
    f = open(filename, 'r')
    config = json.loads(f.read())
    f.close()
    controller = FakeController(config)
    while True:
        cmd = input("|DRUNC> ")
        if cmd == "quit":
            break
        if cmd == "ls":
            print(controller.fsm.get_executable_transitions())
            continue
        if cmd == "here":
            print(controller.fsm.get_current_state())
            continue
        
        if not controller.fsm.can_execute_transition(cmd):
            print(f"\"{cmd}\" is not allowed")
            continue

        sig = controller.fsm.get_transition_arguments(cmd)
        p_list = []
        for p in sig.parameters.keys():
            if p not in ("self", "data"):
                p_list.append(p)
        if p_list != []:
            print(f"Enter the following parameters in order for {cmd}:")
            print(*p_list)
            inp = input("|DRUNC> ")
            data = inp.split()
            all_args = {'tr':process_args(p_list, data)}
        else:
            all_args = {'tr':{}}

        pre = controller.fsm.config.pre_transitions
        if cmd in pre:                          #If there are any pre-transitions for this command
            name = "pre_"+cmd
            for i_name in pre[cmd]['order']:    #For each interface with a transition (in order)
                data = request_args(controller, name, i_name)
                if data:
                    arg_name = "pre_"+i_name
                    all_args[arg_name] = data

        post = controller.fsm.config.post_transitions
        if cmd in post:
            name = "post_"+cmd
            for i_name in post[cmd]['order']:
                data = request_args(controller, name, i_name)
                if data:
                    arg_name = "post_"+i_name
                    all_args[arg_name] = data
        try:
            print(f"Sending command \"{cmd}\"")
            controller.do_command(cmd, all_args)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()