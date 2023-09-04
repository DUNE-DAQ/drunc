import logging
from drunc.fsm.interface_factory import FSMInterfaceFactory
from typing import List, Set, Dict, Tuple
from inspect import signature, Parameter
import drunc.fsm.fsm_errors

def validate_arguments(obj, func_name, arguments) -> None:
    '''
    Checks that each argument of the function is provided if required,
    and has the correct type if there is an annotation.
    An exception is raised otherwise.
    We also extract arguments from FSM.run_data here
    '''
    sig = obj.get_transition_arguments(func_name)
    for p in sig.parameters.keys():
        if p in ("self", "data"):           #ignore these
            continue
        fsm = False                         #Whether we got data from the FSM
        default = sig.parameters[p].default
        typing = sig.parameters[p].annotation
        #If there is no default, an arg must be provided
        if default == Parameter.empty and p not in arguments:
            raise fsm_errors.MissingArgument(p, func_name)
        #We should obey any annotations, assuming a value was provided
        if p in arguments:
            if typing != Parameter.empty and type(arguments[p]) != typing:
                message = f"{type(arguments[p]).__name__} is the wrong type for {p} (should be {typing.__name__})"
                raise TypeError(message)
    for arg in arguments:
        #We shouldn't be providing any arguments that the function doesn't ask for
        if arg not in sig.parameters.keys():
            raise fsm_errors.UnknownArgument(arg, func_name)

class FSMInterface:
    '''Abstract class defining a generic interface'''
    def __init__(self, name):
        self.name = name

    def get_transition_arguments(self, func_name) -> signature:
        func = getattr(self, func_name, None)
        if not func:
            return None
        sig = signature(func)
        return sig

    def pre_transition(self, transition, run_data, arguments):
        '''
        For the given transition, check if we have anything to do before it, then do it.
        '''
        name = "pre_"+transition
        func = getattr(self, "pre_"+transition, None)
        if func:
            validate_arguments(self, name, arguments)  #This will raise an error if something is wrong
            return func(run_data, **arguments)              #The run_data is passed as a dictionary, whereas arguments are unpacked
        else:
            return None


    def post_transition(self, transition, run_data, arguments):
        '''
        For the given transition, check if we have anything to do after it, then do it.
        '''
        name = "post_"+transition
        func = getattr(self, "post_"+transition, None)
        if func:
            validate_arguments(self, name, arguments)
            return func(run_data, **arguments)
        else:
            return None

class FSMConfig:
    def __init__(self, config_data, config_type='file'):
        '''
        Takes a config.json describing the FSM, and stores it as a class object
        '''
        if config_type == 'file':
            self.states           = config_data['states']
            self.transitions      = config_data['transitions']
            self.sequences        = config_data['command_sequences']
            self.pre_transitions  = config_data['pre_transitions']
            self.post_transitions = config_data['post_transitions']
            self.interfaces = {}
            for name, data in config_data['interfaces'].items():
                self.interfaces[name] = FSMInterfaceFactory.get().get_interface(name, data)

        elif 'oks':
            raise RuntimeError(f'config type {config_type} is not supported')

        else:
            raise RuntimeError(f'config type {config_type} is not supported')


class FSM:
    def __init__(self, configuration):
        self.current_state = "none"
        self.config = FSMConfig(configuration)
        self.transition_functions = {}
        self.log = logging.getLogger(self.__class__.__name__)
        self.run_data = {}

    def get_all_states(self) -> List[str]:
        '''
        grabs all the states
        '''
        return self.config.states

    def get_all_transitions(self) -> List[dict]:
        '''
        grab all the transitions
        '''
        return self.config.transitions

    def get_current_state(self) -> str:
        '''
        returns our current state
        '''
        return self.current_state

    def get_destination(self, transition) -> str:
        '''
        Tells us where a particular transition will take us, given our current state
        '''
        right_name = [t for t in self.config.transitions if t['trigger'] == transition]
        for tr in right_name:
            if tr['source'] == '*' or tr['source'] == self.current_state:
                return tr['dest']

    def get_executable_transitions(self) -> List[dict]:
        valid_transitions = []
        for tr in self.config.transitions:
            if self.can_execute_transition(tr['trigger']):
                valid_transitions.append(tr['trigger'])
        return valid_transitions

    def can_execute_transition(self, transition) -> bool:
        '''
        Check that this transition is allowed given the state the FSM is in right now
        '''
        right_name = [t for t in self.config.transitions if t['trigger'] == transition]
        for tr in right_name:
        #We allow states that start in the state we are in, or ones that can start anywhere.
            if tr['source'] == '*' or tr['source'] == self.current_state:
                return True
        return False

    def execute_transition(self, transition, transition_data):
        '''
        Transition data has format {tr:{arg1:val1, arg2:val2, ...} pre_int1:{arg1:val1, arg2:val2, ...}, ...}
        As well as the first dict for the transition, there may be more for relevent interfaces.
        The keys are pre_int or post_int, where int is the interface name
        If we are executing a sequence, it will be a dict with each transition as keys, and objects like this as values.
        '''
        if transition in self.config.sequences:
            #TODO check nanorc.common_commands.py, should be like that
            for command in self.config.sequences[transition]:   #"command" has format {"cmd": "conf", "optional": true }
                try:                                            #Attempt every transition in the sequence
                    self.execute_transition(command['cmd'], transition_data[command['cmd']])
                except Exception as e:
                    if not command["optional"]:                 #If it fails and isn't optional
                        raise e                                 #Pass the error up and stop
            return

        #Check that the transition is valid from our state
        if not self.can_execute_transition(transition):
            raise fsm_errors.InvalidTransition(transition, self.current_state)

        func = self.transition_functions.get(transition)
        if not func:
            raise fsm_errors.UnregisteredTransition(transition)

        pre_data = {k:v for (k,v) in transition_data.items() if "pre_" in k}
        self.pre_transition_sequence(transition, pre_data)
        #Look for the correctly named method of the controller, then call it
        tr_data = transition_data['tr']
        validate_arguments(self, transition, tr_data)
        func(self.run_data, **tr_data)
        #Assuming it worked, update our state
        self.current_state = self.get_destination(transition)

        post_data = {k:v for (k,v) in transition_data.items() if "post_" in k}
        self.post_transition_sequence(transition, post_data)

    def get_interfaces(self, name) -> List[str]:
        return self.config.interfaces.keys()

    def process_responces(self, responses):
        '''
        A dictionary of all the responses from the interfaces, for this pre/post transition.
        '''
        for name, response in responses:
            pass

    def register_transition(self, name, func) -> None:
        '''
        The controller passes its transition methods down to the FSM, so that they can be called later.
        '''
        self.transition_functions[name] = func

    def get_transition_arguments(self, func_name) -> signature:
        if func_name not in self.transition_functions:
            return None
        func = self.transition_functions[func_name]
        sig = signature(func)
        return sig

    def pre_transition_sequence(self, transition, pre_data) -> None:
        if transition in self.config.pre_transitions:
            info = self.config.pre_transitions[transition]          #Information relating to this pre-transition
            for name in info['order']:                              #A list of all interfaces to be tried (in order)
                interface = self.config.interfaces[name]
                try:
                    search_name = "pre_"+name
                    if search_name in pre_data:
                        data = pre_data[search_name]
                    else:
                        data = {}                                   #We may still want to call with no arguments
                    response = interface.pre_transition(transition, self.run_data, data)
                    if response:                                    #The response is a tuple of a name and value (or maybe a list of tuples)
                        self.run_data.update(response)              #We save to run_data
                except Exception as e:
                    if name in info['mandatory']:                   #If the transition is required, raise an error
                        raise e
                    else:
                        self.log(e)

    def post_transition_sequence(self, transition, post_data) -> None:
        if transition in self.config.post_transitions:
            info = self.config.post_transitions[transition]
            for name in info['order']:
                interface = self.config.interfaces[name]
                try:
                    search_name = "post_"+name
                    if search_name in post_data:
                        data = post_data[search_name]
                    else:
                        data = {}
                    response = interface.post_transition(transition, self.run_data, data)
                    if response:
                        self.run_data.update(response)
                except Exception as e:
                    if name in info['mandatory']:
                        raise e
                    else:
                        self.log(e)
