#TODO Fix type indicators
import plugin_factory
from typing import List, Set, Dict, Tuple
from fsm_errors import *

'''
class State(Enum):
    #An abstraction for the states available
    NONE = 0 # the only one we are guaranteed to have

class Transition(Enum):
    #An abstraction for the transitions available
    BOOT = 0
    TERMINATE = 1
'''

class FSMPlugin:
    '''Abstract class defining a generic plugin'''
    def __init__(self, name):
        self.name = name

    def pre_transition(self, transition, data):
        '''
        For the given transition, check if we have anything to do before it, then do it.
        '''
        func = getattr(self, "pre_"+transition, None)
        if func:
            func(data)

    def post_transition(self, transition, data):
        func = getattr(self, "post_"+transition, None)
        if func:
            func(data)


class FSMConfig:
    def __init__(self, config_data):
        '''
        Takes a config.json describing the FSM, and stores it as a class object
        '''
        self.states = config_data['states']
        self.transitions = config_data['transitions']
        self.sequences = config_data['command_sequences']
        self.plugins = []
        for name, data in config_data['plugins'].items():
            self.plugins.append(plugin_factory.FSMInterfacesFact.get(name, data))

class FSM:
    def __init__(self, configuration):
        self.current_state = "none"
        self.config = FSMConfig(configuration)
        self.transition_functions = {}

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
            if self.can_execute_transition(tr):
                valid_transitions.append(tr)
        return valid_transitions

    def can_execute_transition(self, transition) -> bool:
        '''
        Check that this transition is allowed given the state the FSM is right now
        '''
        right_name = [t for t in self.config.transitions if t['trigger'] == transition]
        for tr in right_name:
        #We allow states that start in the state we are in, or ones that can start anywhere.
            if tr['source'] == '*' or tr['source'] == self.current_state:
                return True
        return False

    def execute_transition(self, transition, data) -> bool:
        #check first that the transition is valid
        if not self.can_execute_transition(transition):
            raise InvalidTransition(transition, self.current_state)

        func = self.transition_functions.get(transition)
        if not func:
            raise UnregisteredTransition(transition)

        self.pre_transition_sequence(transition, data)
        #Look for the correctly named method of the controller, then call it
        func(data)
        #Assuming it worked, update our state
        self.current_state = self.get_destination(transition)

        self.post_transition_sequence(transition, data)
           

    def get_plugins(self, name) -> List[str]:
        return self.config.plugins.keys()

    def register_transition(self, name, func) -> None:
        '''
        The controller passes its transition methods down to the FSM, so that they can be called later.
        '''
        self.transition_functions[name] = func

    def get_transition_arguments(self, transition) -> dict:
        data = {}
        for plugin in self.config.plugins:
            data[plugin_name] = plugin.get_transition_arguments(transition)
        return data

    def pre_transition_sequence(self, transition, data) -> None:
        for plugin in self.config.plugins:
            try:
                response = plugin.pre_transition(transition,data)   #TODO sometimes the plugins need to know an order to be called in
            except Exception as e:              #TODO some plugins can fail, some must stop execution if they do
                # log exception
                pass

    def post_transition_sequence(self, transition, data) -> None:
        for plugin in self.config.plugins:
            try:
                response = plugin.post_transition(data)
            except Exception as e:
                # log exception
                pass