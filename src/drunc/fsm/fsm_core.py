import logging
import
import interface_factory
from typing import List, Set, Dict, Tuple
from fsm_errors import *

class FSMInterface:
    '''Abstract class defining a generic interface'''
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
        self.states           = config_data['states']
        self.transitions      = config_data['transitions']
        self.sequences        = config_data['command_sequences']
        self.pre_transitions  = config_data['pre_transitions']
        self.post_transitions = config_data['post_transitions']
        self.interfaces = {}
        for name, data in config_data['interfaces'].items():
            self.interfaces[name] = interface_factory.FSMInterfacesFact.get(name, data)
        

class FSM:
    def __init__(self, configuration):
        self.current_state = "none"
        self.config = FSMConfig(configuration)
        self.transition_functions = {}
        self.log = logging.getLogger(self.__class__.__name__)

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

    def execute_transition(self, transition, data):
        if transition in self.config.sequences: 
            #TODO check nanorc.common_commands.py, should be like that
            for command in self.config.sequences[transition]:   #Format is {"cmd": "conf", "optional": true }
                try:                                            #Attempt every transition in the sequence
                    self.execute_transition(command['cmd'], data)
                except Exception as e:
                    if not command["optional"]:                 #If it fails and isn't optional
                        raise e                                 #Pass the error up and stop
            return

        #Check that the transition is valid from our state
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
        print(f"Current state is {self.current_state}")

        self.post_transition_sequence(transition, data)
           

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

    def get_transition_arguments(self, transition) -> dict:
        data = {}
        for interface_name in self.config.interfaces:
            data[interface_name] = interface.get_transition_arguments(transition)
        return data

    def pre_transition_sequence(self, transition, data) -> None:
        if transition in self.config.pre_transitions:
            responses = {}
            pre_data = self.config.pre_transitions[transition]      #Information relating to this pre-transition
            for name in pre_data['order']:                          #A list of all interfaces to be tried (in order)
                interface = self.config.interfaces[name]
                try:
                    response = interface.pre_transition(transition,data)
                    responses[name] = response
                except Exception as e:
                    if name in pre_data['mandatory']:                   #If the transition is required, raise an error
                        raise e
                    else:
                        self.log(e)

    def post_transition_sequence(self, transition, data) -> None:
        if transition in self.config.post_transitions:
            responses = {}
            post_data = self.config.post_transitions[transition]  
            for name in post_data['order']:
                interface = self.config.interfaces[name]
                try:
                    response = interface.post_transition(transition,data)
                    responses[name] = response
                except Exception as e:
                    if name in post_data['mandatory']:
                        raise e
                    else:
                        self.log(e)