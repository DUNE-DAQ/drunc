from enum import Enum

class State(Enum):
    '''
    An abstraction for the states available
    '''
    NONE = 0 # the only one we are guaranteed to have

class Transition(Enum):
    '''
    An abstraction for the states available
    '''
    BOOT = 0
    TERMINATE = 1

class FSMPlugin:
    def __init__(self, configuration):
        pass

    def pre_transition(self):
        pass

    def post_transition(self):
        pass

class FSM:
    def __init__(self, controlled_object, configuration):
        self.current_state = State.NONE
        self.plugins = [] # user defined plugins
        self.controlled_object = controlled_object # the controller
        ## should now figure out the states and the transitions from configuration

    def get_all_states(self) -> list[State]:
        '''
        grabs all the states
        '''
        pass

    def get_all_transitions(self) -> list[Transition]:
        '''
        grab all the transitions
        '''
        pass

    def get_executable_transitions(self) -> list[Transition]:
        pass

    def can_execute_transition(self, transition) -> bool:
        '''
        Check that this transition is allowed given the state the FSM is right now
        '''
        pass

    def get_plugins(self, name) -> FSMPlugin:
        pass

    def execute_transition(self, data) -> bool:
        self.pre_transition_sequence(data)
        # transition, update the state etc.
        # There should be a hook to the controller here, something like:
        # controlled_object.execute_transition()...
        self.post_transition_sequence(data)


    def pre_transition_sequence(self, data) -> None:
        for plugin in self.plugins:
            try:
                data = plugin.pre_transition(data)
            except Exception as e:
                # log exception
                pass


    def post_transition_sequence(self, data) -> None:
        for plugin in self.plugins:
            try:
                data = plugin.post_transition(data)
            except Exception as e:
                # log exception
                pass