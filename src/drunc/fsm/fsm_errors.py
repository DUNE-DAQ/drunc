class InvalidTransition(Exception):
    '''Raised when the transition isn't in the list of currently accessible transitions'''
    def __init__(self, transition, state):
        self.message = f"Transition {transition} is not allowed from the state {state}."
        super().__init__(self.message)

class UnregisteredTransition(Exception):
    '''Raised when the transition is allowed, but we can't find the implementation'''
    def __init__(self, transition):
        self.message = f"Implementation of {transition} not found."
        super().__init__(self.message)

class UnknownPlugin(Exception):
    '''Raised when a plugin name is provided that does not correspond to any files in /plugins'''
    def __init__(self, name):
        self.message = f"\"{name}\" is not a known plugin."
        super().__init__(self.message)