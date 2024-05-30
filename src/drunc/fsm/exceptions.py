from drunc.exceptions import DruncCommandException
class FSMException(DruncCommandException):
    pass


class NoTransitionOfName(FSMException):
    def __init__(self, transition_name):
        self.message = f'Transition "{transition_name}" does not exist'
        super(NoTransitionOfName, self).__init__(self.message)

class DuplicateTransition(FSMException):
    '''
    If a transition has the same name as another
    '''
    def __init__(self, transition_name):
        self.message = f'Transition "{transition_name}" is a duplicate'
        super(DuplicateTransition, self).__init__(self.message)

class InvalidTransition(FSMException):
    '''Raised when the transition isn't in the list of currently accessible transitions'''
    def __init__(self, transition, state):
        self.message = f"Transition {transition} is not allowed from the state {state}."
        super(InvalidTransition, self).__init__(self.message)

class UnregisteredTransition(FSMException):
    '''Raised when the transition is allowed, but we can't find the implementation'''
    def __init__(self, transition):
        self.message = f"Implementation of {transition} not found."
        super(UnregisteredTransition, self).__init__(self.message)

class UnknownAction(FSMException):
    '''Raised when a plugin name is provided that does not correspond to any files in /plugins'''
    def __init__(self, name):
        self.message = f"\"{name}\" is not a known plugin."
        super(UnknownAction, self).__init__(self.message)

class MissingArgument(FSMException):
    '''Raised when a mandatory argument is not provided for a transition'''
    def __init__(self, param, name):
        self.message = f"The mandatory argument \"{param}\" was not provided to the transition {name}"
        super(MissingArgument, self).__init__(self.message)

class DoubleArgument(FSMException):
    '''Raised when an argument is provided more than once'''
    def __init__(self, txt):
        self.message = txt
        super(DoubleArgument, self).__init__(self.message)

class UnhandledArgumentType(FSMException):
    def __init__(self, annotation):
        self.message = f"The argument \"{annotation}\" cannot be handled"
        super(UnknownArgument, self).__init__(self.message)

class UnknownArgument(FSMException):
    '''Raised when an unwanted argument is given to a transition'''
    def __init__(self, param, name):
        self.message = f"The mandatory argument \"{param}\" is not required by transition {name}"
        super(UnknownArgument, self).__init__(self.message)

class InvalidAction(FSMException):
    '''Raised when an action doesn't have pre/post transitions'''
    def __init__(self, iface):
        self.message = f"The action \"{iface}\" does not have any pre or post transition method"
        super(InvalidAction, self).__init__(self.message)

class InvalidActionMethod(FSMException):
    '''Raised when an action doesn't have the pre/post transitions arguments'''
    def __init__(self, iface, method):
        self.message = f"The action \"{iface}\" method {method} does not have the correct arguements, each one should have at least \"_input_data\" and \"**kwargs\", and have type annotations"
        super(InvalidActionMethod, self).__init__(self.message)


class MethodSignatureMissingAnnotation(FSMException):
    def __init__(self, iface, method, pname):
        self.message = f"The action \"{iface}\" method {method} does not have the correct arguement annotation for \"{pname}\", provide \"argument:int\" to your pre/post methods."
        super(MethodSignatureMissingAnnotation, self).__init__(self.message)


class TransitionDataOfIncorrectFormat(FSMException):
    def __init__(self, data):
        self.message = f'The data "{data}" could not be interpreted as json'
        super(MethodSignatureMissingAnnotation, self).__init__(self.message)

class CannotGetRunNumber(FSMException):
    def __init__(self, data):
        self.message = f'Could not get Run Number because {data}'
        super().__init__(self.message)

class InvalidDataReturnByFSMAction(FSMException):
    def __init__(self, data):
        self.message = f"The action returns an incorrect object which isn't serialisable: {data}"
        super().__init__(self.message)