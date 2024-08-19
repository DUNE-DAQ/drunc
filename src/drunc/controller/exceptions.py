from drunc.exceptions import DruncCommandException

class ControllerException(DruncCommandException):
    pass

class ChildError(ControllerException):
    pass

class CannotSurrenderControl(ControllerException):
    pass

class OtherUserAlreadyInControl(ControllerException):
    pass

class MalformedMessage(ControllerException):
    pass

class MalformedCommand(ControllerException):
    pass

class MalformedCommandArgument(ControllerException):
    pass