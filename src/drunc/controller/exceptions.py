from drunc.exceptions import DruncCommandException

class ControllerException(DruncCommandException):
    pass

class CannotSurrenderControl(ControllerException):
    pass

class OtherUserAlreadyInControl(ControllerException):
    pass

class MalformedMessage(ControllerException):
    pass