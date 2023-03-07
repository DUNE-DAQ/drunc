
class ControllerException(Exception):
    pass

class CannotSurrenderControl(ControllerException):
    pass

class OtherUserAlreadyInControl(ControllerException):
    pass

class MalformedMessage(ControllerException):
    pass