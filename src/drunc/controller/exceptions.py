from drunc.exceptions import DruncCommandException


class ControllerException(DruncCommandException):
    """Base exception for all Controller errors.
    """
    reason: str = "CONTROLLER_COMMAND_ERROR"


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


class ExpertCommandException(ControllerException):
    pass