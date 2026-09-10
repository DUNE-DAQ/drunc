

class ControllerException(Exception):
    pass


class ChildError(ControllerException):
    pass


class CannotSurrenderControl(ControllerException):
    pass


class OtherUserAlreadyInControl(ControllerException):
    pass


class MalformedCommand(ControllerException):
    pass


class MalformedCommandArgument(ControllerException):
    pass


class ExpertCommandException(ControllerException):
    pass