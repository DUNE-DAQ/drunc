from drunc.exceptions import DruncCommandException


# These are all terminal exceptions
class ControllerException(DruncCommandException):
    pass


class ChildError(ControllerException):
    pass


class CannotSurrenderControl(ControllerException):
    pass


class ExpertCommandException(ControllerException):
    pass
