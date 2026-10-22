from drunc.exceptions import DruncException


# REST exceptions but they inherit from DruncException to carry rich error information
class ControllerException(DruncException):
    """Base exception for all controller-related errors."""
    reason = "CONTROLLER_ERROR"
    pass


class ChildError(ControllerException):
    reason = "CHILD_ERROR"
    pass


class CannotSurrenderControl(ControllerException):
    reason = "CANNOT_SURRENDER_CONTROL"
    pass


class ExpertCommandException(ControllerException):
    reason = "EXPERT_COMMAND_ERROR"
    pass
