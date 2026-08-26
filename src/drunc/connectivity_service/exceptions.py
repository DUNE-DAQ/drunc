from google.rpc import code_pb2

from drunc.exceptions import DruncException


class ApplicationRegistryNotPresent(DruncException):
    pass


class ApplicationRegistrationUnsuccessful(DruncException):
    pass


class ApplicationLookupUnsuccessful(DruncException):
    """Raised when an application cannot be found in the connectivity service."""
    grpc_error_code: int = code_pb2.NOT_FOUND
    reason: str = "APPLICATION_NOT_FOUND"


class ApplicationUpdateUnsuccessful(DruncException):
    pass

class ConnectivityServiceUnavailable(DruncException):
    grpc_error_code: int = code_pb2.UNAVAILABLE
    reason: str = "CONNECTIVITY_SERVICE_UNAVAILABLE"
