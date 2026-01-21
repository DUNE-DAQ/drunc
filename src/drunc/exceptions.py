from google.rpc import code_pb2

from drunc.utils.rich_error_builder import build_rich_error


class DruncException(Exception):
    def __init__(
        self,
        message: str = "An error occurred in Drunc.",
        grpc_error_code=None,
        details=None,  # optional rich error detail
        **detail_kwargs,
    ):
        super().__init__(message)

        if message is not None:
            self.message = message

        if grpc_error_code is None:
            grpc_error_code = getattr(self, "grpc_error_code", code_pb2.INTERNAL)

        if details is not None:
            self.details = details

        self.detail_kwargs = detail_kwargs


class DruncShellException(DruncException):
    # Exceptions that gets thrown by shells
    pass


class DruncSetupException(
    DruncException
):  # Exceptions that gets thrown when services start
    grpc_error_code = code_pb2.FAILED_PRECONDITION
    pass


class DruncCommandException(
    DruncException
):  # Exceptions that gets thrown when commands run
    grpc_error_code = code_pb2.INTERNAL
    pass


class DruncServerSideError(
    DruncException
):  # Exceptions that gets thrown when commands run
    def __init__(self, error_txt, stack_txt, server_response, *args, **kwargs):
        self.error_txt = error_txt
        self.stack_txt = stack_txt
        self.server_response = server_response
        super().__init__(error_txt, stack_txt, server_response, *args, **kwargs)

    def __str__(self):
        return f"{self.stack_txt}\n{self.error_txt}\n{self.server_response}"


class DruncNotImplementedException(DruncException):
    grpc_error_code = code_pb2.UNIMPLEMENTED
    pass
