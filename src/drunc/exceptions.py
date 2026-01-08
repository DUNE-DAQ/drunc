from google.rpc import code_pb2
from drunc.utils.rich_error_builder import build_rich_error

# class DruncException(Exception):
#     """Base exception for all Drunc errors."""

#     grpc_error_code = code_pb2.INTERNAL  # default
#     details = None  # optional rich error detail

#     def __init__(
#         self, message=None, grpc_error_code=None, details=None, *args, **kwargs
#     ):
#         if message is None:
#             message = "Drunc Error"  # default message

#         super().__init__(message, *args, **kwargs)

#         if grpc_error_code is not None:
#             self.grpc_error_code = grpc_error_code
#         if details is not None:
#             self.details = details

class DruncException(Exception):
    def __init__(
        self,
        message: str,
        grpc_error_code=code_pb2.INTERNAL,
        detail_type="error_info",
        **detail_kwargs
    ):
        super().__init__(message)
        
        if grpc_error_code is not None:
            self.grpc_error_code = grpc_error_code

        if detail_type is not None:
            self.detail_type = detail_type

        self.detail_kwargs = detail_kwargs
        
    def to_rich_error(self):
        return build_rich_error(self.detail_type, **self.detail_kwargs)


class DruncShellException(DruncException):
    # Exceptions that gets thrown by shells
    pass


class DruncSetupException(
    DruncException
):  # Exceptions that gets thrown when services start

    pass


class DruncCommandException(
    DruncException
):  # Exceptions that gets thrown when commands run
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
