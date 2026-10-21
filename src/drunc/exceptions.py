from google.protobuf.message import Message
from google.rpc import code_pb2, error_details_pb2


class DruncException(Exception):
    """Base exception class for Drunc errors."""
    def __init__(
        self,
        message: str | None = "An error occurred in Drunc.",
        grpc_error_code: int | None = None,
        details: str | None = None,
        reason: str | None = None,
        domain: str | None = None,
        **detail_kwargs: object,
    ) -> None:
        super().__init__(message)

        self.message: str = (
            message if message is not None else "An error occurred in Drunc."
        )

        self.grpc_error_code: int = (
            grpc_error_code
            if grpc_error_code is not None
            else int(getattr(self.__class__, "grpc_error_code", code_pb2.INTERNAL))
        )

        self.reason: str = (
            reason
            if reason is not None
            else str(getattr(self.__class__, "reason", self.__class__.__name__))
        )

        self.domain: str = (
            domain
            if domain is not None
            else str(getattr(self.__class__, "domain", "drunc"))
        )

        self.details: str | None = details
        self.detail_kwargs: dict[str, object] = detail_kwargs

        error_metadata: dict[str, str] = {"message": self.message}
        for key, value in self.detail_kwargs.items():
            error_metadata[key] = str(value)

        self.base_error_info = error_details_pb2.ErrorInfo(
            reason=self.reason, domain=self.domain, metadata=error_metadata
        )

    @property
    def specialised_details(self) -> list[Message]:
        return []

    @property
    def rich_details(self) -> list[Message]:
        details_list: list[Message] = [self.base_error_info]

        if self.specialised_details:
            details_list.extend(self.specialised_details)

        return details_list


class DruncTerminalException(DruncException):
    """The RPC must be aborted with a rich gRPC status."""


class DruncNonTerminalException(DruncException):
    """The command can return a normal response containing the error."""

    
class DruncShellException(DruncTerminalException):
    pass


class DruncSetupException(DruncTerminalException):
    grpc_error_code: int = code_pb2.FAILED_PRECONDITION

    @property
    def specialised_details(self) -> list[Message]:
        precond = error_details_pb2.PreconditionFailure(
            violations=[
                error_details_pb2.PreconditionFailure.Violation(
                    type="MISSING OR INVALID",
                    subject=f"Services could not start. {self.message}",
                    description=self.details or "",
                )
            ]
        )
        return [precond]


class DruncCommandException(DruncTerminalException):
    grpc_error_code: int = code_pb2.INTERNAL
    reason: str = "COMMAND_EXECUTION_FAILED"


class DruncCommandNonTerminalException(DruncNonTerminalException):
    grpc_error_code: int = code_pb2.INTERNAL
    reason: str = "COMMAND_ERROR"


class DruncServerSideError(DruncTerminalException):
    def __init__(
        self,
        error_txt: str,
        stack_txt: str,
        server_response: str,
        *args: object,
        **kwargs: object,
    ) -> None:
        self.error_txt: str = error_txt
        self.stack_txt: str = stack_txt
        self.server_response: str = server_response

        super().__init__(
            message=error_txt,
            details=server_response,
            **kwargs,  # type: ignore[arg-type]
        )

    def __str__(self) -> str:
        return f"{self.stack_txt}\n{self.error_txt}\n{self.server_response}"


class DruncBatchShellError(DruncTerminalException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellArgError(DruncTerminalException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error, unknown command or argument: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellUnknownCommand(DruncTerminalException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error, unknown command: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellMissingArg(DruncTerminalException):
    def __init__(self, msg1: str, msg2: str) -> None:
        err_msg = f"Batch shell error, this optional argument is mandatory in batch mode. Failed command: {msg1}. Next input: {msg2}"
        super().__init__(message=err_msg)


class DruncNotImplementedException(DruncTerminalException):
    grpc_error_code: int = code_pb2.UNIMPLEMENTED
    reason: str = "NOT_IMPLEMENTED"



#########################################################################
#                   CONTROLLER EXCEPTIONS
#########################################################################


class ChildCommandNonTerminalException(DruncCommandNonTerminalException):
    """Non terminal exception for child command failures. Used when a child command fails but the parent can continue."""
    reason = "CHILD_COMMAND_ERROR"

    def __init__(self, child_name: str, child_details: list[Message], **kwargs) -> None:
        self.child_name = child_name
        self._child_details = child_details
        super().__init__(message=f"Child '{child_name}' failed", **kwargs)

    @property
    def child_details(self) -> list[Message]:
        return self._child_details

class ChildCommandTerminalException(DruncCommandException):
    """Terminal exception for child command failures. Used when a child command fails and the parent cannot continue."""
    grpc_error_code = code_pb2.INTERNAL
    reason = "CHILD_COMMAND_EXECUTION_FAILED"

    def __init__(
        self,
        child_name: str,
        child_details: list[Message],
    ) -> None:
        self.child_name = child_name
        self._child_details = child_details
        super().__init__(message=f"Child '{child_name}' failed")

    @property
    def specialised_details(self) -> list[Message]:
        return self._child_details