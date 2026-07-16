from typing import Dict, List, Optional

from google.protobuf.message import Message
from google.rpc import code_pb2, error_details_pb2


class DruncException(Exception):
    def __init__(
        self,
        message: str = "An error occurred in Drunc.",
        grpc_error_code: Optional[int] = None,
        details: Optional[str] = None,
        reason: Optional[str] = None,
        domain: Optional[str] = None,
        **detail_kwargs: object,
    ) -> None:
        super().__init__(message)

        self.message = message if message is not None else "An error occurred in Drunc."

        self.grpc_error_code = grpc_error_code or getattr(
            self.__class__, "grpc_error_code", code_pb2.INTERNAL
        )

        self.reason = (
            reason
            if reason is not None
            else str(getattr(self.__class__, "reason", self.__class__.__name__))
        )
        self.domain = (
            domain
            if domain is not None
            else str(getattr(self.__class__, "domain", "drunc"))
        )

        self.details: Optional[str] = details
        self.detail_kwargs: Dict[str, object] = detail_kwargs

        error_metadata: Dict[str, str] = {"message": self.message}
        for key, value in self.detail_kwargs.items():
            error_metadata[key] = str(value)

        self.base_error_info = error_details_pb2.ErrorInfo(
            reason=self.reason, domain=self.domain, metadata=error_metadata
        )

    @property
    def specialised_details(self) -> List[Message]:
        return []

    @property
    def rich_details(self) -> List[Message]:
        details_list: List[Message] = [self.base_error_info]

        if self.specialised_details:
            details_list.extend(self.specialised_details)

        return details_list


class DruncShellException(DruncException):
    pass


class DruncSetupException(DruncException):
    grpc_error_code: int = code_pb2.FAILED_PRECONDITION

    @property
    def specialised_details(self) -> List[Message]:
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


class DruncCommandException(DruncException):
    grpc_error_code: int = code_pb2.INTERNAL
    reason: str = "COMMAND_ERROR"


class DruncServerSideError(DruncException):
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


class DruncBatchShellError(DruncException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellArgError(DruncException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error, unknown command or argument: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellUnknownCommand(DruncException):
    def __init__(self, msg: str) -> None:
        err_msg = f"Batch shell error, unknown command: {msg}"
        super().__init__(message=err_msg)


class DruncBatchShellMissingArg(DruncException):
    def __init__(self, msg1: str, msg2: str) -> None:
        err_msg = f"Batch shell error, this optional argument is mandatory in batch mode. Failed command: {msg1}. Next input: {msg2}"
        super().__init__(message=err_msg)


class DruncNotImplementedException(DruncException):
    grpc_error_code: int = code_pb2.UNIMPLEMENTED
    reason: str = "NOT_IMPLEMENTED"
