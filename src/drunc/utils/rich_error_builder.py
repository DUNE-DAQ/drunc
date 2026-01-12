from google.protobuf import duration_pb2
from google.rpc import error_details_pb2


def build_rich_error(message: str, detail_type: str, **kwargs):
    """
    Builds a rich error detail based on the specified detail type.
    For a lookup of detail types, see
    https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto .

    Args:
        message: The main error message.
        detail_type: The type of rich error detail to create.
        **kwargs: Additional keyword arguments specific to the detail type.
    """

    # BadRequest: For INVALID_ARGUMENT errors
    if detail_type == "bad_request":
        return error_details_pb2.BadRequest(
            field_violations=[
                error_details_pb2.BadRequest.FieldViolation(
                    field=kwargs.get("field", ""), description=message
                )
            ]
        )

    # PreconditionFailure: For FAILED_PRECONDITION errors
    if detail_type == "precondition":
        return error_details_pb2.PreconditionFailure(
            violations=[
                error_details_pb2.PreconditionFailure.Violation(
                    type=kwargs.get("type", ""),
                    subject=kwargs.get("subject", ""),
                    description=message,
                )
            ]
        )

    # ErrorInfo: General error information
    if detail_type == "error_info":
        return error_details_pb2.ErrorInfo(
            reason=kwargs.get("reason", ""),
            domain=kwargs.get("domain", ""),
            metadata=kwargs.get("metadata", {}),
        )

    # RetryInfo: Tells clients how long to wait before retrying
    if detail_type == "retry_info":
        duration = duration_pb2.Duration()
        # 'delay_seconds' or 'delay_ms' should be passed in kwargs
        seconds = kwargs.get("delay_seconds", 0)
        nanos = int(kwargs.get("delay_ms", 0) * 1e6)

        duration.seconds = seconds
        duration.nanos = nanos
        return error_details_pb2.RetryInfo(retry_delay=duration)

    # ResourceInfo: For NOT_FOUND or ALREADY_EXISTS
    if detail_type == "resource_info":
        return error_details_pb2.ResourceInfo(
            resource_type=kwargs.get("resource_type", ""),
            resource_name=kwargs.get("r~esource_name", ""),
            owner=kwargs.get("owner", ""),
            description=message,
        )

    # DebugInfo: For INTERNAL errors (Stack traces)
    if detail_type == "debug_info":
        return error_details_pb2.DebugInfo(
            stack_entries=kwargs.get("stack_entries", []),  # List of strings
            detail=kwargs.get("detail", ""),
        )

    # Help: Links to documentation
    if detail_type == "help":
        return error_details_pb2.Help(
            links=[
                error_details_pb2.Help.Link(
                    description=kwargs.get("link_description", "Documentation"),
                    url=kwargs.get("url", ""),
                )
            ]
        )

    raise ValueError(f"Unknown detail type: {detail_type}")  #'
