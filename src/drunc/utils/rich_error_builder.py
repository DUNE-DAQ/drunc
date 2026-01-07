from google.rpc import code_pb2, error_details_pb2, status_pb2


def build_rich_error(detail_type: str, **kwargs):
    if detail_type == "bad_request":
        return error_details_pb2.BadRequest(
            field_violations=[
                error_details_pb2.BadRequest.FieldViolation(
                    field=kwargs.get("field", ""),
                    description=kwargs.get("description", "")
                )
            ]
        )

    if detail_type == "precondition":
        return error_details_pb2.PreconditionFailure(
            violations=[
                error_details_pb2.PreconditionFailure.Violation(
                    type=kwargs.get("type", ""),
                    subject=kwargs.get("subject", ""),
                    description=kwargs.get("description", "")
                )
            ]
        )

    if detail_type == "error_info":
        return error_details_pb2.ErrorInfo(
            reason=kwargs.get("reason", ""),
            domain=kwargs.get("domain", ""),
            metadata=kwargs.get("metadata", {}),
        )

    raise ValueError(f"Unknown detail type: {detail_type}")
