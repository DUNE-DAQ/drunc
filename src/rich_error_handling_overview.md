# Drunc Error Handling Architecture

This document explains how `drunc` handles errors across its distributed gRPC services.

We use **gRPC Rich Error Handling** to pass structured metadata across the network, which allows the client interfaces to understand *what* failed, *where* it failed, and *why*.

## 1. Building rich error details

In `drunc`, the rich error metadata is constructed exactly where the exception is defined (for example inside `src/drunc/exceptions.py`).

### i. Base rich error details

Every custom `drunc` exception (`DruncException`) is built to automatically generate a gRPC `ErrorInfo` protobuf object. This contains:

* **`grpc_error_code`**: An integer enum from `google.rpc.code_pb2` (such as `code_pb2.INTERNAL` or `code_pb2.UNIMPLEMENTED`). There are currently 16 specific error codes defined [here](https://grpc.io/docs/guides/status-codes/).
* **`reason`:** A machine-readable string to categorise the error, such as `NOT_IMPLEMENTED` or `COMMAND_ERROR`.
* **`domain`:** The component or part that generated the error (e.g., `drunc`, `ProcessManager.boot`).
* **`details`:** Human-readable context (e.g., "DUNEDAQ_DB_PATH is not set").
### ii. Specialised Error Details

Sometimes, the standard `ErrorInfo` isn't structured enough. gRPC provides specialised Protobuf messages for specific failure states, such as `PreconditionFailure`, `BadRequest`, or `ResourceInfo` and [more](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto). 

You can append these specialised Protobuf objects to the error by overriding the `@property def specialised_details(self)` method in the custom exception.

#### Example:  PreconditionFailure
If a service fails to start you could use `PreconditionFailure.

```python

class DruncSetupException(DruncException):
    grpc_error_code = code_pb2.FAILED_PRECONDITION
    reason = "SETUP_FAILED"
    domain = "drunc.setup"

    @property
    def specialised_details(self):
        precond = error_details_pb2.PreconditionFailure(
            violations=[
                error_details_pb2.PreconditionFailure.Violation(
                    type="MISSING OR INVALID",
                    subject=f"Services could not start: {self.message}",
                    description=str(self.details),
                )
            ]
        )
        return [precond]

```
## 2. Server-Side - Throwing Errors

You do not need to manually pack Protobuf objects. Simply raise a subclass of `DruncException`.

### Example: Raising an existing exception
```python
from drunc.exceptions import DruncSetupException

# The exception automatically handles formatting the reason, domain, and details.
if not config_files:
    raise DruncSetupException(
        message="Config files missing",
        details=f"No configuration files found in {search_paths}"
    )
```
You can overwrite any of the `message`, `grpc_error_code`,`details`,`reason` and `domain`fields. If not explicitly provided, they will automatically fall back to the standard default values defined in the base error.

## 3. Client-Side -  Catching Errors

**Provisional: this will change with the implementation of a `ClientInterceptor`**

Use `extract_grpc_rich_error` to unpack the rich metadata:

```python
try:
    response = stub.boot(request, timeout=60)
except grpc.RpcError as e:
    try:
        error_details = extract_grpc_rich_error(e)
        log.error(error_details)
    except Exception as extraction_error:
        log.debug(f"Could not extract rich error: {extraction_error}")
    
    handle_grpc_error(e)
```

## 4. Testing gRPC errors

Because gRPC status.details is a dynamic list, you need to iterate and check the type using `.Is()`.

```python
status = status_pb2.Status()
for key, value in err.trailing_metadata():
    if key == "grpc-status-details-bin":
        status.ParseFromString(value)

        base_error = None
        precond = None

        for detail in status.details:
            if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                base_error = error_details_pb2.ErrorInfo()
                detail.Unpack(base_error)
            
            elif detail.Is(error_details_pb2.PreconditionFailure.DESCRIPTOR):
                precond = error_details_pb2.PreconditionFailure()
                detail.Unpack(precond)

        assert base_error is not None
        assert base_error.reason == "NOT_IMPLEMENTED"
```