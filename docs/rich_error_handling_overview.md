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
        details=f"No configuration files found in {search_paths}",
    )
```

You can overwrite any of the `message`, `grpc_error_code`,`details`,`reason` and `domain`fields. If not explicitly provided, they will automatically fall back to the standard default values defined in the base error.

## 3. Server-Side - Server Interceptor

`RichErrorServerInterceptor` is a `grpc.ServerInterceptor` (defined in `src/drunc/utils/grpc_utils.py`) that wraps every unary-unary handler in a try/except block. When a `DruncException` is caught, it calls `abort_with_rich_details()` to pack the `ErrorInfo` and any specialised details into the gRPC trailing metadata before aborting the call. Non-`DruncException` errors and streaming calls are passed through unchanged.

Implementing the interceptor:

```python
import grpc
from drunc.utils.grpc_utils import RichErrorServerInterceptor

server = grpc.server(
    futures.ThreadPoolExecutor(max_workers=10),
    interceptors=[RichErrorServerInterceptor()],
)
```

## 4. Client-Side - Catching Errors with a Client Interceptor

Client-side rich error handling is done automatically by the `RichErrorClientInterceptor` (located in `src/drunc/utils/grpc_utils.py`).

Because gRPC network calls are asynchronous, a failed request doesn't throw an error the moment it is sent. Instead, the `RpcError` is raised when the application actually tries to read the data by calling `.result()` on the response `Future`. `_GRPCCallWrapper` wraps the `Future` returned by `continuation` and delays error handling until `.result()` or `.exception()` is actually called by the application.

If the interceptor can't find any rich metadata or if extraction fails, it simply logs a debug warning and passes the standard gRPC error along unchanged.

### Setting up a client with the interceptor

```python
self.log = get_logger("process_manager_driver", rich_handler=True)
self.address = address
options = [
    ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
]
raw_channel = grpc.insecure_channel(self.address, options=options)
rich_interceptor = RichErrorClientInterceptor(logger=self.log)
self.channel = grpc.intercept_channel(raw_channel, rich_interceptor)
self.stub = ProcessManagerStub(self.channel)
self.token = copy_token(token)
```

Once the channel is set up with the interceptor, **no extra try/except blocks are needed** to extract the rich error. Any `grpc.RpcError` that contains rich errors will be logged automatically before being re-raised.

## 5. Testing gRPC errors

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