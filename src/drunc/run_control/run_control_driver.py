import grpc
from druncschema.common_pb2 import LoggerTarget, SendLogRequest, SendLogResponse
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import Request
from druncschema.run_control_pb2_grpc import RunControlStub
from druncschema.token_pb2 import Token

from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    copy_token,
)
from drunc.utils.utils import (
    get_logger,
)


class RunControlDriver:
    def __init__(self, address: str, token: Token):
        self.log = get_logger("run_control_driver", rich_handler=True)
        self.address = address
        options = [
            ("grpc.keepalive_time_ms", 60000)  # pings the server every 60 seconds
        ]
        raw_channel = grpc.insecure_channel(self.address, options=options)
        rich_interceptor = RichErrorClientInterceptor(logger=self.log)
        self.channel = grpc.intercept_channel(raw_channel, rich_interceptor)
        self.stub = RunControlStub(self.channel)
        self.token = copy_token(token)

    def close(self):
        try:
            self.log.info("Closing gRPC channel to Run Control")
            self.channel.close()
        except Exception as e:
            self.log.error(f"Error closing gRPC channel: {e}", exc_info=True)

    # ----- RPC methods -----
    def describe(self, timeout: int | float = 60) -> Description:
        request = Request(token=copy_token(self.token))

        response = self.stub.describe(request, timeout=timeout)

        return response

    def send_log(
        self,
        text: str,
        severity: str = "INFO",
        logger: int = LoggerTarget.ECHO,
        target: str = "",
        execute_along_path: bool = False,
        execute_on_all_subsequent_children_in_path: bool = True,
        timeout: int | float = 60,
    ) -> SendLogResponse:
        """Send a log message to the process manager over gRPC.

        Args:
            text: The message to log.
            severity: The log severity, such as ``INFO`` or ``ERROR``.
            logger: The server logger target.
            target: The target node for the message.
            execute_along_path: Whether to execute along the target path.
            execute_on_all_subsequent_children_in_path: Whether to execute on all
                subsequent children in the target path.
            timeout: The gRPC request timeout in seconds.

        Returns:
            The response from the process manager.

        Raises:
            grpc.RpcError: If the gRPC request fails.
        """
        request = SendLogRequest(
            token=self.token,
            text=text,
            severity=severity,
            logger=logger,
            target=target,
            execute_along_path=execute_along_path,
            execute_on_all_subsequent_children_in_path=execute_on_all_subsequent_children_in_path,
        )
        request.token.CopyFrom(self.token)
        response: SendLogResponse = self.stub.send_log(request, timeout=timeout)
        return response
