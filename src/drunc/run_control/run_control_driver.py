import grpc
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

    def send_log():
        raise ValueError("To be supported soon!")
