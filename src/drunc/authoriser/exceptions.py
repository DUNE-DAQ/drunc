from druncschema.authoriser_pb2 import ActionType, SystemType
from google.rpc import code_pb2, error_details_pb2

from drunc.exceptions import DruncCommandException
from google.protobuf.message import Message



class Unauthorised(DruncCommandException):
    def __init__(
        self,
        user: str,
        action: ActionType.ValueType,
        command: str,
        drunc_system: SystemType,
    ) -> None:
        self.user = user
        self.action = action
        self.action_name = ActionType.Name(action)
        self.command = command
        self.drunc_system = drunc_system

        super(Unauthorised, self).__init__(
            txt=f"'{user}' is not authorised to '{self.action_name}', required for command '{command}' on '{drunc_system}'",
            code=code_pb2.PERMISSION_DENIED,
        )

class AuthenticationSystemUnavailable(DruncCommandException):
    grpc_error_code: int = code_pb2.UNAVAILABLE
    reason: str = "AUTHENTICATION_UNAVAILABLE"
    domain: str = "drunc.authentication"

    def __init__(
        self,
        resource_type: str,
        resource_name: str,
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.resource_type = resource_type
        self.resource_name = resource_name

    @property
    def specialised_details(self) -> list[Message]:
        resource_info = error_details_pb2.ResourceInfo(
            resource_type=self.resource_type,
            resource_name=self.resource_name,
            description=self.details or self.message,
        )
        return [resource_info]