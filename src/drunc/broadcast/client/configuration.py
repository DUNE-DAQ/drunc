from drunc.broadcast.types import BroadcastTypes
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfData, ConfHandler
from drunc.utils.grpc_utils import UnpackingError, unpack_any


class BroadcastClientConfData(ConfData):
    """Wrapper for broadcast client configuration data."""

    def __init__(
        self,
        type: BroadcastTypes | None = None,
        address: str | None = None,
        topic: str | None = None,
    ) -> None:
        self.type = type
        self.address = address
        self.topic = topic

    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from dictionary data."""
        if not data:
            return
        self.type = BroadcastTypes.Kafka
        self.address = data.get("kafka_address", data.get("address"))
        self.topic = data.get("topic")

    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from Protobuf Any message."""
        from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration

        if not pbany_data.ByteSize():
            self.type = None
            self.address = None
            self.topic = None
            return
        try:
            data = unpack_any(pbany_data, KafkaBroadcastHandlerConfiguration)
            self.type = BroadcastTypes.Kafka
            self.address = data.kafka_address
            self.topic = data.topic
        except UnpackingError as e:
            raise DruncSetupException(
                f"Input configuration to configure the broadcast was not understood, could not setup the broadcast handler: {e}",
                e,
            )


class BroadcastClientConfHandler(ConfHandler[BroadcastClientConfData]):
    """Handler for broadcast client configuration."""

    confdata_cls = BroadcastClientConfData

    def get_impl_technology(self):
        return self.data.type
