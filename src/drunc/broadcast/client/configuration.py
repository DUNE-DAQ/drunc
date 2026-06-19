from drunc.broadcast.types import BroadcastTypes
from drunc.exceptions import DruncSetupException
from drunc.utils.configuration import ConfHandler
from drunc.utils.grpc_utils import UnpackingError, unpack_any


class BroadcastClientConfHandler(ConfHandler):
    """Handler for broadcast client configuration."""

    def populate_from_dict(self, data: dict[str, object]) -> None:
        self.type: BroadcastTypes | None = None
        self.address: str | None = None
        self.topic: str | None = None
        if not data:
            return
        self.type = BroadcastTypes.Kafka
        self.address = data.get("kafka_address", data.get("address"))
        self.topic = data.get("topic")

    def populate_from_pbany(self, pbany_data: object) -> None:
        from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration

        self.type = None
        self.address = None
        self.topic = None
        if not pbany_data.ByteSize():
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

    def _post_process_oks(self) -> None:
        raw = self._raw_data
        if raw is not None:
            self.type = getattr(raw, "type", None)
            self.address = getattr(raw, "address", None)
            self.topic = getattr(raw, "topic", None)
        elif not hasattr(self, "type"):
            self.type = None
            self.address = None
            self.topic = None

    def get_impl_technology(self) -> BroadcastTypes | None:
        return self.type
