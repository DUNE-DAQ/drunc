from drunc.utils.configuration import (
    ConfData,
    ConfHandler,
    ConfTypeNotSupported,
    ConfTypes,
)


class KafkaBroadcastSenderConfData:
    def __init__(self, address=None, publish_timeout=None):
        self.address = address
        self.publish_timeout = publish_timeout

    @staticmethod
    def from_dict(data: dict):
        address = data.get("address")
        if address is None:
            address = data["kafka_address"]

        return KafkaBroadcastSenderConfData(
            address=address, publish_timeout=data["publish_timeout"]
        )


class BroadcastSenderConfData(ConfData):
    """Wrapper for broadcast sender configuration data."""

    def __init__(self):
        self.kafka_data = None

    def populate_from_dict(self, data: dict[str, object]) -> None:
        """Populate from dictionary data."""
        if data == {}:
            self.kafka_data = None
        else:
            self.kafka_data = KafkaBroadcastSenderConfData.from_dict(data)

    def populate_from_pbany(self, pbany_data: object) -> None:
        """Populate from Protobuf Any message."""
        raise ConfTypeNotSupported(ConfTypes.ProtobufAny, self.__class__.__name__)


class BroadcastSenderConfHandler(ConfHandler[BroadcastSenderConfData]):
    """Handler for broadcast sender configuration."""

    confdata_cls = BroadcastSenderConfData

    def _post_process_oks(self):
        from drunc.broadcast.types import BroadcastTypes

        # Normalize wrapped JSON-loaded data to the runtime shape used by sender code.
        if hasattr(self.data, "kafka_data"):
            self.data = self.data.kafka_data

        self.impl_technology = BroadcastTypes.Kafka if self.data else None
        self.log.debug(self.data)

    def get_impl_technology(self):
        return self.impl_technology
