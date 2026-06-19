from drunc.utils.configuration import ConfHandler


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


class BroadcastSenderConfHandler(ConfHandler):
    """Handler for broadcast sender configuration."""

    def populate_from_dict(self, data: dict[str, object]) -> None:
        if data == {}:
            self.address = None
            self.publish_timeout = None
        else:
            kafka_data = KafkaBroadcastSenderConfData.from_dict(data)
            self.address = kafka_data.address
            self.publish_timeout = kafka_data.publish_timeout

    def _post_process_oks(self) -> None:
        from drunc.broadcast.types import BroadcastTypes

        raw = self._raw_data
        if raw is not None:
            # OKS/pyobject path: raw is the broadcaster OKS object (or None if no broadcaster)
            self.address = getattr(raw, "address", None)
            self.publish_timeout = getattr(raw, "publish_timeout", None)
        elif not hasattr(self, "address"):
            # Neither OKS nor JSON populated — should not happen in practice
            self.address = None
            self.publish_timeout = None

        self.impl_technology = BroadcastTypes.Kafka if self.address else None
        self.log.debug(self.address)

    def get_impl_technology(self):
        return self.impl_technology
