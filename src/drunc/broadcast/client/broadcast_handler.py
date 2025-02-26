from drunc.broadcast.client.configuration import BroadcastClientConfHandler
from drunc.broadcast.types import BroadcastTypes


class BroadcastHandler:
    def __init__(self, broadcast_configuration: BroadcastClientConfHandler):
        super().__init__()

        from logging import getLogger

        self.log = getLogger("BroadcastHandler")

        self.configuration = broadcast_configuration
        self.implementation = None

        match self.configuration.data.type:
            # Being a bit sloppy here, having a Kafka sender doesn't mean we want to dump everything to stdout
            # There could be cases where we want to do other things.
            # For now, 1 server type <-> 1 client type...
            # Maybe in the future some sort of callback-based functionality would be preferable.
            case BroadcastTypes.Kafka:
                from druncschema.broadcast_pb2 import BroadcastMessage

                from drunc.broadcast.client.kafka_stdout_broadcast_handler import (
                    KafkaStdoutBroadcastHandler,
                )

                self.implementation = KafkaStdoutBroadcastHandler(
                    message_format=BroadcastMessage, conf=self.configuration
                )
            case _:
                self.log.info(
                    "Could not understand the BroadcastHandler technology you want to use, you will get no broadcast!"
                )

    def stop(self):
        if self.implementation:
            self.implementation.stop()
