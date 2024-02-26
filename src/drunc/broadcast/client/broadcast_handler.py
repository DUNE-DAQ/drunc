
from drunc.broadcast.types import BroadcastTypes, BroadcastTypeNotHandled
from drunc.utils.configuration_utils import ConfData
from drunc.broadcast.client.configuration import BroadcastClientConfiguration

class BroadcastHandler:
    def __init__(self, broadcast_configuration:ConfData, **kwargs):
        super(BroadcastHandler, self).__init__(
            **kwargs,
        )
        self.configuration = BroadcastClientConfiguration(broadcast_configuration)
        self.impl_technology = self.configuration.get_impl_technology()
        self.implementation = None

        match self.impl_technology:
            # Being a bit sloppy here, having a Kafka sender doesn't mean we want to dump everything to stdout
            # There could be cases where we want to do other things.
            # For now, 1 server type <-> 1 client type...
            # Maybe in the future some sort of callback-based functionality would be preferable.
            case BroadcastTypes.Kafka:
                from drunc.broadcast.client.kafka_stdout_broadcast_handler import KafkaStdoutBroadcastHandler
                from druncschema.broadcast_pb2 import BroadcastMessage
                self.implementation = KafkaStdoutBroadcastHandler(
                    conf = broadcast_configuration,
                    message_format = BroadcastMessage,
                )
            case BroadcastTypes.gRPC:
                from drunc.exceptions import DruncSetupException
                raise DruncSetupException("gRPC is not available for broadcasting!")
                from drunc.broadcast.client.grpc_stdout_broadcast_handler import gRPCStdoutBroadcastHandler
                from druncschema.broadcast_pb2 import BroadcastMessage
                self.implementation = gRPCStdoutBroadcastHandler(
                    conf = broadcast_configuration,
                    message_format = BroadcastMessage,
                )

    def stop(self):
        if self.implementation:
            self.implementation.stop()