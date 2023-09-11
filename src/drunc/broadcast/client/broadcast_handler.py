
from drunc.broadcast.types import BroadcastTypes, BroadcastTypeNotHandled

class BroadcastHandler:
    def __init__(self, broadcast_configuration, conf_type, **kwargs):
        super(BroadcastHandler, self).__init__(
            **kwargs,
        )

        from drunc.utils.conf_types import ConfTypes, ConfTypeNotSupported
        match conf_type:
            case ConfTypes.Protobuf:
                from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration
                from drunc.utils.grpc_utils import unpack_any

                if broadcast_configuration.Is(KafkaBroadcastHandlerConfiguration.DESCRIPTOR):
                    self.impl_technology = BroadcastTypes.Kafka
                    broadcast_configuration = unpack_any(broadcast_configuration, KafkaBroadcastHandlerConfiguration)
                else:
                    raise BroadcastTypeNotHandled(broadcast_configuration)

            case ConfTypes.OKS:
                raise ConfTypeNotSupported(conf_type, 'BroadcastHandler')

            case ConfTypes.Json:
                self.impl_technology = broadcast_configuration['type']

            case _:
                raise ConfTypeNotSupported(conf_type, 'BroadcastHandler')

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
                    conf_type = conf_type,
                )
            case BroadcastTypes.gRPC:
                from drunc.broadcast.client.grpc_stdout_broadcast_handler import gRPCStdoutBroadcastHandler
                from druncschema.broadcast_pb2 import BroadcastMessage
                self.implementation = gRPCStdoutBroadcastHandler(
                    conf = broadcast_configuration,
                    message_format = BroadcastMessage,
                    conf_type = conf_type,
                )

    def stop(self):
        if self.implementation:
            self.implementation.stop()