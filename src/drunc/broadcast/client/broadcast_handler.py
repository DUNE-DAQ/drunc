
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
                match broadcast_configuration.DESCRIPTOR:
                    case KafkaBroadcastHandlerConfiguration.DESCRIPTOR:
                        self.impl_technology = BroadcastTypes.Kafka
                        broadcast_configuration = unpack_any(broadcast_configuration, KafkaBroadcastHandlerConfiguration)
                    case _:
                        raise BroadcastTypeNotHandled(str(broadcast_configuration.DESCRIPTOR))

            case ConfTypes.OKS:
                raise ConfTypeNotSupported(conf_type, 'BroadcastHandler')
            case ConfTypes.Json:
                self.impl_technology = broadcast_configuration['type']

        match self.impl_technology:
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