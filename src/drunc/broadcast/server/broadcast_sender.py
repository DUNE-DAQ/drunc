class BroadcastSenderTechnologyUnknown(Exception):
    def __init__(self, implementation):
        super().__init__(f'The implementation {implementation} is not supported for the BroadcastSender')

class BroadcastSender:
    def __init__(self, configuration:dict={}):
        self.impl_technology = configuration['type']
        match self.impl_technology:
            case 'kafka':
                from drunc.broadcast.server.kafka_sender import KafkaSender
                self.implementation = KafkaSender(configuration)
            case 'grpc':
                from drunc.broadcast.server.grpc_servicer import GRCPBroadcastSender
                self.implementation = GRCPBroadcastSender(configuration)
            case _:
                raise

    def broadcast(self, message, btype):
        from druncschema.broadcast_pb2 import BroadcastMessage
        from druncschema.generic_pb2 import PlainText
        from drunc.utils.grpc_utils import pack_to_any
        any = pack_to_any(PlainText(text=message))

        bm = BroadcastMessage(
            name = self.name,
            type = btype,
            data = any,
        )
        self.implementation._send(bm)
