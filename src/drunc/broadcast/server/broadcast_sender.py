

class BroadcastSenderTechnologyUnknown(Exception):
    def __init__(self, implementation):
        super().__init__(f'The implementation {implementation} is not supported for the BroadcastSender')

class BroadcastSender:
    def __init__(self, configuration:dict={}):
        self.impl_technology = configuration.get('type')

        if self.impl_technology is None:
            return

        match self.impl_technology:
            case 'kafka':
                from drunc.broadcast.server.kafka_sender import KafkaSender
                self.implementation = KafkaSender(configuration, self.name)
            case 'grpc':
                from drunc.broadcast.server.grpc_servicer import GRCPBroadcastSender
                self.implementation = GRCPBroadcastSender(configuration)
            case _:
                raise BroadcastSenderTechnologyUnknown(self.impl_technology)

    def broadcast(self, message, btype):
        if self.impl_technology is None:
            # nice and easy case
            return

        from druncschema.broadcast_pb2 import BroadcastMessage
        from druncschema.generic_pb2 import PlainText
        from drunc.utils.grpc_utils import pack_to_any
        any = pack_to_any(PlainText(text=message))

        bm = BroadcastMessage(
            emitter = self.name,
            type = btype,
            data = any,
        )
        bm.type = btype

        self.implementation._send(bm)
