
from drunc.utils.configuration import ConfHandler
from drunc.broadcast.types import BroadcastTypes

class BroadcastClientConfData: # OKSeroo
    def __init__(self, type:BroadcastTypes, address:str, topic:str):
        self.type = type
        self.address = address
        self.topic = topic


class BroadcastClientConfHandler(ConfHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def get_impl_technology(self):
        return self.impl_technology

    def _parse_pbany(self, data):

        # potentially do something more complicated with different implementation technology here
        # match data.format():
        #    case KafkaBroadcastHandlerConfiguration
        #    ...

        from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration
        from drunc.utils.grpc_utils import unpack_any, UnpackingError
        if not data.ByteSize():
            return BroadcastClientConfData(
                type = None,
                address = None,
                topic = None
            )
        try:
            data = unpack_any(data, KafkaBroadcastHandlerConfiguration)
            return BroadcastClientConfData(
                type = BroadcastTypes.Kafka,
                address = data.kafka_address,
                topic = data.topic
            )

        except UnpackingError as e:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(f'Input configuration to configure the broadcast was not understood, could not setup the broadcast handler: {e}', e)
