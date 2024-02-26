
from drunc.utils.configuration_utils import ConfigurationHandler

class BroadcastClientConfiguration(ConfigurationHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from drunc.broadcast.types import BroadcastTypes
        self.impl_technology = BroadcastTypes.Unknown


    def get_impl_technology(self):
        return self.impl_technology


    def _parse_pbany(self, data):

        # potentially do something more complicated with different implementation technology here
        # match data.format():
        #    case KafkaBroadcastHandlerConfiguration
        #    ...

        from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration
        from drunc.utils.grpc_utils import unpack_any, UnpackingError
        try:
            from drunc.broadcast.types import BroadcastTypes
            self.impl_technology = BroadcastTypes.Kafka
            return unpack_any(data, KafkaBroadcastHandlerConfiguration)

        except UnpackingError as e:
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(f'Input configuration to configure the broadcast was not understood, could not setup the broadcast handler: {e}')
