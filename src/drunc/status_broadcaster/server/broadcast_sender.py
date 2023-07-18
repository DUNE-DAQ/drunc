
class BroadcastSender:
    def __init__(self, impl:str='daq_streamer'):
        if impl == 'daq_streamer':
            self.implementation = None
        elif impl == 'kafka':
            from drunc.status_broadcaster.server.kafka_sender import KafkaSender
            self.implementation = KafkaSender()
        elif impl == 'grpc':
            from drunc.status_broadcaster.server.grpc_servicer import GRCPBroadcastSender
            self.implementation = GRCPBroadcastSender()