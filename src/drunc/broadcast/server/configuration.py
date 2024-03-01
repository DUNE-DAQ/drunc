from drunc.utils.configuration import ConfHandler



class KafkaBroadcastSenderConfData:
    def __init__(self, address=None, publish_timeout=None):
        self.address = address
        self.publish_timeout = publish_timeout

    @staticmethod
    def from_dict(data:dict):
        address = data.get('address')
        if address is None:
            address = data['kafka_address']

        return KafkaBroadcastSenderConfData(
            address = address,
            publish_timeout = data['publish_timeout']
        )

class BroadcastSenderConfHandler(ConfHandler):
    def _post_process_oks(self):
        from drunc.broadcast.types import BroadcastTypes
        self.impl_technology = BroadcastTypes.Kafka
        self.log.info(self.data)
    def get_impl_technology(self):
        return self.impl_technology

    def __parse_dict(self, data):
        return KafkaBroadcastSenderConfData.from_dict(data)