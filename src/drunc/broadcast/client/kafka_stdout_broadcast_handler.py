

class KafkaStdoutBroadcastHandler:
    def __init__(self, conf, topic, message_format):
        self.kafka_address = conf['kafka_address']
        self.topic = topic
        self.message_format = message_format

        import logging
        self._log = logging.getLogger(f'{topic} message')
        from kafka import KafkaConsumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers = [self.kafka_address],
            #value_deserializer = lambda m: self.message_format().ParseFromString(m)
        )
        self.run = True
        import threading
        self.thread = threading.Thread(
            target=self.consume
        )
        self.thread.start()

    def stop(self):
        self._log.info(f'Stopping listening to \'{self.topic}\'')
        self.run = False
        self.thread.join()

    def consume(self):
        from google.protobuf import text_format
        import druncschema.broadcast_pb2 as b_desc
        from druncschema.broadcast_pb2 import BroadcastMessage, BroadcastType
        from druncschema.generic_pb2 import PlainText
        from drunc.utils.grpc_utils import unpack_any
        while self.run:
            for messages in self.consumer.poll(timeout_ms = 500).values():
                for message in messages:
                    decoded=''
                    try:
                        decoded = self.message_format()
                        decoded.ParseFromString(message.value)
                        self._log.debug(f'{decoded=}, {type(decoded)=}')
                    except Exception as e:
                        self._log.error(f'Unhandled broadcast message: {message} (error: {str(e)})')
                        pass

                    try:
                        if decoded.data.Is(PlainText.DESCRIPTOR):
                            txt = unpack_any(decoded.data, PlainText).text
                        else:
                            txt = decoded.data

                        self._log.info(f'"{decoded.emitter}": "{BroadcastType.Name(decoded.type)}" {txt}')

                    except Exception as e:
                        self._log.error(f'Weird broadcast message: {message} (error: {str(e)})')
                        text_proto = text_format.MessageToString(decoded)
                        self._log.info(text_proto)
                        raise e
