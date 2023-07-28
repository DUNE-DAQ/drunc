

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

        while self.run:
            for messages in self.consumer.poll(timeout_ms = 500).values():
                for message in messages:
                    try:
                        decoded = self.message_format()
                        decoded.ParseFromString(message.value)
                        # hum, we would really prefer something a bit better here... I don't know what
                        text_proto = text_format.MessageToString(decoded)
                        self._log.info(text_proto)
                    except:
                        pass
