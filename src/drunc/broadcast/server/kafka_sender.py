
from druncschema.broadcast_pb2 import BroadcastMessage
from drunc.broadcast.server.broadcast_sender_implementation import BroadcastSenderImplementation

class KafkaSender(BroadcastSenderImplementation):
    def __init__(self, conf, topic):
        import logging
        self._log = logging.getLogger(f"{topic}_KafkaSender")

        from kafka import KafkaProducer
        self.topic = topic
        self.kafka_address = conf['kafka_address']
        self.kafka = KafkaProducer(
            bootstrap_servers = [self.kafka_address],
            client_id = 'run_control',
        )
        self.publish_timeout = conf['publish_timeout']

    def _send(self, bm:BroadcastMessage):
        from kafka.errors import KafkaError

        future = self.kafka.send(
            self.topic, bm.SerializeToString()
        )

        try:
            record_metadata = future.get(timeout=self.publish_timeout)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            self._log.error(f'Kafka exception sending message {bm}: {str(e)}')
            pass
        except Exception as e:
            # Decide what to do if produce request failed...
            self._log.error(f'Unhandled exception sending message {bm}: {str(e)}')
            pass

        self._log.debug(f'{record_metadata} published')

