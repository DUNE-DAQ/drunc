
from druncschema.broadcast_pb2 import BroadcastMessage
from drunc.broadcast.server.broadcast_sender_implementation import BroadcastSenderImplementation
from drunc.utils.configuration_utils import ConfTypes, ConfTypeNotSupported, ConfData

class KafkaSender(BroadcastSenderImplementation):
    def __init__(self, kafka_address:str, publish_timeout:int, topic:str, **kwargs):
        super(KafkaSender, self).__init__(**kwargs)

        import logging
        self._log = logging.getLogger(f"{topic}.KafkaSender")

        from kafka import KafkaProducer
        from kafka import errors as Errors
        self.topic = topic
        self._can_broadcast = False

        self.kafka_address = kafka_address
        self.publish_timeout = publish_timeout

        try:
            self.kafka = KafkaProducer(
                bootstrap_servers = [self.kafka_address],
                client_id = 'run_control',
            )
        except Errors.NoBrokersAvailable as e:
            t = f'{self.kafka_address} does not seem to point to a kafka broker.'
            self._log.critical(t)
            from drunc.exceptions import DruncSetupException
            raise DruncSetupException(t) from e

        self._log.info(f'Broadcasting to Kafka ({self.kafka_address}) client_id: "run_control", topic: "{self.topic}"')
        self._can_broadcast = True

    def can_broadcast(self):
        return self._can_broadcast


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

    def describe_broadcast(self):
        from druncschema.broadcast_pb2 import KafkaBroadcastHandlerConfiguration
        return KafkaBroadcastHandlerConfiguration(
            topic = self.topic,
            kafka_address = self.kafka_address,
        )
