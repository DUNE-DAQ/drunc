
from druncschema.broadcast_pb2 import BroadcastMessage
from drunc.broadcast.server.broadcast_sender_implementation import BroadcastSenderImplementation
from drunc.utils.conf_types import ConfTypes, ConfTypeNotSupported

class KafkaSender(BroadcastSenderImplementation):
    kafka = None
    can_broadcast = False

    def __init__(self, conf, topic, conf_type:ConfTypes=ConfTypes.Json, **kwargs):
        super(KafkaSender, self).__init__(**kwargs)
        import logging
        self._log = logging.getLogger(f"{topic}_KafkaSender")

        from kafka import KafkaProducer
        from kafka import errors as Errors
        self.topic = topic
        self._can_broadcast = False

        match conf_type:
            case ConfTypes.Json:
                self.kafka_address = conf['kafka_address']
                self.publish_timeout = conf['publish_timeout']
            case _:
                ConfTypeNotSupported(conf_type, 'KafkaSender')

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
        return self._can_broadcast and not self.kafka._closed

    def _send(self, bm:BroadcastMessage):
        from kafka.errors import KafkaError
        if not self.can_broadcast():
            self._log.error(f'Kafka cannot broadcast (not yet setup or closed)')
            return

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

    def terminate(self):
        if self.kafka:
            self.kafka.flush()
        print('kafka sender decimated')
        self._can_broadcast = False