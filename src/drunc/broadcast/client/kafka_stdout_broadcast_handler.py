
from drunc.broadcast.client.broadcast_handler_implementation import BroadcastHandlerImplementation
from drunc.utils.conf_types import ConfTypes, ConfTypeNotSupported

class KafkaStdoutBroadcastHandler(BroadcastHandlerImplementation):

    def __init__(self, conf, message_format, conf_type:ConfTypes=ConfTypes.Json, topic=''):

        from drunc.broadcast.utils import broadcast_types_loglevels
        self.broadcast_types_loglevels = broadcast_types_loglevels # in this case, we stick with default

        import os
        drunc_shell_conf = os.getenv('DRUNC_SHELL_CONF', None)
        if drunc_shell_conf is not None:
            print(drunc_shell_conf)

            with open(drunc_shell_conf) as f:
                import json
                self.global_kafka_stdout_conf = json.load(f).get('kafka_broadcast_handler', {})
                if 'broadcast_types_loglevels' in self.global_kafka_stdout_conf:
                    self.broadcast_types_loglevels.update(self.global_kafka_stdout_conf['broadcast_types_loglevels'])

        if conf_type == ConfTypes.Json:
            self.kafka_address = conf['kafka_address']
            self.topic = topic
            if self.topic == '':
                raise RuntimeError('The topic must be specified for json configuration')

            self.broadcast_types_loglevels.update(conf.get('broadcast_types_loglevels', {}))

        elif conf_type == ConfTypes.Protobuf:
            self.kafka_address = conf.kafka_address
            self.topic = conf.topic

        else:
            raise ConfTypeNotSupported(conf_type, 'KafkaStdoutBroadcastHandler')

        self.message_format = message_format

        import logging
        self._log = logging.getLogger(f'Broadcast')

        from kafka import KafkaConsumer
        self.consumer = KafkaConsumer(
            self.topic,
            client_id = 'run_control',
            bootstrap_servers = [self.kafka_address],
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
        from druncschema.broadcast_pb2 import BroadcastType
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

                        # everything goes to info... but I'm too lazy to fix this now
                        from drunc.broadcast.utils import get_broadcast_level_from_broadcast_type
                        from druncschema.broadcast_pb2 import BroadcastType
                        bt = BroadcastType.Name(decoded.type)

                        get_broadcast_level_from_broadcast_type(decoded.type, self._log, self.broadcast_types_loglevels)(f'\'{bt}\' {txt}')
                        # self._log.info(f'"{decoded.emitter.process}.{decoded.emitter.session}": "{BroadcastType.Name(decoded.type)}" {txt}')


                    except Exception as e:
                        self._log.error(f'Weird broadcast message: {message} (error: {str(e)})')
                        text_proto = text_format.MessageToString(decoded)
                        self._log.info(text_proto)
                        raise e
