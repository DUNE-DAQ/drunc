
from drunc.broadcast.client.broadcast_handler_implementation import BroadcastHandlerImplementation


class KafkaStdoutBroadcastHandler(BroadcastHandlerImplementation):

    def __init__(self, message_format, conf):

        from drunc.broadcast.utils import broadcast_types_loglevels
        self.broadcast_types_loglevels = broadcast_types_loglevels # in this case, we stick with default
        self.conf = conf
        # import os
        # drunc_shell_conf = os.getenv('DRUNC_SHELL_CONF', None)
        # if drunc_shell_conf is not None:

        #     with open(drunc_shell_conf) as f:
        #         import json
        #         self.global_kafka_stdout_conf = json.load(f).get('kafka_broadcast_handler', {})
        #         if 'broadcast_types_loglevels' in self.global_kafka_stdout_conf:
        #             self.broadcast_types_loglevels.update(self.global_kafka_stdout_conf['broadcast_types_loglevels'])

        self.kafka_address = self.conf.data.address
        self.topic = self.conf.data.topic

        # self.broadcast_types_loglevels.update(conf.data.get('broadcast_types_loglevels', {}))

        self.message_format = message_format

        import logging
        self._log = logging.getLogger(f'Broadcast')

        from drunc.utils.utils import now_str, get_random_string
        import getpass
        group_id = f'drunc-stdout-broadcasthandler-{getpass.getuser()}-{now_str(True)}-{get_random_string(5)}'

        from kafka import KafkaConsumer
        self.consumer = KafkaConsumer(
            self.topic,
            client_id = 'run_control',
            bootstrap_servers = [self.kafka_address],
            group_id = group_id,
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

                        from drunc.broadcast.utils import get_broadcast_level_from_broadcast_type
                        from druncschema.broadcast_pb2 import BroadcastType
                        bt = BroadcastType.Name(decoded.type)

                        get_broadcast_level_from_broadcast_type(decoded.type, self._log, self.broadcast_types_loglevels)(f'\'{bt}\' {txt}')

                    except Exception as e:
                        self._log.error(f'Weird broadcast message: {message} (error: {str(e)})')
                        text_proto = text_format.MessageToString(decoded)
                        self._log.info(text_proto)
                        pass