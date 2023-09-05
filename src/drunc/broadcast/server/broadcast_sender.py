

class BroadcastSenderTechnologyUnknown(Exception):
    def __init__(self, implementation):
        super().__init__(f'The implementation {implementation} is not supported for the BroadcastSender')

class BroadcastSender:
    def __init__(self, name:str, session:str='no_session', broadcast_configuration:dict={}, **kwargs):
        super(BroadcastSender, self).__init__(
            **kwargs,
        )
        self.name = name
        self.session = session
        self.identifier = f'{self.name}.{self.session}'

        from logging import getLogger
        self.logger = getLogger(self.identifier)

        broadcast_types_loglevels_str = broadcast_configuration.get(
            'broadcast_types_loglevels',
            {
                'ACK'                             : 'debug',
                'RECEIVER_REMOVED'                : 'info',
                'RECEIVER_ADDED'                  : 'info',
                'SERVER_READY'                    : 'info',
                'SERVER_SHUTDOWN'                 : 'info',
                'TEXT_MESSAGE'                    : 'info',
                'COMMAND_EXECUTION_START'         : 'info',
                'COMMAND_EXECUTION_SUCCESS'       : 'info',
                'EXCEPTION_RAISED'                : 'error',
                'UNHANDLED_EXCEPTION_RAISED'      : 'critical',
                'STATUS_UPDATE'                   : 'info',
                'SUBPROCESS_STATUS_UPDATE'        : 'info',
                'DEBUG'                           : 'debug',
                'CHILD_COMMAND_EXECUTION_START'   : 'info',
                'CHILD_COMMAND_EXECUTION_SUCCESS' : 'info',
                'CHILD_COMMAND_EXECUTION_FAILED'  : 'error',
            }
        )

        self.impl_technology = broadcast_configuration.get('type')


        from collections import defaultdict
        def def_value():
            return "info"

        broadcast_types_loglevels_str_dd = defaultdict(def_value, **broadcast_types_loglevels_str)

        from druncschema.broadcast_pb2 import BroadcastType
        self.broadcast_types_loglevels = {
            v: broadcast_types_loglevels_str_dd[n].lower() for n,v in BroadcastType.items()
        }
        self.logger.debug(f'{broadcast_configuration}, {self.identifier}')

        if self.impl_technology is None:
            return
        match self.impl_technology:
            case 'kafka':
                from drunc.broadcast.server.kafka_sender import KafkaSender
                self.implementation = KafkaSender(broadcast_configuration, self.identifier)
            case 'grpc':
                from drunc.broadcast.server.grpc_servicer import GRCPBroadcastSender
                self.implementation = GRCPBroadcastSender(broadcast_configuration)
            case _:
                raise BroadcastSenderTechnologyUnknown(self.impl_technology)

    def broadcast(self, message, btype):

        if self.logger:
            from druncschema.broadcast_pb2 import BroadcastType
            f = getattr(self.logger, self.broadcast_types_loglevels[btype])
            f(f'{self.name}.{self.session} {BroadcastType.Name(btype)}: {message}')

        if self.impl_technology is None:
            # nice and easy case
            return

        from druncschema.broadcast_pb2 import BroadcastMessage, Emitter
        from druncschema.generic_pb2 import PlainText
        from drunc.utils.grpc_utils import pack_to_any
        any = pack_to_any(PlainText(text=message))
        emitter = Emitter(
            process = self.name,
            session = self.session,
        )
        bm = BroadcastMessage(
            emitter = emitter,
            type = btype,
            data = any,
        )
        bm.type = btype

        self.implementation._send(bm)
