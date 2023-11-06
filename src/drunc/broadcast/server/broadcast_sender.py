
from drunc.utils.conf_types import ConfTypes, ConfTypeNotSupported

class BroadcastSenderTechnologyUnknown(Exception):
    def __init__(self, implementation):
        super().__init__(f'The implementation {str(implementation)} is not supported for the BroadcastSender')

class BroadcastSender:
    def __init__(self, name:str, session:str='no_session', broadcast_configuration:dict={}, conf_type:ConfTypes=ConfTypes.Json, **kwargs):
        super(BroadcastSender, self).__init__(
            **kwargs,
        )

        self.name = name
        self.session = session
        self.identifier = f'{self.session}.{self.name}'

        from logging import getLogger
        self.logger = getLogger('Broadcast')

        self.logger.info('Initialising broadcast')

        from drunc.broadcast.utils import broadcast_types_loglevels
        self.broadcast_types_loglevels = broadcast_types_loglevels
        self.broadcast_types_loglevels.update(broadcast_configuration.get('broadcast_types_loglevels', {}))

        self.logger.debug(f'{broadcast_configuration}, {self.identifier}')
        self.impl_technology = broadcast_configuration.get('type')

        if self.impl_technology is None:
            return

        match self.impl_technology:
            case 'kafka':
                from drunc.broadcast.server.kafka_sender import KafkaSender
                self.implementation = KafkaSender(broadcast_configuration, conf_type=conf_type, topic=f'control.{self.identifier}')
            case 'grpc':
                from drunc.broadcast.server.grpc_servicer import GRCPBroadcastSender
                self.implementation = GRCPBroadcastSender(broadcast_configuration, conf_type = conf_type)
            case _:
                self.logger.error('There is no broadcasting service!')

    def describe_broadcast(self):
        if self.implementation:
            return self.implementation.describe_broadcast()
        else:
            return None

    def can_broadcast(self):
        if not self.implementation:
            return False

        return self.implementation.can_broadcast()

    def broadcast(self, message, btype):

        if self.logger:
            from drunc.broadcast.utils import get_broadcast_level_from_broadcast_type
            get_broadcast_level_from_broadcast_type(btype, self.logger, self.broadcast_types_loglevels)(message)

        if self.implementation is None:
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

    # def broadcast_stacktrace()?

    def _interrupt_with_message(self, message, context):
        from druncschema.generic_pb2 import PlainText
        from druncschema.broadcast_pb2 import BroadcastType
        from drunc.utils.grpc_utils import pack_to_any
        from google.rpc import code_pb2
        from google.rpc import status_pb2
        from grpc_status import rpc_status
        self.broadcast(
            btype = BroadcastType.TEXT_MESSAGE,
            message = message
        )

        detail = pack_to_any(PlainText(text = message))

        context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=code_pb2.INTERNAL,
                    message=message,
                    details=[detail],
                )
            )
        )


    def _interrupt_with_exception(self, ex_stack, ex_text, context):
        from druncschema.broadcast_pb2 import BroadcastType

        self.broadcast(
            btype = BroadcastType.EXCEPTION_RAISED,
            message = ex_text
        )
        self.logger.error(
            ex_stack+"\n"+ex_text
        )

        from druncschema.generic_pb2 import Stacktrace
        from drunc.utils.grpc_utils import pack_to_any
        from google.rpc import code_pb2
        from google.rpc import status_pb2
        from grpc_status import rpc_status

        detail = pack_to_any(
            Stacktrace(
                text = ex_stack.split('\n')
            )
        )

        context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=code_pb2.INTERNAL,
                    message=f'Exception thrown: {str(ex_text)}',
                    details=[detail],
                )
            )
        )
