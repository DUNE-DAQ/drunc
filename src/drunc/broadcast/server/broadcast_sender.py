from drunc.utils.configuration_utils import ConfTypes, ConfTypeNotSupported, ConfData


class BroadcastSender:

    implementation = None

    def __init__(self, name:str, configuration:ConfData, session:str='no_session', **kwargs):
        from drunc.broadcast.server.configuration import BroadcastSenderConfiguration
        self.configuration = BroadcastSenderConfiguration(configuration)

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

        # TODO
        # self.broadcast_types_loglevels.update(self.configuration.get_raw('broadcast_types_loglevels', {}))

        #self.logger.debug(f'{broadcast_configuration.data}, {self.identifier}')
        self.impl_technology = self.configuration.get_raw('type')
        self.implementation = None

        if self.impl_technology is None:
            self.logger.warning('There is no broadcasting service!')
            return

        match self.impl_technology:
            case 'kafka':
                from drunc.broadcast.server.kafka_sender import KafkaSender
                self.implementation = KafkaSender(
                    self.configuration.get_raw('address'),
                    self.configuration.get_raw('publish_timeout'),
                    topic=f'control.{self.identifier}',
                )
            case 'grpc':
                from drunc.broadcast.server.grpc_servicer import GRCPBroadcastSender
                self.implementation = GRCPBroadcastSender()
            case _:
                from drunc.exceptions import DruncSetupException
                raise DruncSetupException(f"Broadcaster cannot be {self.impl_technology}")

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


    def _interrupt_with_exception(self, exception, context, stack=''):
        from druncschema.broadcast_pb2 import BroadcastType
        txt = f'\'{exception.__class__.__name__}\' exception thrown: {exception}'

        from drunc.exceptions import DruncException
        self.broadcast(
            btype = BroadcastType.DRUNC_EXCEPTION_RAISED if isinstance(exception, DruncException) else BroadcastType.UNHANDLED_EXCEPTION_RAISED,
            message = txt,
        )

        if stack:
            txt += '\n\n'+stack

        from google.rpc import code_pb2
        error_code = getattr(exception, 'grpc_error_code', code_pb2.INTERNAL)
        context.abort(
            code = error_code,
            details = txt,
        )


    async def _async_interrupt_with_exception(self, exception, context, stack=''):
        from druncschema.broadcast_pb2 import BroadcastType
        txt = f'\'{exception.__class__.__name__}\' exception thrown: {exception}'

        from drunc.exceptions import DruncException
        self.broadcast(
            btype = BroadcastType.DRUNC_EXCEPTION_RAISED if isinstance(exception, DruncException) else BroadcastType.UNHANDLED_EXCEPTION_RAISED,
            message = txt,
        )

        if stack:
            txt += '\n\n'+stack

        from google.rpc import code_pb2
        error_code = getattr(exception, 'grpc_error_code', code_pb2.INTERNAL)
        await context.abort(
            code = error_code,
            details = txt,
        )