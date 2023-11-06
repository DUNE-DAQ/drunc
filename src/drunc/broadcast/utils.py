
broadcast_types_loglevels = {
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

def get_broadcast_level_from_broadcast_type(btype, logger, levels=broadcast_types_loglevels):

    from druncschema.broadcast_pb2 import BroadcastType

    bt = BroadcastType.Name(btype)
    if not bt in levels:
        return logger.info
    else:
        return getattr(logger, levels[bt].lower())
