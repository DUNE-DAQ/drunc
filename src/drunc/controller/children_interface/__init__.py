from drunc.utils.utils import get_logger

# Initialise child interface with stream
# Gets redirected to log
get_logger("controller.child_iface", stream_handlers=True)
