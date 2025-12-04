from drunc.utils.utils import get_logger

# Initialise controller core logger with Rich handler
# The core has the stream handler which gets redirected to the log output
get_logger("controller.core", stream_handlers=True)
