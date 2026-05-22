from drunc.utils.utils import get_logger

# Initialise process manager logger with Rich handler
# This is the tty interface, so its designed to be coloured
get_logger("resource_manager", rich_handler=True)
