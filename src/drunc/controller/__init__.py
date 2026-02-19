import os

# Setting GRPC_ENABLE_FORK_SUPPORT to false is used to silence a gRPC warning:
# I0000 00:00:1771500950.868506   96479 fork_posix.cc:71] Other threads are currently calling into gRPC, skipping fork() handlers
# when a gunicorn response listener is launched.
# We may transition from restAPI -> gRPC for the response listener in the future.
# In that case, fork support can be re-enabled by removing the line below.
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "false"
from drunc.utils.utils import get_logger

# Initialise controller core logger with Rich handler
# The core has the stream handler which gets redirected to the log output
get_logger("controller.core", stream_handlers=True)
