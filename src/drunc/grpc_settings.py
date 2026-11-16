# see https://github.com/grpc/grpc/blob/v1.74.x/include/grpc/impl/channel_arg_names.h
# for a list of all possible gRPC channel arguments
# gRPC keepalive reference: https://github.com/grpc/grpc/blob/master/doc/keepalive.md


MANAGER_SERVER_GRPC_CONFIG = []
MANAGER_CLIENT_GRPC_CONFIG = []
CONTROLLER_SERVER_GRPC_CONFIG = []
CONTROLLER_CLIENT_GRPC_CONFIG = []

MANAGER_SERVER_GRPC_MAX_WORKERS = 10
CONTROLLER_SERVER_GRPC_MAX_WORKERS = 10

# If set, the address a controller advertises to the connectivity service
# instead of the one it binds to (set by the K8s process manager)
CONTROLLER_ADVERTISE_ADDRESS_ENV = "DRUNC_CONTROLLER_ADVERTISE_ADDRESS"
