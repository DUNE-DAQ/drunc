# see https://github.com/grpc/grpc/blob/v1.74.x/include/grpc/impl/channel_arg_names.h
# for a list of all possible gRPC channel arguments
# gRPC keepalive reference: https://github.com/grpc/grpc/blob/master/doc/keepalive.md

MANAGER_SERVER_GRPC_CONFIG = []
MANAGER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 600_000),
]
CONTROLLER_SERVER_GRPC_CONFIG = []
CONTROLLER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 600_000),
]

MANAGER_SERVER_GRPC_MAX_WORKERS = 10
CONTROLLER_SERVER_GRPC_MAX_WORKERS = 10
