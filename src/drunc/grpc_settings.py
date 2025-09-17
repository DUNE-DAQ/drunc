# see https://github.com/grpc/grpc/blob/v1.74.x/include/grpc/impl/channel_arg_names.h
# for a list of all possible gRPC channel arguments

MANAGER_SERVER_GRPC_CONFIG = [
    (
        "grpc.http2.min_ping_interval_without_data_ms",
        45_000,
    ),
]
MANAGER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 90_000),
]
CONTROLLER_SERVER_GRPC_CONFIG = [
    (
        "grpc.http2.min_ping_interval_without_data_ms",
        45_000,
    ),
]

CONTROLLER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 90_000),
]

MANAGER_SERVER_GRPC_MAX_WORKERS = 10
CONTROLLER_SERVER_GRPC_MAX_WORKERS = 10
