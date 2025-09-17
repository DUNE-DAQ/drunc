MANAGER_SERVER_GRPC_CONFIG = [
    (
        "grpc.http2.min_ping_interval_without_data_ms",
        20_000,
    ),  # allow pings every 30s when there is no data being sent, to keep the connection alive
    (
        "grpc.keepalive_permit_without_calls",
        1,
    ),  # allow pings even when there are no calls
]
MANAGER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 10_000),  # ping connected servers every minute
    (
        "grpc.keepalive_permit_without_calls",
        1,
    ),  # allow pings even when there are no calls
]
CONTROLLER_SERVER_GRPC_CONFIG = [
    (
        "grpc.http2.min_ping_interval_without_data_ms",
        30_000,
    ),  # allow pings every 30s when there is no data being sent, to keep the connection alive
    (
        "grpc.keepalive_permit_without_calls",
        1,
    ),  # allow pings even when there are no calls
]

CONTROLLER_CLIENT_GRPC_CONFIG = [
    ("grpc.keepalive_time_ms", 60_000),  # ping connected servers every minute
    (
        "grpc.keepalive_permit_without_calls",
        1,
    ),  # allow pings even when there are no calls
]

MANAGER_SERVER_GRPC_MAX_WORKERS = 10
CONTROLLER_SERVER_GRPC_MAX_WORKERS = 10
