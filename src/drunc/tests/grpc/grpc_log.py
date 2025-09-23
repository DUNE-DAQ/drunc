import os

if __name__ == "__main__":
    os.environ['GRPC_TRACE'] = 'all'
    import grpc
    with grpc.insecure_channel('localhost:50051') as channel:
        pass
    