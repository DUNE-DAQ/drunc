import asyncio
from concurrent import futures
import grpc
import drunc.commandcomm.grpc.command_pb2 as command_pb2
import drunc.commandcomm.grpc.command_pb2_grpc as command_pb2_grpc

class CommandReceiverServicer(command_pb2_grpc.CommandReceiverServicer):

    def __init__(self, controller, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.controller = controller

    async def ExecuteCommand(self, request, context) -> command_pb2.CommandResponse:
        print(request)
        self.controller.execute_command()
        return CommandResponse(request.name)

class GRPCCommandReceiver(CommandReceiver):
    def __init__(self, controller, *args, **kwargs):
        super().__init__(controller, *args, **kwargs)
        # self.servicer = servicer

    def serve(self,port:int) -> None:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.service.add_CommandSenderServicer_to_server(CommandSenderServicer(), server)
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        print(f'Server started on port {port}')
        server.wait_for_termination()


# # import helloworld_pb2
# # import helloworld_pb2_grpc



# def serve():
#     port = '50051'
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
#     server.add_insecure_port('[::]:' + port)
#     server.start()
#     print("Server started, listening on " + port)
#     server.wait_for_termination()
