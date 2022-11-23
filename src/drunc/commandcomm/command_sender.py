

class CommandSender:
    def __init__(self):
        from drunc.commandcomm.grpc.sender import GRPCCommandSender
        self.sender = GRPCCommandSender()

    def send(self, message:str='') -> None:
        self.sender('yo pierre', 'localhost:3100')
