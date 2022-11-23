



class CommandReceiver:
    def __init__(self, controller) -> None:
        self.controller = controller
        from drunc.commandcomm.grpc.receiver import GRPCCommandReceiver
        self.receiver = GRPCCommandReceiver(self.controller)
        self.receiver.serve(5011)


    
