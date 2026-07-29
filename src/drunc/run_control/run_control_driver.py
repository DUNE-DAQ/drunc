from druncschema.run_control_pb2_grpc import RunControlStub


class RunControlDriver:
    def __init__(self, config):
        self.config = config
        self.stub = RunControlStub(config)

    def validate_session(self):
        pass

    def start_session(self):
        pass

    def end_session(self):
        pass
