

from google.rpc import code_pb2

class DruncException(Exception): # All exceptions known to drunc
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class DruncShellException(DruncException): # Exceptions that gets thrown by shells
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class DruncSetupException(DruncException): # Exceptions that gets thrown when services start
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class DruncCommandException(DruncException): # Exceptions that gets thrown when commands run
    def __init__(self, txt, grpc_error_code=code_pb2.INTERNAL, *args, **kwargs):
        self.grpc_error_code = grpc_error_code
        super().__init__(txt, *args, **kwargs)

class DruncServerSideError(DruncException): # Exceptions that gets thrown when commands run
    def __init__(self, error_txt, stack_txt, *args, **kwargs):
        self.error_txt = error_txt
        self.stack_txt = stack_txt
        super().__init__(error_txt, stack_txt, *args, **kwargs)

    def __str__(self):
        return f'{self.stack_txt}\n{self.error_txt}'