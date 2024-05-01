

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

class DruncK8sNamespaceAlreadyExists(DruncException): # Exceptions that gets thrown when namespaces already exists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
