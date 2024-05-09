from drunc.exceptions import DruncException

class DruncK8sNamespaceAlreadyExists(DruncException): # Exceptions that gets thrown when namespaces already exists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
