from drunc.exceptions import DruncException


class DruncK8sException(DruncException):
    """Exception thrown when there is a Kubernetes error."""
    pass

class DruncK8sNamespaceException(DruncException):
    """Exception thrown for namespace-related errors."""
    pass


class DruncK8sPodException(DruncException):
    """Exception thrown for pod-related errors."""
    pass

class DruncK8sNodeException(DruncException):
    """Exception thrown for node-related errors such as unavailable or invalid nodes."""
    pass

