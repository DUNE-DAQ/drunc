from drunc.exceptions import DruncException


class DruncK8sException(DruncException):
    """Exception thrown when there is a Kubernetes error."""
    pass

class DruncK8sNamespaceException(DruncException):
    """Exception thrown when namespaces already exist."""
    pass


class DruncK8sPodException(DruncException):
    """Exception thrown when pods already exist."""
    pass

class DruncK8sNodeException(DruncException):
    """Exception thrown when nodes are not valid."""
    pass

