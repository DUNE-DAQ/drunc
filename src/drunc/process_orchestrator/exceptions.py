from drunc_core.exceptions import DruncException, DruncSetupException


class UnknownProcessOrchestratorType(DruncSetupException):
    def __init__(self, pm_type):
        super().__init__(f"'{pm_type}' is not handled/unknown")


class BadConfiguration(DruncException):
    pass


class DruncK8sNamespaceAlreadyExists(
    DruncException
):  # Exceptions that gets thrown when namespaces already exists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
