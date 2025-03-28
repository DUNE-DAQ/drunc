from drunc.exceptions import DruncException, DruncSetupException


class UnknownProcessManagerType(DruncSetupException):
    def __init__(self, pm_type):
        super().__init__(f"'{pm_type}' is not handled/unknown")


class BadConfiguration(DruncException):
    pass
