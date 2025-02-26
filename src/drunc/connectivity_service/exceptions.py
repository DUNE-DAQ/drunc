from drunc.exceptions import DruncException


class ApplicationRegistryNotPresent(DruncException):
    pass


class ApplicationRegistrationUnsuccessful(DruncException):
    pass


class ApplicationLookupUnsuccessful(DruncException):
    pass


class ApplicationUpdateUnsuccessful(DruncException):
    pass
