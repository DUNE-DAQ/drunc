
from drunc.exceptions import DruncException

class ApplicationRegistryNotPresent(DruncException):
    pass

class ApplicationRegistrationUnsuccessful(DruncException):
    pass

class ApplicationLookupUnsuccessful(DruncException):
    pass


class ApplicationRegistryClient:
    def __init__(self, session:str, address:str):
        self.session = session
        from logging import getLogger
        self.logger = getLogger('ApplicationRegistryClient')

        if address.startswith('http'):
            self.address = f'{address}/app-control-connection'
        else:
            self.address = f'http://{address}/app-control-connection'

    def remove(self, name):
        from drunc.utils.utils import http_delete
        data = {
            'session': self.session,
            'name': name
        }
        try:
            http_delete(
                self.address,
                data = data
            )
        except Exception as e:
            self.logger.critical(f'Could not find the address of \'{name}\' on the application registry:\n{str(e)}')
            raise ApplicationLookupUnsuccessful from e




    def lookup(self, name):
        from drunc.utils.utils import http_get
        data = {
            'session': self.session,
            'name': name
        }
        try:
            http_get(
                self.address,
                data = data
            )
        except Exception as e:
            self.logger.critical(f'Could not find the address of \'{name}\' on the application registry:\n{str(e)}')
            raise ApplicationLookupUnsuccessful from e

    def update(self, name, endpoint=None):
        from drunc.utils.utils import http_update
        data = {
            'session': self.session,
            'name': name
        }
        if endpoint:
            data['endpoint'] = endpoint
        try:
            http_update(
                self.address,
                data = data
            )
        except Exception as e:
            self.logger.critical(f'Could not find the address of \'{name}\' on the application registry:\n{str(e)}')
            raise ApplicationLookupUnsuccessful from e


    def register(self, name, address):
        from drunc.utils.utils import http_post
        try:
            http_post(
                self.address,
                data = {
                    'session': self.session,
                    'endpoints':[
                        {
                            'endpoint': address,
                            'name': name
                        }
                    ]
                }
            )

        except Exception as e: # do we still want to run if we cannot advertise ourselves?
            self.logger.critical(f'Could not post the address of self ({address}) on the application registry:\n{str(e)}')
            raise ApplicationRegistrationUnsuccessful from e
