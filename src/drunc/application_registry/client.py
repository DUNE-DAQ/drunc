from requests.exceptions import HTTPError
from drunc.exceptions import DruncException

class ApplicationRegistryNotPresent(DruncException):
    pass

class ApplicationRegistrationUnsuccessful(DruncException):
    pass

class ApplicationLookupUnsuccessful(DruncException):
    pass

class ApplicationUpdateUnsuccessful(DruncException):
    pass


class ApplicationRegistryClient:
    def __init__(self, session:str, address:str):
        self.session = session
        from logging import getLogger
        self.logger = getLogger('ApplicationRegistryClient')

        if address.startswith('http'):
            self.address = f'{address}/app-registry/v0.0.0/app-control-connection'
        else:
            self.address = f'http://{address}/app-registry/v0.0.0/app-control-connection'

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
        for _ in range(20):
            try:
                response = http_get(
                    self.address,
                    data = data,
                    timeout = 0.5,
                )
                print(response.json())
                endpoint = response.json()[0]['endpoint']
                return endpoint
            except HTTPError as e:
                from time import sleep
                sleep(0.5)
                continue

        self.logger.critical(f'Could not find the address of \'{name}\' on the application registry')
        raise ApplicationLookupUnsuccessful


    def patch(self, name, endpoint=None):
        from drunc.utils.utils import http_patch
        data = {
            'session': self.session,
            'name': name
        }
        if endpoint:
            data['endpoint'] = endpoint

        for _ in range(5):
            try:
                response = http_patch(
                    self.address,
                    data = data
                )
                return response
            except HTTPError as e:
                from time import sleep
                sleep(0.5)
                continue

        self.logger.critical(f'Could not update the address of \'{name}\' on the application registry')
        raise ApplicationUpdateUnsuccessful


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
