import requests
from girder.exceptions import RestException

from ..verificator import Verificator


class DataverseVerificator(Verificator):
    @property
    def headers(self):
        if self.key:
            return {"X-Dataverse-key": f"{self.key}"}
        return {}

    def verify(self):
        try:
            r = requests.get(
                f"https://{self.resource_server}/api/users/token", headers=self.headers
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise RestException(
                f"Key '{self.key}' is not valid for '{self.resource_server}'"
            )
