import pytest
import responses
from girder.exceptions import RestException
from girder.models.user import User

from girder_wholetale.lib.dataverse.auth import DataverseVerificator
from girder_wholetale.lib.verificator import Verificator
from girder_wholetale.lib.zenodo.auth import ZenodoVerificator


@pytest.mark.plugin("wholetale")
def test_base_errors(server):
    msg = "Either 'resource_server' or 'url' must be provided"
    with pytest.raises(ValueError, match=msg):
        Verificator()
    msg = "Either 'key' or 'user' must be provided"
    with pytest.raises(ValueError, match=msg):
        Verificator(resource_server="zenodo.org")

    verificator = Verificator(resource_server="some server", key="some key")
    assert verificator.headers == {}
    with pytest.raises(NotImplementedError):
        verificator.verify()


@responses.activate
@pytest.mark.plugin("wholetale")
def test_dataverse_verificator(server, user):
    responses.add(
        responses.GET,
        "https://dataverse.harvard.edu/api/users/token",
        json={"status": "ERROR", "message": "Token blah not found."},
        status=404,
    )

    verificator = DataverseVerificator(
        resource_server="dataverse.harvard.edu", user=user
    )
    assert verificator.headers == {}

    user["otherTokens"] = [
        {"resource_server": "dataverse.harvard.edu", "access_token": "blah"}
    ]
    user = User().save(user)
    verificator = DataverseVerificator(
        resource_server="dataverse.harvard.edu", user=user
    )
    assert verificator.headers == {"X-Dataverse-key": "blah"}

    with pytest.raises(RestException):
        verificator.verify()  # Invalid key


@responses.activate
@pytest.mark.plugin("wholetale")
def test_zenodo_verificator(server, user):
    responses.add(
        responses.POST,
        "https://sandbox.zenodo.org/api/deposit/depositions",
        json={
            "message": (
                "The server could not verify that you are authorized to access the URL "
                "requested. You either supplied the wrong credentials (e.g. a bad passw"
                "ord), or your browser doesn't understand how to supply the credentials"
                "required."
            ),
            "status": 401,
        },
        status=401,
    )

    verificator = ZenodoVerificator(resource_server="sandbox.zenodo.org", user=user)
    assert verificator.headers == {}

    user["otherTokens"] = [
        {"resource_server": "sandbox.zenodo.org", "access_token": "blah"}
    ]
    user = User().save(user)
    verificator = ZenodoVerificator(resource_server="sandbox.zenodo.org", user=user)
    assert verificator.headers == {"Authorization": "Bearer blah"}

    with pytest.raises(RestException):
        verificator.verify()  # Invalid key
