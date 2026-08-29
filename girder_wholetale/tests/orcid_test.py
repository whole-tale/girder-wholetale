import json

import httmock
import pytest
from girder.exceptions import RestException
from girder.models.setting import Setting
from girder.models.user import User
from girder.settings import SettingKey
from girder_oauth.settings import PluginSettings as OAuthPluginSettings

from girder_wholetale import store_other_globus_tokens
from girder_wholetale.lib.orcid import ORCID, SandboxORCID

TEST_ORCID_ID = "0000-0001-2345-6789"


def _personResponse(email=None, firstName="Jane", lastName="Doe", includeName=True):
    body = {"emails": {"email": [{"email": email}] if email else []}}
    if includeName:
        body["name"] = {
            "family-name": {"value": lastName},
            "given-names": {"value": firstName},
        }
    return body


def _mockPerson(response):
    @httmock.urlmatch(
        scheme="https", netloc="pub.orcid.org", path=r"^/v3\.0/.*/person$", method="GET"
    )
    def mock(url, request):
        return json.dumps(response)

    return mock


@httmock.all_requests
def mockOtherRequests(url, request):
    raise AssertionError(f"Unexpected url {request.url!s}")


@pytest.fixture
def orcidToken():
    return {"access_token": "blah", "orcid": TEST_ORCID_ID}


@pytest.fixture
def closedRegistration():
    Setting().set(SettingKey.REGISTRATION_POLICY, "closed")
    yield
    Setting().set(SettingKey.REGISTRATION_POLICY, "open")


@pytest.mark.plugin("wholetale")
def test_get_user_creates_new_user(server, orcidToken):
    response = _personResponse(email="jane.doe@example.com")
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        user = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert user["email"] == "jane.doe@example.com"
    assert user["firstName"] == "Jane"
    assert user["lastName"] == "Doe"
    assert {"provider": "orcid", "id": TEST_ORCID_ID} in user["oauth"]


@pytest.mark.plugin("wholetale")
def test_get_user_missing_email_defaults_to_orcid_placeholder(server, orcidToken):
    response = _personResponse(email=None)
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        user = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert user["email"] == f"{TEST_ORCID_ID}@orcid.org"


@pytest.mark.plugin("wholetale")
def test_get_user_missing_name_key_falls_back_to_na(server, orcidToken):
    response = _personResponse(email="jane.doe@example.com", includeName=False)
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        user = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert user["firstName"] == "N/A"
    assert user["lastName"] == "N/A"


@pytest.mark.plugin("wholetale")
def test_get_user_missing_orcid_id_raises(server):
    response = _personResponse(email="jane.doe@example.com")
    with (
        httmock.HTTMock(_mockPerson(response), mockOtherRequests),
        pytest.raises(RestException, match="did not return a user ID"),
    ):
        ORCID(redirectUri="http://localhost").getUser(
            {"access_token": "blah", "orcid": ""}
        )


@pytest.mark.plugin("wholetale")
def test_get_user_empty_name_raises(server, orcidToken):
    response = _personResponse(email="jane.doe@example.com", firstName="", lastName="")
    with (
        httmock.HTTMock(_mockPerson(response), mockOtherRequests),
        pytest.raises(RestException, match="did not return a user name"),
    ):
        ORCID(redirectUri="http://localhost").getUser(orcidToken)


@pytest.mark.plugin("wholetale")
def test_get_user_updates_existing_user_matched_by_oauth_id(server, admin, orcidToken):
    admin["oauth"] = [{"provider": "orcid", "id": TEST_ORCID_ID}]
    admin = User().save(admin)

    response = _personResponse(
        email="new.email@example.com", firstName="NewFirst", lastName="NewLast"
    )
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        user = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert user["_id"] == admin["_id"]
    assert user["email"] == "new.email@example.com"
    assert user["firstName"] == "NewFirst"
    assert user["lastName"] == "NewLast"


@pytest.mark.plugin("wholetale")
def test_get_user_links_existing_user_matched_by_email(server, user, orcidToken):
    response = _personResponse(email=user["email"], firstName="user", lastName="user")
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        result = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert result["_id"] == user["_id"]
    reloaded = User().load(user["_id"], force=True)
    assert {"provider": "orcid", "id": TEST_ORCID_ID} in reloaded["oauth"]


@pytest.mark.plugin("wholetale")
def test_get_user_registration_closed_raises(server, orcidToken, closedRegistration):
    response = _personResponse(email="closed@example.com")
    with (
        httmock.HTTMock(_mockPerson(response), mockOtherRequests),
        pytest.raises(RestException, match="Registration is closed"),
    ):
        ORCID(redirectUri="http://localhost").getUser(orcidToken)


@pytest.mark.plugin("wholetale")
def test_get_user_registration_closed_ignored(server, orcidToken, closedRegistration):
    Setting().set(OAuthPluginSettings.IGNORE_REGISTRATION_POLICY, True)
    try:
        response = _personResponse(email="ignored@example.com")
        with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
            user = ORCID(redirectUri="http://localhost").getUser(orcidToken)
        assert user["email"] == "ignored@example.com"
    finally:
        Setting().set(OAuthPluginSettings.IGNORE_REGISTRATION_POLICY, False)


def test_sandbox_orcid_provider_name():
    assert SandboxORCID.getProviderName() == "orcid"
    assert SandboxORCID.getProviderName(external=True) == "ORCID"


class _FakeEvent:
    def __init__(self, info):
        self.info = info


class _FakeGlobusProvider:
    @staticmethod
    def getProviderName():
        return "globus"


class _FakeOtherProvider:
    @staticmethod
    def getProviderName():
        return "github"


@pytest.mark.plugin("wholetale")
def test_store_other_globus_tokens_orcid_branch(server, user):
    user["otherTokens"] = []
    user = User().save(user)

    token = {"access_token": "blah", "orcid": TEST_ORCID_ID}
    store_other_globus_tokens(_FakeEvent({"token": token, "user": user, "provider": ORCID}))

    reloaded = User().load(user["_id"], force=True)
    assert len(reloaded["otherTokens"]) == 1
    stored = reloaded["otherTokens"][0]
    assert stored["access_token"] == "blah"
    assert stored["resource_server"] == "orcid.org"


@pytest.mark.plugin("wholetale")
def test_store_other_globus_tokens_orcid_branch_updates_existing(server, user):
    user["otherTokens"] = [{"resource_server": "orcid.org", "access_token": "old"}]
    user = User().save(user)

    token = {"access_token": "new", "orcid": TEST_ORCID_ID}
    store_other_globus_tokens(_FakeEvent({"token": token, "user": user, "provider": ORCID}))

    reloaded = User().load(user["_id"], force=True)
    assert len(reloaded["otherTokens"]) == 1
    assert reloaded["otherTokens"][0]["access_token"] == "new"


@pytest.mark.plugin("wholetale")
def test_store_other_globus_tokens_globus_branch(server, user):
    user["otherTokens"] = []
    user = User().save(user)

    token = {
        "other_tokens": [
            {"resource_server": "transfer.api.globus.org", "access_token": "abc"}
        ]
    }
    store_other_globus_tokens(
        _FakeEvent({"token": token, "user": user, "provider": _FakeGlobusProvider})
    )

    reloaded = User().load(user["_id"], force=True)
    assert reloaded["otherTokens"] == [
        {"resource_server": "transfer.api.globus.org", "access_token": "abc"}
    ]


@pytest.mark.plugin("wholetale")
def test_store_other_globus_tokens_unrelated_provider_is_noop(server, user):
    user["otherTokens"] = []
    user = User().save(user)

    token = {"access_token": "irrelevant"}
    store_other_globus_tokens(
        _FakeEvent({"token": token, "user": user, "provider": _FakeOtherProvider})
    )

    reloaded = User().load(user["_id"], force=True)
    assert reloaded["otherTokens"] == []
