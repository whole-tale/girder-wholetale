import json
from urllib.parse import quote

import httmock
import pytest
from girder.exceptions import GirderException, RestException
from girder.models.setting import Setting
from girder.models.user import User
from girder.settings import SettingKey
from girder_oauth import providers
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


def _mockPerson(response, netloc="pub.orcid.org"):
    @httmock.urlmatch(
        scheme="https", netloc=netloc, path=r"^/v3\.0/.*/person$", method="GET"
    )
    def mock(url, request):
        return json.dumps(response)

    return mock


def _mockSandboxPerson(response):
    return _mockPerson(response, netloc="api.sandbox.orcid.org")


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


def test_provider_names_are_distinct():
    assert ORCID.getProviderName() == "orcid"
    assert ORCID.getProviderName(external=True) == "ORCID"
    assert SandboxORCID.getProviderName() == "orcid_sandbox"
    assert SandboxORCID.getProviderName(external=True) == "ORCID Sandbox"


@pytest.mark.plugin("wholetale")
def test_both_providers_are_registered(server):
    assert providers.idMap["orcid"] is ORCID
    assert providers.idMap["orcid_sandbox"] is SandboxORCID


@pytest.mark.plugin("wholetale")
def test_providers_use_separate_credentials_and_callbacks(server, monkeypatch):
    monkeypatch.setattr(
        "girder_wholetale.lib.orcid.getApiUrl", lambda: "https://girder.example.com/api/v1"
    )
    Setting().set("oauth.orcid_client_id", "prod-id")
    Setting().set("oauth.orcid_sandbox_client_id", "sandbox-id")
    try:
        prodUrl = ORCID.getUrl("state")
        sandboxUrl = SandboxORCID.getUrl("state")
    finally:
        Setting().unset("oauth.orcid_client_id")
        Setting().unset("oauth.orcid_sandbox_client_id")

    assert prodUrl.startswith("https://orcid.org/oauth/authorize?")
    assert "client_id=prod-id" in prodUrl
    assert quote("/oauth/orcid/callback", safe="") in prodUrl

    assert sandboxUrl.startswith("https://sandbox.orcid.org/oauth/authorize?")
    assert "client_id=sandbox-id" in sandboxUrl
    assert quote("/oauth/orcid_sandbox/callback", safe="") in sandboxUrl


@pytest.mark.plugin("wholetale")
def test_missing_client_id_raises_with_provider_name(server):
    with pytest.raises(GirderException, match="ORCID Sandbox client ID"):
        SandboxORCID.getUrl("state")


@pytest.mark.plugin("wholetale")
def test_sandbox_get_user_creates_user_with_own_provider_id(server, orcidToken):
    response = _personResponse(email="jane.doe@example.com")
    with httmock.HTTMock(_mockSandboxPerson(response), mockOtherRequests):
        user = SandboxORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert {"provider": "orcid_sandbox", "id": TEST_ORCID_ID} in user["oauth"]


@pytest.mark.plugin("wholetale")
def test_sandbox_placeholder_email_uses_sandbox_domain(server, orcidToken):
    response = _personResponse(email=None)
    with httmock.HTTMock(_mockSandboxPerson(response), mockOtherRequests):
        user = SandboxORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert user["email"] == f"{TEST_ORCID_ID}@sandbox.orcid.org"


@pytest.mark.plugin("wholetale")
def test_same_orcid_id_in_both_namespaces_yields_separate_users(server, orcidToken):
    prodResponse = _personResponse(email="prod@example.com")
    with httmock.HTTMock(_mockPerson(prodResponse), mockOtherRequests):
        prodUser = ORCID(redirectUri="http://localhost").getUser(orcidToken)

    sandboxResponse = _personResponse(email="sandbox@example.com")
    with httmock.HTTMock(_mockSandboxPerson(sandboxResponse), mockOtherRequests):
        sandboxUser = SandboxORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert prodUser["_id"] != sandboxUser["_id"]
    assert prodUser["oauth"] == [{"provider": "orcid", "id": TEST_ORCID_ID}]
    assert sandboxUser["oauth"] == [{"provider": "orcid_sandbox", "id": TEST_ORCID_ID}]


@pytest.mark.plugin("wholetale")
def test_shared_email_links_both_providers_to_one_user(server, user, orcidToken):
    response = _personResponse(email=user["email"], firstName="user", lastName="user")
    with httmock.HTTMock(_mockPerson(response), mockOtherRequests):
        ORCID(redirectUri="http://localhost").getUser(orcidToken)
    with httmock.HTTMock(_mockSandboxPerson(response), mockOtherRequests):
        result = SandboxORCID(redirectUri="http://localhost").getUser(orcidToken)

    assert result["_id"] == user["_id"]
    reloaded = User().load(user["_id"], force=True)
    assert {"provider": "orcid", "id": TEST_ORCID_ID} in reloaded["oauth"]
    assert {"provider": "orcid_sandbox", "id": TEST_ORCID_ID} in reloaded["oauth"]


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
def test_store_other_globus_tokens_keeps_both_orcid_flavors(server, user):
    user["otherTokens"] = []
    user = User().save(user)

    for provider in (ORCID, SandboxORCID):
        token = {"access_token": provider.getProviderName(), "orcid": TEST_ORCID_ID}
        store_other_globus_tokens(
            _FakeEvent({"token": token, "user": user, "provider": provider})
        )
        user = User().load(user["_id"], force=True)

    stored = {_["resource_server"]: _["access_token"] for _ in user["otherTokens"]}
    assert stored == {"orcid.org": "orcid", "sandbox.orcid.org": "orcid_sandbox"}


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
