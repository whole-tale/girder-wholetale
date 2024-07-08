import json

import httmock
import pytest
from girder.exceptions import ValidationException
from girder.models.setting import Setting
from girder.models.token import Token
from girder.models.user import User
from girder_oauth.settings import PluginSettings as OAuthPluginSettings
from pytest_girder.assertions import assertStatus, assertStatusOk

AUTH_PROVIDERS = [
    {
        "name": "orcid",
        "logo": "",
        "fullName": "ORCID",
        "tags": ["publish"],
        "url": "",
        "type": "bearer",
        "state": "unauthorized",
    },
    {
        "name": "zenodo",
        "logo": "",
        "fullName": "Zenodo",
        "tags": ["data", "publish"],
        "url": "",
        "type": "apikey",
        "docs_href": "https://zenodo.org/account/settings/applications/tokens/new/",
        "targets": [],
    },
    {
        "name": "dataverse",
        "logo": "",
        "fullName": "Dataverse",
        "tags": ["data", "publish"],
        "url": "",
        "type": "apikey",
        "docs_href": "https://dataverse.org/",
        "targets": [],
    },
]

APIKEY_GROUPS = [
    {"name": "zenodo", "targets": ["sandbox.zenodo.org", "zenodo.org"]},
    {"name": "dataverse", "targets": ["demo.dataverse.org"]},
]


@httmock.urlmatch(
    scheme="https", netloc="orcid.org", path="/oauth/token", method="POST"
)
def mockGetOrcidToken(url, request):
    return json.dumps({"access_token": "blah"})


@httmock.urlmatch(
    scheme="https", netloc="orcid.org", path="/oauth/revoke", method="POST"
)
def mockRevokeOrcidToken(url, request):
    return json.dumps({})


@httmock.all_requests
def mockOtherRequests(url, request):
    raise Exception("Unexpected url %s" % str(request.url))


@httmock.urlmatch(
    scheme="https",
    netloc="sandbox.zenodo.org",
    path="/api/deposit/depositions",
    method="POST",
)
def mockCreateDeposition(url, request):
    if request.headers["Authorization"].endswith("valid_key"):
        return httmock.response(
            status_code=201,
            content={"id": 123},
            headers={},
            reason=None,
            elapsed=5,
            request=request,
            stream=False,
        )
    else:
        return httmock.response(
            status_code=401,
            content={"cause": "reason"},
            headers={},
            reason="Some reason",
            elapsed=5,
            request=request,
            stream=False,
        )


@httmock.urlmatch(
    scheme="https",
    netloc="sandbox.zenodo.org",
    path="/api/deposit/depositions/123",
    method="DELETE",
)
def mockDeleteDeposition(url, request):
    if request.headers["Authorization"].endswith("valid_key"):
        return httmock.response(
            status_code=204,
            content=None,
            headers={},
            reason=None,
            elapsed=5,
            request=request,
            stream=False,
        )
    else:
        return httmock.response(
            status_code=401,
            content={"cause": "reason"},
            headers={},
            reason="Some reason",
            elapsed=5,
            request=request,
            stream=False,
        )


@pytest.fixture
def enabledOrcidAuth():
    """
    Side effect: enables ORCID OAuth
    """
    Setting().set(OAuthPluginSettings.ORCID_CLIENT_ID, "orcid_client_id")
    Setting().set(OAuthPluginSettings.ORCID_CLIENT_SECRET, "orcid_client_secret")
    Setting().set(OAuthPluginSettings.PROVIDERS_ENABLED, ["orcid"])
    yield
    Setting().set(OAuthPluginSettings.PROVIDERS_ENABLED, [])
    Setting().set(OAuthPluginSettings.ORCID_CLIENT_ID, "")
    Setting().set(OAuthPluginSettings.ORCID_CLIENT_SECRET, "")


@pytest.mark.plugin("wholetale")
def test_default_settings():
    from girder_wholetale.constants import PluginSettings, SettingDefault

    for key, error_msg in (
        (
            PluginSettings.EXTERNAL_AUTH_PROVIDERS,
            "Invalid External Auth Providers",
        ),
        (
            PluginSettings.EXTERNAL_APIKEY_GROUPS,
            "Invalid External Apikey Groups",
        ),
    ):
        assert Setting().get(key) == SettingDefault.defaults[key]
        with pytest.raises(ValidationException, match=error_msg):
            Setting().set(key, "blah")


@pytest.fixture
def externalAuthProviders(enabledOrcidAuth):
    from girder_wholetale.constants import PluginSettings, SettingDefault

    Setting().set(PluginSettings.EXTERNAL_AUTH_PROVIDERS, AUTH_PROVIDERS)
    Setting().set(PluginSettings.EXTERNAL_APIKEY_GROUPS, APIKEY_GROUPS)
    yield
    for key in (
        PluginSettings.EXTERNAL_AUTH_PROVIDERS,
        PluginSettings.EXTERNAL_APIKEY_GROUPS,
    ):
        Setting().set(key, SettingDefault.defaults[key])


@pytest.mark.plugin("wholetale")
def test_list_accounts(server, user, externalAuthProviders):
    resp = server.request(
        path="/account",
        method="GET",
        user=user,
        params={"redirect": "http://localhost"},
    )
    assertStatusOk(resp)
    accounts = resp.json

    assert sorted([_["name"] for _ in accounts]) == sorted(
        [_["name"] for _ in AUTH_PROVIDERS]
    )
    orcid_account = next((_ for _ in accounts if _["name"] == "orcid"))
    assert "%2Fapi%2Fv1%2Faccount%2Forcid%2Fcallback" in orcid_account["url"]

    resp = server.request(path="/account/zenodo/targets", method="GET", user=user)
    assertStatusOk(resp)
    zenodo_targets = resp.json
    assert zenodo_targets == APIKEY_GROUPS[0]["targets"]

    # Pretend we have authorized with Orcid and one Zenodo target
    other_tokens = [
        {
            "provider": "orcid",
            "access_token": "orcid_token",
            "resource_server": "orcid.org",
            "token_type": "bearer",
        },
        {
            "provider": "zenodo",
            "access_token": "zenodo_key",
            "resource_server": "sandbox.zenodo.org",
            "token_type": "apikey",
        },
        {
            "provider": "zenodo",
            "access_token": "blah",
            "resource_server": "example.org",
            "token_type": "apikey",
        },
    ]
    user["otherTokens"] = other_tokens
    user = User().save(user)

    resp = server.request(path="/account/foo/targets", method="GET", user=user)
    assertStatus(resp, 400)
    assert resp.json["message"] == 'Unknown provider "foo".'

    resp = server.request(path="/account/zenodo/targets", method="GET", user=user)
    assertStatusOk(resp)
    zenodo_targets = resp.json
    assert zenodo_targets == ["zenodo.org"]

    resp = server.request(
        path="/account",
        method="GET",
        user=user,
        params={"redirect": "http://localhost"},
    )
    assertStatusOk(resp)
    accounts = resp.json

    orcid_account = next((_ for _ in accounts if _["name"] == "orcid"))
    assert orcid_account["state"] == "authorized"
    assert orcid_account["url"].endswith("/account/orcid/revoke")

    zenodo_account = next((_ for _ in accounts if _["name"] == "zenodo"))
    assert zenodo_account["targets"][0]["resource_server"] == "sandbox.zenodo.org"


@pytest.mark.plugin("wholetale")
def test_callback(server, user, enabledOrcidAuth):
    provider_info = AUTH_PROVIDERS[0]  # ORCID

    # Try callback, for a nonexistent provider
    resp = server.request(path="/account/foobar/callback")
    assertStatus(resp, 400)

    # Try callback, without providing any params
    resp = server.request(path="/account/%s/callback" % provider_info["name"])
    assertStatus(resp, 400)

    # Try callback, providing params as though the provider failed
    resp = server.request(
        method="GET",
        path="/account/%s/callback" % provider_info["name"],
        params={"code": None, "error": "some_custom_error"},
        exception=True,
    )
    assertStatus(resp, 502)
    assert resp.json["message"] == "Provider returned error: 'some_custom_error'."

    resp = server.request(
        method="GET",
        path="/account/%s/callback" % provider_info["name"],
        params={"code": "orcid_code", "state": "some_state"},
    )
    assertStatus(resp, 403)
    assert resp.json["message"] == 'Invalid CSRF token (state="some_state").'

    invalid_token_no_user = Token().createToken(user=None, days=0.25)
    state = "{_id}.blah".format(**invalid_token_no_user)
    resp = server.request(
        method="GET",
        path="/account/%s/callback" % provider_info["name"],
        params={"code": "orcid_code", "state": state},
    )
    assertStatus(resp, 400)
    assert resp.json["message"].startswith("No valid user")

    invalid_token_expired = Token().createToken(user=user, days=1e-10)
    state = "{_id}.blah".format(**invalid_token_expired)
    resp = server.request(
        method="GET",
        path="/account/%s/callback" % provider_info["name"],
        params={"code": "orcid_code", "state": state},
    )
    assertStatus(resp, 403)
    assert resp.json["message"].startswith("Expired CSRF token")

    valid_token = Token().createToken(user=user, days=0.25)
    invalid_state = "{_id}".format(**valid_token)
    resp = server.request(
        method="GET",
        path="/account/%s/callback" % provider_info["name"],
        params={"code": "orcid_code", "state": invalid_state},
    )
    assertStatus(resp, 400)
    assert resp.json["message"].startswith("No redirect")

    valid_token = Token().createToken(user=user, days=0.25)
    valid_state = "{_id}.blah".format(**valid_token)
    with httmock.HTTMock(mockGetOrcidToken, mockOtherRequests):
        resp = server.request(
            method="GET",
            path="/account/%s/callback" % provider_info["name"],
            params={"code": "orcid_code", "state": valid_state},
            isJson=False,
        )
    assertStatus(resp, 303)
    assert "girderToken" in resp.cookie

    user = User().load(user["_id"], force=True)
    assert user["otherTokens"][0]["provider"] == "orcid"
    assert user["otherTokens"][0]["access_token"] == "blah"
    assert user["otherTokens"][0]["resource_server"] == "orcid"

    # Change token to see if it updates
    user["otherTokens"][0]["access_token"] = "different_blah"
    user = User().save(user)
    valid_token = Token().createToken(user=user, days=0.25)
    valid_state = "{_id}.blah".format(**valid_token)
    with httmock.HTTMock(mockGetOrcidToken, mockOtherRequests):
        resp = server.request(
            method="GET",
            path="/account/%s/callback" % provider_info["name"],
            params={"code": "orcid_code", "state": valid_state},
            isJson=False,
        )
    assertStatus(resp, 303)
    user = User().load(user["_id"], force=True)
    assert user["otherTokens"][0]["access_token"] == "blah"

    # Reset to defaults
    user["otherTokens"] = []
    user = User().save(user)


@pytest.mark.plugin("wholetale")
def test_revoke(server, user, externalAuthProviders):
    user["otherTokens"] = [
        {
            "provider": "orcid",
            "access_token": "orcid_token",
            "resource_server": "orcid.org",
            "token_type": "bearer",
            "refresh_token": "orcid_refresh_token",
        },
        {
            "provider": "zenodo",
            "access_token": "zenodo_key",
            "resource_server": "sandbox.zenodo.org",
            "token_type": "apikey",
        },
    ]
    user = User().save(user)
    valid_token = Token().createToken(user=user, days=0.25)

    resp = server.request(
        method="GET",
        path="/account/foo/revoke",
        params={"redirect": "somewhere", "token": valid_token["_id"]},
    )
    assertStatus(resp, 400)
    assert resp.json["message"] == "Invalid account provider (provider=foo)"

    with httmock.HTTMock(mockRevokeOrcidToken, mockOtherRequests):
        resp = server.request(
            method="GET",
            path="/account/orcid/revoke",
            params={"redirect": "https://somewhere", "token": valid_token["_id"]},
            isJson=False,
        )
        assertStatusOk(resp)

    user = User().load(user["_id"], force=True)
    assert len(user["otherTokens"]) == 1
    assert user["otherTokens"][0]["provider"] == "zenodo"

    resp = server.request(
        method="GET",
        path="/account/zenodo/revoke",
        params={"redirect": "somewhere", "token": valid_token["_id"]},
    )
    assertStatus(resp, 400)
    assert resp.json["message"].startswith("Missing resource_server")

    current_other_tokens = user["otherTokens"]
    resp = server.request(
        method="GET",
        path="/account/zenodo/revoke",
        params={
            "redirect": "somewhere",
            "resource_server": "zenodo.org",  # non exisiting, should be noop
            "token": valid_token["_id"],
        },
        isJson=False,
    )
    assertStatusOk(resp)
    user = User().load(user["_id"], force=True)
    assert current_other_tokens == user["otherTokens"]

    resp = server.request(
        method="GET",
        path="/account/zenodo/revoke",
        params={
            "redirect": "somewhere",
            "resource_server": "sandbox.zenodo.org",
            "token": valid_token["_id"],
        },
        isJson=False,
    )
    assertStatusOk(resp)
    user = User().load(user["_id"], force=True)
    assert user["otherTokens"] == []


@pytest.mark.plugin("wholetale")
def test_dataverse_apikey(server, user, externalAuthProviders):
    @httmock.urlmatch(
        scheme="https",
        netloc="demo.dataverse.org",
        path="/api/users/token",
        method="GET",
    )
    def mockDataverseVerification(url, request):
        if request.headers["X-Dataverse-key"].endswith("valid_key"):
            return httmock.response(
                status_code=200,
                content={"id": 123},
                headers={},
                reason=None,
                elapsed=5,
                request=request,
                stream=False,
            )
        else:
            return httmock.response(
                status_code=401,
                content={"cause": "reason"},
                headers={},
                reason="Some reason",
                elapsed=5,
                request=request,
                stream=False,
            )

    with httmock.HTTMock(mockDataverseVerification, mockOtherRequests):
        resp = server.request(
            method="POST",
            path="/account/dataverse/key",
            params={"resource_server": "demo.dataverse.org", "key": "key"},
            user=user,
        )
        assertStatus(resp, 400)
        assert resp.json["message"] == "Key 'key' is not valid for 'demo.dataverse.org'"

        resp = server.request(
            method="POST",
            path="/account/dataverse/key",
            params={"resource_server": "demo.dataverse.org", "key": "valid_key"},
            user=user,
        )
        assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def test_adding_apikeys(server, user, externalAuthProviders):
    resp = server.request(
        method="POST",
        path="/account/foo/key",
        params={"resource_server": "blah", "key": "key"},
        user=user,
    )
    assertStatus(resp, 400)
    assert resp.json["message"] == 'Unknown provider "foo".'

    resp = server.request(
        method="POST",
        path="/account/zenodo/key",
        params={"resource_server": "blah", "key": "key"},
        user=user,
    )
    assertStatus(resp, 400)
    assert resp.json["message"] == 'Unsupported resource server "blah".'

    with httmock.HTTMock(mockDeleteDeposition, mockCreateDeposition, mockOtherRequests):
        resp = server.request(
            method="POST",
            path="/account/zenodo/key",
            params={"resource_server": "sandbox.zenodo.org", "key": "key"},
            user=user,
        )
        assertStatus(resp, 400)
        assert resp.json["message"] == "Key 'key' is not valid for 'sandbox.zenodo.org'"

        resp = server.request(
            method="POST",
            path="/account/zenodo/key",
            params={"resource_server": "sandbox.zenodo.org", "key": "valid_key"},
            user=user,
        )
        assertStatusOk(resp)

        user = User().load(user["_id"], force=True)
        assert user["otherTokens"][0]["resource_server"] == "sandbox.zenodo.org"
        assert user["otherTokens"][0]["access_token"] == "valid_key"

        # Update
        resp = server.request(
            method="POST",
            path="/account/zenodo/key",
            params={
                "resource_server": "sandbox.zenodo.org",
                "key": "new_valid_key",
            },
            user=user,
        )
        assertStatusOk(resp)

        user = User().load(user["_id"], force=True)
        assert len(user["otherTokens"]) == 1
        assert user["otherTokens"][0]["resource_server"] == "sandbox.zenodo.org"
        assert user["otherTokens"][0]["access_token"] == "new_valid_key"
