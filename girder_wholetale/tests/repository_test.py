import json

import pytest
from girder.models.user import User
from pytest_girder.assertions import assertStatus, assertStatusOk


@pytest.mark.vcr
@pytest.mark.plugin("wholetale")
def test_error_handling(server, user):
    def _lookup(url, path):
        return server.request(
            path="/repository/" + path,
            method="GET",
            user=user,
            params={"dataId": json.dumps([url])},
        )

    for path in ("lookup", "listFiles"):
        resp = _lookup("https://doi.org/10.7910/DVN/blah", path)
        assertStatus(resp, 400)
        assert resp.json == {
            "message": 'Id "https://doi.org/10.7910/DVN/blah" was '
            "categorized as DOI, but its resolution failed.",
            "type": "rest",
        }

        resp = _lookup("https://wrong.url", path)
        assertStatus(resp, 400)
        if path == "lookup":
            msg = 'Lookup for "https://wrong.url" failed with:'
        else:
            msg = 'Listing files at "https://wrong.url" failed with:'
        assert resp.json["message"].startswith(msg)


@pytest.mark.plugin("wholetale")
def test_publishers(server, user):
    # This assumes some defaults that probably should be set here instead...
    resp = server.request(path="/repository", method="GET")
    assertStatusOk(resp)
    assert resp.json == []
    # Pretend we have authorized with Zenodo
    user["otherTokens"] = [
        {
            "provider": "zenodo",
            "access_token": "zenodo_key",
            "resource_server": "sandbox.zenodo.org",
            "token_type": "apikey",
        }
    ]
    user = User().save(user)

    resp = server.request(path="/repository", method="GET", user=user)
    assertStatusOk(resp)
    assert resp.json == [
        {"name": "Zenodo Sandbox", "repository": "sandbox.zenodo.org"},
    ]
