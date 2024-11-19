import json

import mock
import pytest
from girder.constants import AccessType
from pytest_girder.assertions import assertStatus, assertStatusOk

from girder_wholetale.models.instance import Instance
from girder_wholetale.models.tale import Tale


@pytest.fixture
def simple_tale(server, admin, image):
    tale = Tale().createTale(image, [], creator=admin, title="Some Title")
    return tale


@pytest.mark.plugin("wholetale")
def test_tale_with_instance_delete(server, simple_tale, user, admin):
    tale = simple_tale
    tale = Tale().setUserAccess(tale, user=user, level=AccessType.WRITE, save=True)
    instance = Instance().createInstance(
        tale, user, name="instance_1", save=True, spawn=False
    )

    resp = server.request(
        path="/tale/{_id}".format(**tale),
        method="DELETE",
        user=admin,
        exception=True,
    )
    assertStatus(resp, 409)

    with mock.patch.object(Instance, "deleteInstance") as delete_mocked:
        delete_mocked.return_value = None
        resp = server.request(
            path="/tale/{_id}".format(**tale),
            params={"force": True},
            method="DELETE",
            user=admin,
        )
        delete_mocked.assert_called_once()
        call = delete_mocked.mock_calls[0]
        assert call.args[0]["_id"] == instance["_id"]
        assert call.args[1]["_id"] == user["_id"]
        Instance().remove(instance)
        assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def testTaleWithInstanceUnshare(server, simple_tale, user, admin):
    tale = simple_tale
    tale = Tale().setUserAccess(tale, user=user, level=AccessType.WRITE, save=True)
    instance = Instance().createInstance(
        tale, user, name="instance_1", save=True, spawn=False
    )

    resp = server.request(
        path="/tale/{_id}/relinquish".format(**tale),
        method="PUT",
        user=user,
        exception=True,
        params={"level": 0},
    )
    assertStatus(resp, 409)

    with mock.patch.object(Instance, "deleteInstance") as delete_mocked:
        delete_mocked.return_value = None
        resp = server.request(
            path="/tale/{_id}/relinquish".format(**tale),
            method="PUT",
            user=user,
            params={"level": 0, "force": True},
        )
        assertStatusOk(resp)
        delete_mocked.assert_called_once()
        call = delete_mocked.mock_calls[0]
        assert call.args[0]["_id"] == instance["_id"]
        assert call.args[1]["_id"] == user["_id"]
        Instance().remove(instance)
        assert resp.json["_id"] == str(tale["_id"])
        assert resp.json["_accessLevel"] == 0

    resp = server.request(path=f"/tale/{tale['_id']}/access", method="GET", user=admin)
    assertStatusOk(resp)
    orig_access = resp.json

    tale = Tale().setUserAccess(tale, user=user, level=AccessType.WRITE, save=True)
    instance = Instance().createInstance(
        tale, user, name="instance_1", save=True, spawn=False
    )

    resp = server.request(
        path=f"/tale/{tale['_id']}/access",
        params={"access": json.dumps(orig_access)},
        method="PUT",
        user=admin,
        exception=True,
    )
    assertStatus(resp, 409)
    with mock.patch.object(Instance, "deleteInstance") as delete_mocked:
        delete_mocked.return_value = None
        resp = server.request(
            path=f"/tale/{tale['_id']}/access",
            method="PUT",
            user=admin,
            params={"force": True, "access": json.dumps(orig_access)},
        )
        assertStatusOk(resp)
        delete_mocked.assert_called_once()
        call = delete_mocked.mock_calls[0]
        assert call.args[0]["_id"] == instance["_id"]
        assert call.args[1]["_id"] == user["_id"]
        Instance().remove(instance)
