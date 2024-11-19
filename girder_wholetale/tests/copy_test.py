import json
import os
import time

import mock
import pytest
from girder.models.folder import Folder
from pytest_girder.assertions import assertStatusOk

from girder_wholetale.constants import TaleStatus


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_copy_version(server, register_datasets, tale, user, extra_user):
    patcher = mock.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder = patcher.start()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = "some_image_digest"
    workspace = Folder().load(tale["workspaceId"], force=True)

    with open(os.path.join(workspace["fsPath"], "version1"), "wb") as fp:
        fp.write(b"This belongs to version1")

    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "First Version", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    with open(os.path.join(workspace["fsPath"], "current_file"), "wb") as fp:
        fp.write(b"This belongs to current unversioned state")
    os.remove(os.path.join(workspace["fsPath"], "version1"))

    resp = server.request(
        path=f"/tale/{tale['_id']}/copy",
        method="POST",
        user=user,
        params={"versionId": version["_id"]},
    )
    assertStatusOk(resp)
    copied_tale = resp.json

    retries = 10
    while copied_tale["status"] < TaleStatus.READY or retries > 0:
        time.sleep(0.5)
        resp = server.request(
            path=f"/tale/{copied_tale['_id']}", method="GET", user=user
        )
        assertStatusOk(resp)
        copied_tale = resp.json
        retries -= 1
    assert copied_tale["status"] == TaleStatus.READY
    workspace = Folder().load(copied_tale["workspaceId"], force=True)
    assert os.path.exists(os.path.join(workspace["fsPath"], "version1"))
    assert not os.path.exists(os.path.join(workspace["fsPath"], "current_file"))

    # Clean up
    resp = server.request(
        path=f"/tale/{copied_tale['_id']}",
        method="DELETE",
        user=user,
    )
    assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_full_copy(server, register_datasets, tale, user, extra_user):
    patcher = mock.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder = patcher.start()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = "some_image_digest"
    workspace = Folder().load(tale["workspaceId"], force=True)

    with open(os.path.join(workspace["fsPath"], "entrypoint.sh"), "wb") as fp:
        fp.write(b"echo 'Performed a run!'")

    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "First Version", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    resp = server.request(
        path="/run",
        method="POST",
        user=user,
        params={"versionId": version["_id"], "name": "test run (failed)"},
    )
    assertStatusOk(resp)
    run = resp.json

    resp = server.request(
        path=f"/run/{run['_id']}/status",
        method="PATCH",
        user=user,
        params={"status": 4},
    )

    resp = server.request(
        path="/run",
        method="POST",
        user=user,
        params={"versionId": version["_id"], "name": "test run (success)"},
    )
    assertStatusOk(resp)
    run = resp.json

    resp = server.request(
        path=f"/run/{run['_id']}/status",
        method="PATCH",
        user=user,
        params={"status": 3},
    )

    # 1. Make it public
    resp = server.request(path=f"/tale/{tale['_id']}/access", method="GET", user=user)
    assertStatusOk(resp)
    tale_access = resp.json

    resp = server.request(
        path=f"/tale/{tale['_id']}/access",
        method="PUT",
        user=user,
        params={"access": json.dumps(tale_access), "public": True},
    )
    assertStatusOk(resp)

    # 2. Perform copy as user2
    resp = server.request(
        path=f"/tale/{tale['_id']}/copy", method="POST", user=extra_user
    )
    assertStatusOk(resp)
    copied_tale = resp.json

    retries = 10
    while copied_tale["status"] < TaleStatus.READY or retries > 0:
        time.sleep(0.5)
        resp = server.request(
            path=f"/tale/{copied_tale['_id']}", method="GET", user=extra_user
        )
        assertStatusOk(resp)
        copied_tale = resp.json
        retries -= 1
    assert copied_tale["status"] == TaleStatus.READY

    resp = server.request(
        path="/version",
        method="GET",
        user=extra_user,
        params={"taleId": copied_tale["_id"]},
    )
    assertStatusOk(resp)
    assert len(resp.json) == 1
    copied_version = resp.json[0]
    assert copied_version["name"] == version["name"]

    resp = server.request(
        path="/run",
        method="GET",
        user=extra_user,
        params={"taleId": copied_tale["_id"]},
    )
    assertStatusOk(resp)
    assert len(resp.json) == 2
    copied_runs = resp.json

    assert {_["runVersionId"] for _ in copied_runs} == {copied_version["_id"]}
    assert {_["name"] for _ in copied_runs} == {
        "test run (success)",
        "test run (failed)",
    }
    assert {_["runStatus"] for _ in copied_runs} == {3, 4}
