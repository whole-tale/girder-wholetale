import json
import os
import pathlib
import time

import mock
import pytest
from girder import events
from girder.models.folder import Folder
from girder.models.setting import Setting
from pytest_girder.assertions import assertStatus, assertStatusOk

from .conftest import _compare_tales


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_basic_version_ops(
    server, register_datasets, dataset, tale, user, extra_user, image_two
):
    from girder_wholetale.constants import PluginSettings, TaleStatus
    from girder_wholetale.models.tale import Tale

    patcher = mock.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder = patcher.start()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = "some_image_digest"

    workspace = Folder().load(tale["workspaceId"], force=True)

    file1_content = b"Hello World!"
    file1_name = "test_file.txt"
    file2_content = b"I'm in a directory!"
    file2_name = "file_in_a_dir.txt"
    dir_name = "some_directory"

    with open(os.path.join(workspace["fsPath"], file1_name), "wb") as f:
        f.write(file1_content)

    resp = server.request(
        path="/version/exists",
        method="GET",
        user=user,
        params={"name": "First Version", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    assert not resp.json["exists"]

    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "First Version", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    resp = server.request(
        path="/version/exists",
        method="GET",
        user=user,
        params={"name": "First Version", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    assert resp.json["exists"]
    assert resp.json["obj"]["_id"] == version["_id"]

    version_root = Setting().get(PluginSettings.VERSIONS_DIRS_ROOT)
    version_path = pathlib.Path(version_root) / str(tale["_id"])[:2] / str(tale["_id"])

    assert version_path.is_dir()
    should_be_a_file = version_path / version["_id"] / "workspace" / file1_name
    assert should_be_a_file.is_file()

    # Try to create a version with no changes (should fail)
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatus(resp, 303)
    assert resp.json == {
        "extra": str(version["_id"]),
        "message": "Not modified",
        "type": "rest",
    }

    # Make some modification to the workspace
    workspace_path = pathlib.Path(workspace["fsPath"])
    workspace_dir = workspace_path / dir_name
    workspace_dir.mkdir()
    nested_file = workspace_dir / file2_name
    with open(nested_file.as_posix(), "wb") as f:
        f.write(file2_content)

    # Make some mods to Tale itself
    resp = server.request(
        path=f"/tale/{tale['_id']}",
        method="GET",
        user=user,
    )
    assertStatusOk(resp)
    first_version_tale = resp.json

    tale = Tale().load(tale["_id"], force=True)
    tale["dataSet"] = dataset(user, 1)
    tale["authors"].append(
        {
            "firstName": "Craig",
            "lastName": "Willis",
            "orcid": "https://orcid.org/0000-0002-6148-7196",
        }
    )
    tale.update(
        {
            "category": "rocket science",
            "config": {"foo": "bar"},
            "description": "A better description",
            "imageId": image_two["_id"],
            "title": "New better title",
        }
    )
    tale = Tale().save(tale)

    # Try to create a 2nd version, but using old name (should fail)
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "First Version", "taleId": str(tale["_id"])},
    )
    assertStatus(resp, 409)
    assert resp.json == {
        "message": f"Name already exists: {version['name']}",
        "type": "rest",
    }

    # Try to create a 2nd version providing no name (should work)
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    new_version = resp.json
    year = new_version["created"][:4]
    assert year in new_version["name"]  # it's a date

    # Check that Tale has two versions
    resp = server.request(
        path="/version",
        method="GET",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    assert len(resp.json) == 2
    assert list(_["_id"] for _ in resp.json) == [version["_id"], new_version["_id"]]

    # Rename 2nd version to something silly (should fail)
    resp = server.request(
        path=f"/version/{new_version['_id']}",
        method="PUT",
        user=user,
        params={"name": "*/*"},
    )
    assertStatus(resp, 400)

    # Rename 2nd version to 2nd version (should work)
    resp = server.request(
        path=f"/version/{new_version['_id']}",
        method="PUT",
        user=user,
        params={"name": "Second version"},
    )
    assertStatusOk(resp)
    new_version = resp.json

    # Check if GET /version/:id works
    resp = server.request(
        path=f"/version/{new_version['_id']}", method="GET", user=user
    )
    assertStatusOk(resp)
    new_version["updated"] = resp.json["updated"]  # There's a small drift between those
    assert new_version == resp.json

    # Check if data is where it's supposed to be
    should_be_a_file = (
        version_path / new_version["_id"] / "workspace" / dir_name / file2_name
    )
    assert should_be_a_file.is_file()

    # Try to create a version with no changes (should fail) test recursion
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatus(resp, 303)
    assert resp.json == {
        "extra": str(new_version["_id"]),
        "message": "Not modified",
        "type": "rest",
    }

    # View First Version
    resp = server.request(
        method="GET",
        user=user,
        path=f"/tale/{tale['_id']}/restore",
        params={"versionId": version["_id"]},
    )
    assertStatusOk(resp)
    view_tale = resp.json
    assert view_tale["workspaceId"].startswith("wtlocal:")
    _compare_tales(view_tale, first_version_tale)

    # Do the same thing via event
    event = events.trigger(
        "tale.view_restored", info={"tale": tale, "version": version}
    )
    event_tale = Tale().filter(event.responses[0], user)
    assert event_tale["workspaceId"] == view_tale["workspaceId"]
    _compare_tales(event_tale, first_version_tale)

    # Restore First Version
    resp = server.request(
        method="PUT",
        user=user,
        path=f"/tale/{tale['_id']}/restore",
        params={"versionId": version["_id"]},
    )
    assertStatusOk(resp)
    restored_tale = resp.json
    _compare_tales(restored_tale, first_version_tale)

    for key in restored_tale.keys():
        if key in {
            "created",
            "updated",
            "restoredFrom",
            "imageInfo",
            "dataSetCitation",  # slow
            "icon",  # TODO: bug
        }:
            continue
        try:
            assert restored_tale[key] == first_version_tale[key]
        except AssertionError:
            print(key)
            raise

    workspace = Folder().load(restored_tale["workspaceId"], force=True)
    workspace_path = pathlib.Path(workspace["fsPath"])
    w_should_be_a_file = workspace_path / file1_name
    assert w_should_be_a_file.is_file()
    w_should_not_be_a_file = workspace_path / dir_name / file2_name
    assert not w_should_not_be_a_file.is_file()

    # Remove and see if it's gone
    resp = server.request(
        path=f"/version/{new_version['_id']}", method="DELETE", user=user
    )
    assertStatusOk(resp)
    assert not should_be_a_file.is_file()
    resp = server.request(
        path=f"/version/{new_version['_id']}", method="GET", user=user
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "message": f"Invalid folder id ({new_version['_id']}).",
        "type": "rest",
    }

    # Test allow rename
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={
            "name": "First Version",
            "taleId": tale["_id"],
            "allowRename": True,
            "force": True,
        },
    )
    assertStatusOk(resp)
    assert resp.json["name"] == "First Version (1)"

    # Test copying Tale
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

    # 3. Check that copied Tale has two versions
    resp = server.request(
        path="/version",
        method="GET",
        user=extra_user,
        params={"taleId": copied_tale["_id"]},
    )
    assertStatusOk(resp)
    assert len(resp.json) == 2


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_dataset_handling(server, register_datasets, dataset, tale, user):
    patcher = mock.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder = patcher.start()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = "some_image_digest"
    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    # Check if dataset was stored
    resp = server.request(
        path=f"/version/{version['_id']}/dataSet", method="GET", user=user
    )
    assertStatusOk(resp)
    assert len(resp.json) == 1
    assert resp.json[0]["itemId"] == tale["dataSet"][0]["itemId"]


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_force_version(
    server, register_datasets, dataset, tale, user, extra_user, image_two
):
    patcher = mock.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder = patcher.start()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = "some_image_digest"
    # Check that the tale has no versions.
    resp = server.request(
        path="/version",
        method="GET",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    assert resp.json == []

    # We're doing it twice to verify that only one version is created
    # if there are no changes to the Tale.
    for _ in range(2):
        # Export the Tale. This should trigger the event to create the new version
        resp = server.request(
            path=f"/tale/{tale['_id']}/export",
            method="GET",
            user=user,
            isJson=False,
        )
        assertStatusOk(resp)

        # Get the versions for this Tale; there should only by a single one
        # triggered by the export event
        resp = server.request(
            path="/version",
            method="GET",
            user=user,
            params={"taleId": tale["_id"]},
        )
        assertStatusOk(resp)
        assert len(resp.json) == 1
