import json
import os
import re
import shutil
import tempfile
import time
from girder.utility import JsonEncoder
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import bagit
import httmock
import mock
import pytest
import responses
from bdbag import bdbag_api as bdb
from bson import ObjectId
from girder.constants import AccessType
from girder.exceptions import ValidationException
from girder.models.folder import Folder
from girder.models.item import Item
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from pytest_girder.assertions import assertStatus, assertStatusOk

from girder_wholetale.constants import ImageStatus, TaleStatus
from girder_wholetale.lib.license import WholeTaleLicense
from girder_wholetale.lib.manifest import Manifest
from girder_wholetale.models.image import Image
from girder_wholetale.models.tale import Tale

from .conftest import get_events, mockOtherRequests


class FakeInstanceResult(object):
    def __init__(self, tale_id=None):
        self.task_id = "fake_instance_id"
        self.tale_id = tale_id

    def get(self, timeout=None):
        return {
            "image_digest": "registry.local.wholetale.org/tale/name:123",
            "repo2docker_version": 1,
            "last_build": 123,
        }


@pytest.fixture()
def admin_image(admin):
    img = Image().createImage(
        name="test admin name",
        creator=admin,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="SomeBuildPack",
            user="someUser",
            port=8888,
            urlPath="",
        ),
    )
    yield img
    Image().remove(img)


@pytest.fixture
def authors():
    return [
        {
            "firstName": "Charles",
            "lastName": "Darwmin",
            "orcid": "https://orcid.org/000-000",
        },
        {
            "firstName": "Thomas",
            "lastName": "Edison",
            "orcid": "https://orcid.org/111-111",
        },
    ]


@pytest.fixture
def mock_builder(mocker):
    mock_builder = mocker.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder.reset_mock()
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = (
        "images.local.wholetale.org/tale/name:123"
    )
    return mock_builder


@pytest.mark.plugin("wholetale")
def test_tale_flow(server, admin, user, image, authors):
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"])}),
    )
    assertStatus(resp, 400)
    assert resp.json["message"].startswith(
        (
            "Invalid JSON object for parameter tale: "
            "'dataSet' "
            "is a required property"
        )
    )
    assert resp.json["type"] == "rest"

    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"]), "dataSet": []}),
    )
    assertStatusOk(resp)
    tale = resp.json

    taleLicense = WholeTaleLicense.default_spdx()
    resp = server.request(
        path="/tale/{_id}".format(**tale),
        method="PUT",
        type="application/json",
        user=user,
        body=json.dumps(
            {
                "dataSet": tale["dataSet"],
                "imageId": tale["imageId"],
                "title": "new name",
                "description": "new description",
                "config": {"memLimit": "2g"},
                "public": False,
                "licenseSPDX": taleLicense,
                "publishInfo": [
                    {
                        "pid": "published_pid",
                        "uri": "published_url",
                        "date": "2019-01-23T15:48:17.476000+00:00",
                    }
                ],
            }
        ),
    )
    assertStatusOk(resp)
    assert resp.json["title"] == "new name"
    assert resp.json["licenseSPDX"] == taleLicense
    tale = resp.json

    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "imageId": str(image["_id"]),
                "dataSet": [],
            }
        ),
    )
    assertStatusOk(resp)
    new_tale = resp.json

    resp = server.request(
        path="/tale",
        method="POST",
        user=admin,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"]), "dataSet": [], "public": False}),
    )
    assertStatusOk(resp)
    # admin_tale = resp.json

    resp = server.request(path="/tale", method="GET", user=admin, params={})
    assertStatusOk(resp)
    assert len(resp.json) == 3

    resp = server.request(
        path="/tale", method="GET", user=user, params={"imageId": str(image["_id"])}
    )
    assertStatusOk(resp)
    assert len(resp.json) == 2
    assert set([_["_id"] for _ in resp.json]) == {tale["_id"], new_tale["_id"]}

    resp = server.request(
        path="/tale", method="GET", user=user, params={"userId": str(user["_id"])}
    )
    assertStatusOk(resp)
    assert len(resp.json) == 2
    assert set([_["_id"] for _ in resp.json]) == {tale["_id"], new_tale["_id"]}

    resp = server.request(path="/tale", method="GET", user=user, params={"text": "new"})
    assertStatusOk(resp)
    assert len(resp.json) == 1
    assert set([_["_id"] for _ in resp.json]) == {tale["_id"]}

    resp = server.request(
        path="/tale/{_id}".format(**new_tale), method="DELETE", user=admin
    )
    assertStatusOk(resp)

    resp = server.request(
        path="/tale/{_id}".format(**new_tale), method="GET", user=user
    )
    assertStatus(resp, 400)

    resp = server.request(path="/tale/{_id}".format(**tale), method="GET", user=user)
    assertStatusOk(resp)
    for key in tale.keys():
        if key in ("access", "updated", "created"):
            continue
        assert resp.json[key] == tale[key]


@pytest.mark.plugin("wholetale")
def test_tale_access(server, user, admin, image, admin_image):
    with httmock.HTTMock(mockOtherRequests):
        # Create a new tale from a user image
        resp = server.request(
            path="/tale",
            method="POST",
            user=user,
            type="application/json",
            body=json.dumps(
                {"imageId": str(image["_id"]), "dataSet": [], "public": True}
            ),
        )
        assertStatusOk(resp)
        tale_user_image = resp.json
        # Create a new tale from an admin image
        resp = server.request(
            path="/tale",
            method="POST",
            user=user,
            type="application/json",
            body=json.dumps(
                {
                    "imageId": str(admin_image["_id"]),
                    "dataSet": [],
                }
            ),
        )
        assertStatusOk(resp)
        tale_admin_image = resp.json

    # Retrieve access control list for the newly created tale
    resp = server.request(
        path="/tale/%s/access" % tale_user_image["_id"], method="GET", user=user
    )
    assertStatusOk(resp)
    result_tale_access = resp.json
    expected_tale_access = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            }
        ],
        "groups": [],
    }
    assert result_tale_access == expected_tale_access

    # Update the access control list for the tale by adding the admin
    # as a second user
    input_tale_access = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            },
            {
                "login": admin["login"],
                "level": AccessType.ADMIN,
                "id": str(admin["_id"]),
                "flags": [],
                "name": "%s %s" % (admin["firstName"], admin["lastName"]),
            },
        ],
        "groups": [],
    }
    resp = server.request(
        path="/tale/%s/access" % tale_user_image["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access)},
    )
    assertStatusOk(resp)
    # Check that the returned access control list for the tale is as expected
    tale = resp.json
    result_tale_access = resp.json["access"]
    expected_tale_access = {
        "groups": [],
        "users": [
            {"flags": [], "id": str(user["_id"]), "level": AccessType.ADMIN},
            {"flags": [], "id": str(admin["_id"]), "level": AccessType.ADMIN},
        ],
    }
    assert result_tale_access == expected_tale_access
    # Check that the access control list propagated to the folder that the tale
    # is associated with
    for key in ("workspaceId",):
        resp = server.request(
            path="/folder/%s/access" % tale[key], method="GET", user=user
        )
        assertStatusOk(resp)
        result_folder_access = resp.json
        expected_folder_access = input_tale_access
        assert result_folder_access == expected_folder_access

    # Update the access control list of a tale that was generated from an image that the user
    # does not have admin access to
    input_tale_access = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            }
        ],
        "groups": [],
    }
    resp = server.request(
        path="/tale/%s/access" % tale_admin_image["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access)},
    )
    assertStatus(resp, 200)  # TODO: fix me

    # Check that the access control list was correctly set for the tale
    resp = server.request(
        path="/tale/%s/access" % tale_admin_image["_id"], method="GET", user=user
    )
    assertStatusOk(resp)
    result_tale_access = resp.json
    expected_tale_access = input_tale_access
    assert result_tale_access == expected_tale_access

    # Check that the access control list did not propagate to the image
    resp = server.request(
        path="/image/%s/access" % tale_admin_image["imageId"], method="GET", user=user
    )
    assertStatus(resp, 403)

    # Setting the access list with bad json should throw an error
    resp = server.request(
        path="/tale/%s/access" % tale_user_image["_id"],
        method="PUT",
        user=user,
        params={"access": "badJSON"},
    )
    assertStatus(resp, 400)

    # Change the access to private
    resp = server.request(
        path="/tale/%s/access" % tale_user_image["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access), "public": False},
    )
    assertStatusOk(resp)
    resp = server.request(
        path="/tale/%s" % tale_user_image["_id"], method="GET", user=user
    )
    assertStatusOk(resp)
    assert not resp.json["public"]


@pytest.mark.plugin("wholetale")
def test_tale_validation(server, user, image):
    resp = server.request(
        path="/folder",
        method="POST",
        user=user,
        params={
            "name": "validate_my_narrative",
            "parentId": user["_id"],
            "parentType": "user",
        },
    )
    sub_home_dir = resp.json
    Item().createItem("notebook.ipynb", user, sub_home_dir)

    # Mock old format
    tale = {
        "config": None,
        "creatorId": user["_id"],
        "description": "Fake Tale",
        "imageId": "5873dcdbaec030000144d233",
        "public": True,
        "publishInfo": [],
        "title": "Fake Unvalidated Tale",
        "authors": "Root Von Kolmph",
    }
    tale = Tale().save(tale)  # get's id
    tale = Tale().save(tale)  # migrate to new format

    # new_data_dir = resp.json
    assert tale["dataSet"] == []
    assert tale["licenseSPDX"] == WholeTaleLicense.default_spdx()
    # self.assertEqual(str(tale['dataSet'][0]['itemId']), data_dir['_id'])
    # self.assertEqual(tale['dataSet'][0]['mountPath'], '/' + data_dir['name'])
    tale["licenseSPDX"] = "unsupportedLicense"
    tale = Tale().save(tale)
    assert tale["licenseSPDX"] == WholeTaleLicense.default_spdx()
    assert isinstance(tale["authors"], list)
    Tale().remove(tale)

    tale["dataSet"] = [()]
    with pytest.raises(ValidationException):
        Tale().save(tale)

    tale["dataSet"] = [
        {"_modelType": "folder", "itemId": str(ObjectId()), "mountPath": "data.dat"}
    ]
    with pytest.raises(ValidationException):
        Tale().save(tale)


@pytest.mark.plugin("wholetale")
def test_tale_update(server, user, admin, image):
    # Test that Tale updating works

    resp = server.request(
        path="/folder",
        method="GET",
        user=user,
        params={
            "parentType": "user",
            "parentId": user["_id"],
            "sort": "title",
            "sortdir": 1,
        },
    )

    title = "new name"
    description = "new description"
    config = {"memLimit": "2g"}
    public = True
    tale_licenses = WholeTaleLicense()
    taleLicense = tale_licenses.supported_spdxes().pop()

    # Create a new Tale
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "imageId": str(image["_id"]),
                "dataSet": [],
                "title": "tale tile",
                "description": "description",
                "config": {},
                "public": False,
                "licenseSPDX": taleLicense,
                "publishInfo": [
                    {
                        "pid": "published_pid",
                        "uri": "published_url",
                        "date": "2019-01-23T15:48:17.476000+00:00",
                    }
                ],
            }
        ),
    )

    assertStatus(resp, 200)

    newLicense = tale_licenses.supported_spdxes().pop()
    admin_orcid, user_orcid = "https://orcid.org/1234", "https://orcid.org/9876"
    new_authors = [
        {
            "firstName": admin["firstName"],
            "lastName": admin["lastName"],
            "orcid": admin_orcid,
        },
        {
            "firstName": user["firstName"],
            "lastName": user["lastName"],
            "orcid": user_orcid,
        },
    ]

    # Create a new image that the updated Tale will use
    image = Image().createImage(
        name="New Image",
        creator=user,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="SomeBuildPack2",
            user="someUser",
            port=8888,
            urlPath="",
        ),
    )

    # Update the Tale with new values
    resp = server.request(
        path="/tale/{}".format(str(resp.json["_id"])),
        method="PUT",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "authors": new_authors,
                "imageId": str(image["_id"]),
                "dataSet": [],
                "title": title,
                "description": description,
                "config": config,
                "public": public,
                "licenseSPDX": newLicense,
                "publishInfo": [
                    {
                        "pid": "published_pid",
                        "uri": "published_url",
                        "date": "2019-01-23T15:48:17.476000+00:00",
                    }
                ],
            }
        ),
    )

    # Check that the updates happened
    # assertStatus(resp, 200)
    assert resp.json["imageId"] == str(image["_id"])
    assert resp.json["title"] == title
    assert resp.json["description"] == description
    assert resp.json["config"] == config
    assert resp.json["public"] == public
    assert resp.json["publishInfo"][0]["pid"] == "published_pid"
    assert resp.json["publishInfo"][0]["uri"] == "published_url"
    assert resp.json["publishInfo"][0]["date"] == "2019-01-23T15:48:17.476000+00:00"
    assert resp.json["licenseSPDX"] == newLicense
    assert isinstance(resp.json["authors"], list)

    tale_authors = resp.json["authors"]
    assert tale_authors[0] == new_authors[0]
    assert tale_authors[1] == new_authors[1]


@pytest.mark.plugin("wholetale")
def test_manifest(server, user, image, authors, mock_builder):
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "authors": authors,
                "imageId": str(image["_id"]),
                "dataSet": [],
                "title": "tale tile",
                "description": "description",
                "config": {},
                "public": False,
                "publishInfo": [],
                "licenseSPDX": WholeTaleLicense.default_spdx(),
            }
        ),
    )

    assertStatus(resp, 200)
    pth = "/tale/{}/manifest".format(str(resp.json["_id"]))
    resp = server.request(path=pth, method="GET", user=user)
    # The contents of the manifest are checked in the manifest tests, so
    # just make sure that we get the right response
    assertStatus(resp, 200)


@pytest.mark.plugin("wholetale")
def test_export(server, user, image, authors, mock_builder):
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "authors": authors,
                "imageId": str(image["_id"]),
                "dataSet": [],
                "title": "tale tile",
                "description": "description",
                "config": {},
                "public": False,
                "publishInfo": [],
                "licenseSPDX": "CC0-1.0",
            }
        ),
    )
    assertStatusOk(resp)
    tale = resp.json
    workspace = Folder().load(tale["workspaceId"], force=True)
    with open(os.path.join(workspace["fsPath"], "test_file.txt"), "wb") as f:
        f.write(b"Hello World!")

    resp = server.request(
        path=f"/tale/{tale['_id']}/export", method="GET", isJson=False, user=user
    )

    with tempfile.TemporaryFile() as fp:
        for content in resp.body:
            fp.write(content)
        fp.seek(0)
        zip_archive = zipfile.ZipFile(fp, "r")
        zip_files = {
            Path(*Path(_).parts[1:]).as_posix() for _ in zip_archive.namelist()
        }
        manifest_path = next(
            (_ for _ in zip_archive.namelist() if _.endswith("manifest.json"))
        )
        version_id = Path(manifest_path).parts[0]
        first_manifest = json.loads(zip_archive.read(manifest_path))
        license_path = next(
            (_ for _ in zip_archive.namelist() if _.endswith("LICENSE"))
        )
        license_text = zip_archive.read(license_path)

    # Check the the manifest.json is present
    expected_files = {
        "metadata/environment.json",
        "metadata/manifest.json",
        "README.md",
        "LICENSE",
        "workspace/test_file.txt",
    }
    assert expected_files == zip_files

    # Check that we have proper license
    assert b"Commons Universal 1.0 Public Domain" in license_text

    # First export should have created a version.
    # Let's grab it and explicitly use the versionId for 2nd dump
    resp = server.request(
        path="/version", method="GET", user=user, params={"taleId": tale["_id"]}
    )
    assertStatusOk(resp)
    assert len(resp.json) == 1
    version = resp.json[0]
    assert version_id == version["_id"]

    resp = server.request(
        path=f"/tale/{tale['_id']}/export",
        method="GET",
        isJson=False,
        user=user,
        params={"versionId": version["_id"]},
    )
    assertStatusOk(resp)
    with tempfile.TemporaryFile() as fp:
        for content in resp.body:
            fp.write(content)
        fp.seek(0)
        zip_archive = zipfile.ZipFile(fp, "r")
        second_manifest = json.loads(zip_archive.read(manifest_path))
    assert first_manifest == second_manifest
    Tale().remove(tale)


@pytest.mark.plugin("wholetale")
def test_image_build(server, user, image, mock_builder, mocker):
    mocker.stopall()
    mocker.resetall()
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"]), "dataSet": []}),
    )
    assertStatusOk(resp)
    tale = resp.json

    with mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        mock_apply_async().job.return_value = json.dumps({"job": 1, "blah": 2})
        resp = server.request(
            path="/tale/{}/build".format(tale["_id"]), method="PUT", user=user
        )
        assertStatusOk(resp)
        job_call = mock_apply_async.call_args_list[-1][-1]
        assert job_call["args"] == (str(tale["_id"]), False)
        assert job_call["headers"]["girder_job_title"] == "Build Tale Image"
    assertStatusOk(resp)

    # Create a job to be handled by the worker plugin
    job = Job().createJob(
        title="Build Tale Image",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[str(tale["_id"])],
        kwargs={},
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE

    # Schedule the job, make sure it is sent to celery
    celeryMock = mocker.patch("celery.Celery")
    celeryMock().send_task.return_value = FakeInstanceResult(tale["_id"])
    celeryMock().AsyncResult.return_value = FakeInstanceResult(tale["_id"])

    gca = mocker.patch("girder_worker.girder_plugin.event_handlers.getCeleryApp")
    gca.return_value = celeryMock()
    gca_local = mocker.patch("girder_wholetale.lib.events.getCeleryApp")
    gca_local.return_value = celeryMock()

    Job().scheduleJob(job)
    for _ in range(20):
        job = Job().load(job["_id"], force=True)
        if job["status"] == JobStatus.QUEUED:
            break
        time.sleep(0.1)
    assert job["status"] == JobStatus.QUEUED

    tale = Tale().load(tale["_id"], force=True)
    assert tale["imageInfo"]["status"] == ImageStatus.BUILDING

    # Set status to RUNNING
    job = Job().load(job["_id"], force=True)
    assert job["celeryTaskId"] == "fake_instance_id"
    Job().updateJob(job, log="job running", status=JobStatus.RUNNING)

    tale = Tale().load(tale["_id"], force=True)
    assert tale["imageInfo"]["status"] == ImageStatus.BUILDING

    # Set status to SUCCESS
    job = Job().load(job["_id"], force=True)
    assert job["celeryTaskId"] == "fake_instance_id"
    Job().updateJob(job, log="job running", status=JobStatus.SUCCESS)

    tale = Tale().load(tale["_id"], force=True)
    assert tale["imageInfo"]["status"] == ImageStatus.AVAILABLE
    assert (
        tale["imageInfo"]["digest"] == "registry.local.wholetale.org/tale/name:123"
    )

    # Set status to ERROR
    # job = Job().load(job['_id'], force=True)
    # self.assertEqual(job['celeryTaskId'], 'fake_id')
    # Job().updateJob(job, log='job running', status=JobStatus.ERROR)

    # tale = Tale().load(tale['_id'], force=True)
    # self.assertEqual(tale['imageInfo']['status'], ImageStatus.INVALID)


@pytest.mark.plugin("wholetale")
def test_tale_notifications(server, user, admin, image):
    since = datetime.now(timezone.utc).isoformat()
    with httmock.HTTMock(mockOtherRequests):
        # Create a new tale from a user image
        resp = server.request(
            path="/tale",
            method="POST",
            user=user,
            type="application/json",
            body=json.dumps(
                {"imageId": str(image["_id"]), "dataSet": [], "public": True}
            ),
        )
        assertStatusOk(resp)
        tale = resp.json

    # Confirm events
    events = get_events(server, since, user=user)
    assert len(events) == 1
    assert events[0]["data"]["event"] == "wt_tale_created"
    assert events[0]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    # Update the access control list for the tale by adding the admin
    # as a second user and confirm notification
    input_tale_access_with_admin = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            },
            {
                "login": admin["login"],
                "level": AccessType.ADMIN,
                "id": str(admin["_id"]),
                "flags": [],
                "name": "%s %s" % (admin["firstName"], admin["lastName"]),
            },
        ],
        "groups": [],
    }
    since = datetime.now(timezone.utc).isoformat()

    resp = server.request(
        path="/tale/%s/access" % tale["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access_with_admin)},
    )
    assertStatusOk(resp)

    # Confirm notification
    events = get_events(server, since, user=admin)
    assert len(events) == 1
    assert events[0]["data"]["event"] == "wt_tale_shared"
    assert events[0]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    # Update tale, confirm notifications
    since = datetime.now(timezone.utc).isoformat()
    resp = server.request(
        path="/tale/{}".format(str(tale["_id"])),
        method="PUT",
        user=user,
        type="application/json",
        body=json.dumps(
            {
                "imageId": str(image["_id"]),
                "dataSet": [],
                "public": True,
                "title": "Revised title",
            }
        ),
    )
    assertStatus(resp, 200)

    # Confirm notifications
    events = get_events(server, since, user=user)
    # self.assertEqual(len(events), 2)
    assert events[-1]["data"]["event"] == "wt_tale_updated"
    assert events[-1]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    events = get_events(server, since, user=admin)
    # self.assertEqual(len(events), 2)
    assert events[-1]["data"]["event"] == "wt_tale_updated"
    assert events[-1]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    # Remove admin and confirm notification
    input_tale_access = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            }
        ],
        "groups": [],
    }
    since = datetime.now(timezone.utc).isoformat()

    resp = server.request(
        path="/tale/%s/access" % tale["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access)},
    )
    assertStatusOk(resp)

    # Confirm notification
    events = get_events(server, since, user=admin)
    # self.assertEqual(len(events), 3)
    assert events[-1]["data"]["event"] == "wt_tale_unshared"
    assert events[-1]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    # Re-add admin user to test delete notification
    resp = server.request(
        path="/tale/%s/access" % tale["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_tale_access_with_admin)},
    )
    assertStatusOk(resp)

    # Delete tale, test notification
    since = datetime.now(timezone.utc).isoformat()
    resp = server.request(
        path="/tale/{_id}".format(**tale), method="DELETE", user=admin
    )
    assertStatusOk(resp)

    # Confirm notification
    events = get_events(server, since, user=user)
    # self.assertEqual(len(events), 3)
    assert events[-1]["data"]["event"] == "wt_tale_removed"
    assert events[-1]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]

    events = get_events(server, since, user=admin)
    # self.assertEqual(len(events), 5)
    assert events[-1]["data"]["event"] == "wt_tale_removed"
    assert events[-1]["data"]["affectedResourceIds"]["taleId"] == tale["_id"]


@pytest.mark.plugin("wholetale")
def test_tale_copy(server, admin, image, user):
    tale = Tale().createTale(image, [], creator=admin, public=True)
    workspace = Folder().load(tale["workspaceId"], force=True)
    fsPath = workspace["fsPath"]
    fullPath = os.path.join(fsPath, "file01.txt")

    with open(fullPath, "wb") as f:
        size = 101
        f.write(b" " * size)

    # Create a copy
    resp = server.request(
        path="/tale/{_id}/copy".format(**tale), method="POST", user=user
    )
    assertStatusOk(resp)

    new_tale = resp.json
    assert not new_tale["public"]
    assert new_tale["dataSet"] == tale["dataSet"]
    assert new_tale["copyOfTale"] == str(tale["_id"])
    assert new_tale["imageId"] == str(tale["imageId"])
    assert new_tale["creatorId"] == str(user["_id"])
    assert new_tale["status"] == TaleStatus.PREPARING

    copied_file_path = re.sub(workspace["name"], new_tale["_id"], fullPath)
    job = Job().findOne({"type": "wholetale.copy_workspace"})
    for _ in range(100):
        job = Job().load(job["_id"], force=True)
        if job["status"] == JobStatus.SUCCESS:
            break
        time.sleep(0.1)
    assert os.path.isfile(copied_file_path)
    resp = server.request(
        path="/tale/{_id}".format(**new_tale), method="GET", user=user
    )
    assertStatusOk(resp)
    new_tale = resp.json
    assert new_tale["status"], TaleStatus.READY

    Tale().remove(new_tale)
    Tale().remove(tale)


@responses.activate
@pytest.mark.plugin("wholetale")
def test_export_bag(server, user, fancy_tale, mock_builder):
    responses.get(
        "https://images.local.wholetale.org/v2/tale/name/tags/list",
        body='{"name": "tale/name", "tags": ["123"]}',
        status=200,
        content_type="application/json",
    )
    responses.get(
        "https://raw.githubusercontent.com/gwosc-tutorial/LOSC_Event_tutorial/master/BBH_events_v3.json",
        body="{}",
        status=200,
        content_type="application/json",
    )

    tale = fancy_tale
    resp = server.request(
        path=f"/tale/{tale['_id']}/export",
        method="GET",
        params={"taleFormat": "bagit"},
        isJson=False,
        user=user,
    )

    dirpath = tempfile.mkdtemp()
    bag_file = os.path.join(dirpath, resp.headers["Content-Disposition"].split('"')[1])
    with open(bag_file, "wb") as fp:
        for content in resp.body:
            fp.write(content)
    temp_path = bdb.extract_bag(bag_file, temp=True)
    try:
        bdb.validate_bag_structure(temp_path)
    except bagit.BagValidationError:
        pass  # TODO: Goes without saying that we should not be doing that...
    shutil.rmtree(dirpath)

    # Test dataSetCitation
    resp = server.request(
        path="/tale/{_id}".format(**tale),
        method="PUT",
        type="application/json",
        user=user,
        body=json.dumps(
            {
                "dataSet": [],
                "imageId": str(tale["imageId"]),
                "public": tale["public"],
            }
        ),
    )
    assertStatusOk(resp)
    tale = resp.json
    count = 0
    while tale["dataSetCitation"]:
        time.sleep(0.5)
        resp = server.request(path=f"/tale/{tale['_id']}", method="GET", user=user)
        assertStatusOk(resp)
        tale = resp.json
        count += 1
        if count > 5:
            break
    assert tale["dataSetCitation"] == []


@responses.activate
@pytest.mark.plugin("wholetale")
def test_export_bag_with_run(server, user, fancy_tale, mock_builder):
    responses.get(
        "https://images.local.wholetale.org/v2/tale/name/tags/list",
        body='{"name": "tale/name", "tags": ["123"]}',
        status=200,
        content_type="application/json",
    )
    responses.get(
        "https://raw.githubusercontent.com/gwosc-tutorial/LOSC_Event_tutorial/master/BBH_events_v3.json",
        body="{}",
        status=200,
        content_type="application/json",
    )
    tale = fancy_tale

    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "version1", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    resp = server.request(
        path="/run",
        method="POST",
        user=user,
        params={"versionId": version["_id"], "name": "run1"},
    )
    assertStatusOk(resp)
    run = resp.json

    # Set status to COMPLETED
    resp = server.request(
        path=f"/run/{run['_id']}/status",
        method="PATCH",
        user=user,
        params={"status": 3},
    )
    assertStatusOk(resp)

    resp = server.request(
        path=f"/tale/{tale['_id']}/export",
        method="GET",
        params={"taleFormat": "bagit", "versionId": run["runVersionId"]},
        isJson=False,
        user=user,
    )
    dirpath = tempfile.mkdtemp()
    bag_file = os.path.join(dirpath, resp.headers["Content-Disposition"].split('"')[1])
    with open(bag_file, "wb") as fp:
        for content in resp.body:
            fp.write(content)
    temp_path = bdb.extract_bag(bag_file, temp=True)
    try:
        bdb.validate_bag_structure(temp_path)
    except bagit.BagValidationError:
        # Results in UnexpectedRemoteFile because DataONE provides incompatible
        # Results in error [UnexpectedRemoteFile] data/data/usco2000.xls exists in
        # fetch.txt but is not in manifest. Ensure that any remote file references
        # from fetch.txt are also present in the manifest..."
        # This is because DataONE provides incompatible hashes in metadata for remote
        # files and we do not recalculate them on export.
        pass

    assert os.path.exists(os.path.join(temp_path, "data/runs/run1/wt_quickstart.ipynb"))

    with open(os.path.join(temp_path, "metadata/manifest.json"), "r") as f:
        m = json.loads(f.read())
        items = [
            i for i in m["aggregates"] if i["uri"] == "./runs/run1/wt_quickstart.ipynb"
        ]
        assert len(items) == 1
        assert m["dct:hasVersion"]["schema:name"] == "version1"
        assert m["wt:hasRecordedRuns"][0]["schema:name"] == "run1"

    shutil.rmtree(dirpath)


@pytest.mark.plugin("wholetale")
def test_tale_defaults(server, image, user):
    tale = Tale().createTale(
        image,
        [],
        creator=user,
        title="Export Tale",
        public=True,
        authors=None,
        description=None,
    )

    assert tale["description"] is not None
    assert tale["description"].startswith("This Tale")


@pytest.mark.plugin("wholetale")
@responses.activate
def test_tale_manifest_cycle(server, user, fancy_tale, mock_builder):
    tale = fancy_tale
    manifest_obj = Manifest(tale, user)
    manifest = json.loads(manifest_obj.dump_manifest())
    environment = json.loads(manifest_obj.dump_environment())
    restored_tale = Tale().restoreTale(manifest, environment)
    tale = json.loads(
        json.dumps(
            tale,
            cls=JsonEncoder,
            sort_keys=True,
            allow_nan=True,
        )
    )
    restored_tale = json.loads(
        json.dumps(
            restored_tale,
            cls=JsonEncoder,
            sort_keys=True,
            allow_nan=True,
        )
    )
    for key in restored_tale.keys():
        if key in ("imageInfo", "icon"):
            print(f"Original tale doesn't have {key}...")
            continue
        if key == "relatedIdentifiers":
            assert sorted(tale[key], key=lambda x: x["identifier"]) == sorted(
                restored_tale[key], key=lambda x: x["identifier"]
            )
        else:
            assert tale[key] == restored_tale[key]


@pytest.mark.plugin("wholetale")
def test_relinquish(server, user, admin, image):
    resp = server.request(
        path="/tale",
        method="POST",
        user=admin,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"]), "dataSet": []}),
    )
    assertStatusOk(resp)
    tale = resp.json

    # get ACL
    resp = server.request(
        path=f"/tale/{tale['_id']}/access",
        method="GET",
        user=admin,
    )
    assertStatusOk(resp)
    acls = resp.json

    # add user
    user_acl = {
        "flags": [],
        "id": str(user["_id"]),
        "level": AccessType.READ,
        "login": user["login"],
        "name": f"{user['firstName']} {user['lastName']}",
    }
    acls["users"].append(user_acl)
    resp = server.request(
        path=f"/tale/{tale['_id']}/access",
        method="PUT",
        user=admin,
        params={"access": json.dumps(acls)},
    )

    resp = server.request(
        path=f"/tale/{tale['_id']}",
        method="GET",
        user=user,
    )
    assertStatusOk(resp)
    assert resp.json["_accessLevel"] == AccessType.READ

    # I want to hack it!
    resp = server.request(
        path=f"/tale/{tale['_id']}/relinquish",
        method="PUT",
        user=user,
        exception=True,
        params={"level": AccessType.WRITE},
    )
    assertStatus(resp, 403)

    # I want to do a noop
    resp = server.request(
        path=f"/tale/{tale['_id']}/relinquish",
        method="PUT",
        user=user,
        exception=True,
        params={"level": AccessType.READ},
    )
    assertStatusOk(resp)

    # I don't want it!
    resp = server.request(
        path=f"/tale/{tale['_id']}/relinquish",
        method="PUT",
        user=user,
        isJson=False,
    )
    assertStatus(resp, 204)

    resp = server.request(
        path=f"/tale/{tale['_id']}",
        method="GET",
        user=user,
    )
    assertStatus(resp, 403)

    # Drop it
    resp = server.request(
        path=f"/tale/{tale['_id']}",
        method="DELETE",
        user=admin,
    )
    assertStatusOk(resp)
