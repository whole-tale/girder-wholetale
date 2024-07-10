import json
import os
import time

import mock
import pytest
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from pytest_girder.assertions import assertStatusOk

from girder_wholetale.constants import TaleStatus
from girder_wholetale.models.image import Image
from girder_wholetale.models.tale import Tale


@pytest.mark.xfail(reason="Needs to be ported to Celery")
@pytest.mark.plugin("wholetale")
def testTaleImportBinderFail(server, user, image):
    with mock.patch("girder_wholetale.lib.pids_to_entities") as mock_pids:
        mock_pids.side_effect = ValueError
        resp = server.request(
            path="/tale/import",
            method="POST",
            user=user,
            params={
                "url": "http://use.yt/upload/ef4cd901",
                "spawn": False,
                "imageId": image["_id"],
                "asTale": True,
                "taleKwargs": json.dumps({"title": "tale should fail"}),
            },
        )
        assertStatusOk(resp)
        tale = resp.json

        job = Job().findOne({"type": "wholetale.import_binder"})
        assert json.loads(job["kwargs"])["taleId"]["$oid"] == tale["_id"]

        for i in range(300):
            if job["status"] in {JobStatus.SUCCESS, JobStatus.ERROR}:
                break
            time.sleep(0.1)
            job = Job().load(job["_id"], force=True)
        assert job["status"] == JobStatus.ERROR
        Job().remove(job)
    tale = Tale().load(tale["_id"], force=True)
    assert tale["status"] == TaleStatus.ERROR
    Tale().remove(tale)


@pytest.mark.xfail(reason="Needs to be ported to Celery")
@pytest.mark.plugin("wholetale")
def testTaleImportZipFail(server, user, image, fsAssetstore):
    image = Image().createImage(
        name="Jupyter Classic",
        creator=user,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="PythonBuildPack",
            user="someUser",
            port=8888,
            urlPath="",
        ),
    )
    with mock.patch("girder_wholetale.lib.pids_to_entities") as mock_pids:
        mock_pids.side_effect = ValueError
        with open(
            os.path.join(
                os.path.dirname(__file__), "data", "5c92fbd472a9910001fbff72.zip"
            ),
            "rb",
        ) as fp:
            resp = server.request(
                path="/tale/import",
                method="POST",
                user=user,
                type="application/zip",
                body=fp.read(),
            )

        assertStatusOk(resp)
        tale = resp.json

        job = Job().findOne({"type": "wholetale.import_tale"})
        assert json.loads(job["kwargs"])["taleId"]["$oid"] == tale["_id"]
        for i in range(300):
            if job["status"] in {JobStatus.SUCCESS, JobStatus.ERROR}:
                break
            time.sleep(0.1)
            job = Job().load(job["_id"], force=True)
        assert job["status"] == JobStatus.ERROR
        Job().remove(job)
    tale = Tale().load(tale["_id"], force=True)
    assert tale["status"] == TaleStatus.ERROR
    Tale().remove(tale)
    Image().remove(image)


@pytest.mark.xfail(reason="Needs to be ported to Celery")
@pytest.mark.plugin("wholetale")
def testCopyWorkspaceFail(server, admin, user, image):
    tale = Tale().createTale(
        image,
        [],
        creator=admin,
        title="tale one",
        public=True,
        config={"memLimit": "2g"},
    )
    new_tale = Tale().createTale(
        image,
        [],
        creator=admin,
        title="tale one (copy)",
        public=True,
        config={"memLimit": "2g"},
    )
    new_workspace = Folder().load(new_tale["workspaceId"], force=True)
    Folder().remove(new_workspace)  # oh no!
    job = Job().createLocalJob(
        title='Copy "{title}" workspace'.format(**tale),
        user=user,
        type="wholetale.copy_workspace",
        public=False,
        asynchronous=True,
        module="girder_wholetale.tasks.copy_workspace",
        args=(tale, new_tale),
    )
    Job().scheduleJob(job)
    for i in range(300):
        if job["status"] in {JobStatus.SUCCESS, JobStatus.ERROR}:
            break
        time.sleep(0.1)
        job = Job().load(job["_id"], force=True)
    assert job["status"] == JobStatus.ERROR
    Job().remove(job)
    new_tale = Tale().load(new_tale["_id"], force=True)
    assert new_tale["status"] == TaleStatus.ERROR
    Tale().remove(tale)
    Tale().remove(new_tale)
