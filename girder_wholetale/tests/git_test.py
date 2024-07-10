import os
import shutil
import tempfile
import time
from datetime import datetime

import git
import mock
import pymongo
import pytest
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from pytest_girder.assertions import assertStatusOk

from girder_wholetale.constants import InstanceStatus, TaleStatus
from girder_wholetale.models.tale import Tale

from .conftest import get_events

GIT_FILE_NAME = "hello.txt"
GIT_FILE_ON_BRANCH = "on_branch.txt"


@pytest.fixture
def git_repo_dir():
    git_repo_dir = tempfile.mkdtemp()
    r = git.Repo.init(git_repo_dir)
    with open(os.path.join(git_repo_dir, GIT_FILE_NAME), "w") as fp:
        fp.write("World!")
    r.index.add([GIT_FILE_NAME])
    r.index.commit("initial commit")
    feature = r.create_head("feature")
    r.head.reference = feature
    with open(os.path.join(git_repo_dir, GIT_FILE_ON_BRANCH), "w") as fp:
        fp.write("MAGIC!")
    r.index.add([GIT_FILE_ON_BRANCH])
    r.index.commit("Commit on a branch")
    r.head.reference = r.refs["main"]
    r.head.reset(index=True, working_tree=True)
    yield git_repo_dir
    shutil.rmtree(git_repo_dir, ignore_errors=True)


def _import_git_repo(server, user, tale, url):
    resp = server.request(
        path=f"/tale/{tale['_id']}/git",
        method="PUT",
        user=user,
        params={"url": url},
    )
    assertStatusOk(resp)

    job = (
        Job()
        .find({"type": "wholetale.import_git_repo"})
        .sort([("created", pymongo.DESCENDING)])
        .limit(1)
        .next()
    )

    for i in range(10):
        time.sleep(0.5)
        job = Job().load(job["_id"], force=True, includeLog=True)
        if job["status"] >= JobStatus.SUCCESS:
            break

    return job


def _import_from_git_repo(url, server, image, user):
    resp = server.request(
        path="/tale/import",
        method="POST",
        user=user,
        params={
            "url": url,
            "git": True,
            "imageId": str(image["_id"]),
            "spawn": True,
        },
    )
    assertStatusOk(resp)
    tale = resp.json

    job = (
        Job()
        .find({"type": "wholetale.import_git_repo"})
        .sort([("created", pymongo.DESCENDING)])
        .limit(1)
        .next()
    )

    for i in range(60):
        time.sleep(0.5)
        job = Job().load(job["_id"], force=True, includeLog=True)
        if job["status"] >= JobStatus.SUCCESS:
            break
    tale = Tale().load(tale["_id"], user=user)
    return tale, job

@pytest.mark.xfail(reason="Local task needs to be ported to celery.")
@pytest.mark.plugin("wholetale")
def test_import_git_as_tale(server, user, image, git_repo_dir):
    class fakeInstance(object):
        _id = "123456789"

        def createInstance(self, tale, user, /, *, spawn=False):
            return {"_id": self._id, "status": InstanceStatus.LAUNCHING}

        def load(self, instance_id, user=None):
            assert instance_id == self._id
            return {"_id": self._id, "status": InstanceStatus.RUNNING}

    with mock.patch("girder_wholetale.tasks.import_git_repo.Instance", fakeInstance):
        since = datetime.utcnow().isoformat()
        # Custom branch
        tale, job = _import_from_git_repo(
            f"file://{git_repo_dir}@feature", server, image, user
        )
        workspace = Folder().load(tale["workspaceId"], force=True)
        workspace_path = workspace["fsPath"]
        assert job["status"] == JobStatus.SUCCESS
        assert os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_NAME))
        assert os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_ON_BRANCH))
        # Confirm events
        events = get_events(server, since)
        assert len(events) == 3
        assert events[0]["data"]["event"] == "wt_tale_created"
        assert events[1]["data"]["event"] == "wt_import_started"
        assert events[2]["data"]["event"] == "wt_import_completed"
        shutil.rmtree(workspace_path)
        os.mkdir(workspace_path)
        Tale().remove(tale)

    # Invalid url
    since = datetime.utcnow().isoformat()
    tale, job = _import_from_git_repo("blah", server, image, user)
    workspace = Folder().load(tale["workspaceId"], force=True)
    workspace_path = workspace["fsPath"]
    assert job["status"] == JobStatus.ERROR
    assert "does not appear to be a git repo" in job["log"][0]
    assert tale["status"] == TaleStatus.ERROR
    # Confirm events
    events = get_events(server, since)
    assert len(events) == 3
    assert events[0]["data"]["event"] == "wt_tale_created"
    assert events[1]["data"]["event"] == "wt_import_started"
    assert events[2]["data"]["event"] == "wt_import_failed"
    Tale().remove(tale)


@pytest.mark.xfail(reason="Local task needs to be ported to celery.")
@pytest.mark.plugin("wholetale")
def test_git_import(server, user, image, git_repo_dir):
    tale = Tale().createTale(image, [], creator=user, public=True)
    workspace = Folder().load(tale["workspaceId"], force=True)
    workspace_path = workspace["fsPath"]

    # Invalid path
    since = datetime.utcnow().isoformat()
    job = _import_git_repo(server, user, tale, "blah")
    assert job["status"] == JobStatus.ERROR
    assert "does not appear to be a git repo" in job["log"][0]
    if os.path.isdir(os.path.join(workspace_path, ".git")):
        shutil.rmtree(os.path.join(workspace_path, ".git"))
    # Confirm events
    events = get_events(server, since)
    assert len(events) == 2
    assert events[0]["data"]["event"] == "wt_import_started"
    assert events[1]["data"]["event"] == "wt_import_failed"

    # Default branch (master)
    since = datetime.utcnow().isoformat()
    job = _import_git_repo(server, user, tale, f"file://{git_repo_dir}")
    assert job["status"] == JobStatus.SUCCESS
    assert os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_NAME))
    assert not os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_ON_BRANCH))
    # Confirm events
    events = get_events(server, since)
    assert len(events) == 2
    assert events[0]["data"]["event"] == "wt_import_started"
    assert events[1]["data"]["event"] == "wt_import_completed"
    shutil.rmtree(workspace_path)
    os.mkdir(workspace_path)

    # Custom branch
    job = _import_git_repo(server, user, tale, f"file://{git_repo_dir}@feature")
    assert job["status"] == JobStatus.SUCCESS
    assert os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_NAME))
    assert os.path.isfile(os.path.join(workspace["fsPath"], GIT_FILE_ON_BRANCH))
    shutil.rmtree(workspace_path)
    os.mkdir(workspace_path)
    Tale().remove(tale)
