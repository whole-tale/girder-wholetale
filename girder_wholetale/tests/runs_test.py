import json
import os
import time
from datetime import datetime, timedelta

import mock
import pytest
from girder.models.folder import Folder
from girder.models.token import Token
from girder_jobs.models.job import Job
from girder_jobs.constants import JobStatus

from pytest_girder.assertions import assertStatus, assertStatusOk
from girder_wholetale.constants import FIELD_STATUS_CODE, RunStatus
from girder_wholetale.models.run_hierarchy import RunHierarchyModel
from girder_wholetale.models.tale import Tale


class FakeAsyncResult(object):
    def __init__(self, tale_id=None):
        self.task_id = "fake_id"
        self.tale_id = tale_id

    def get(self, timeout=None):
        return {
            "image_digest": "registry.local.wholetale.org/tale/name:123",
            "repo2docker_version": 1,
            "last_build": 123,
        }


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
@pytest.mark.vcr
def test_basic_runs_ops(server, register_datasets, dataset, tale, image, user, mock_builder):
    workspace = Folder().load(tale["workspaceId"], force=True)

    file1_content = b"Hello World!"
    file1_name = "test_file.txt"

    with open(os.path.join(workspace["fsPath"], file1_name), "wb") as f:
        f.write(file1_content)

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
        params={"versionId": version["_id"], "name": "test run"},
    )
    assertStatusOk(resp)
    run = resp.json

    resp = server.request(path=f"/run/{run['_id']}", method="GET", user=user)
    assertStatusOk(resp)
    refreshed_run = resp.json
    for key in ("created", "updated"):
        run.pop(key)
        refreshed_run.pop(key)
    assert refreshed_run == run

    run = Folder().load(run["_id"], force=True)  # Need fsPath
    assert os.path.isfile(os.path.join(run["fsPath"], "workspace", file1_name))

    # Try to delete version with an existing run.
    # It should fail.
    resp = server.request(path=f"/version/{version['_id']}", method="DELETE", user=user)
    assertStatus(resp, 461)

    # Rename run
    resp = server.request(
        path=f"/run/{run['_id']}",
        method="PUT",
        params={"name": "a better name"},
        user=user,
    )
    assertStatusOk(resp)
    assert resp.json["name"] == "a better name"
    run = Folder().load(run["_id"], force=True)
    assert run["name"] == resp.json["name"]

    resp = server.request(
        path="/run/exists",
        method="GET",
        params={"name": "test run", "taleId": tale["_id"]},
        user=user,
    )
    assertStatusOk(resp)
    assert not resp.json["exists"]

    resp = server.request(
        path="/run/exists",
        method="GET",
        params={"name": "a better name", "taleId": tale["_id"]},
        user=user,
    )
    assertStatusOk(resp)
    assert resp.json["exists"]
    assert resp.json["obj"]["_id"] == str(run["_id"])

    # Get current status, should be UNKNOWN
    resp = server.request(path=f"/run/{run['_id']}/status", method="GET", user=user)
    assertStatusOk(resp)
    assert resp.json == dict(status=0, statusString="UNKNOWN")

    # Set status to RUNNING
    resp = server.request(
        path=f"/run/{run['_id']}/status",
        method="PATCH",
        user=user,
        params={"status": 2},
    )
    assertStatusOk(resp)

    # Get current status, should be RUNNING
    resp = server.request(path=f"/run/{run['_id']}/status", method="GET", user=user)
    assertStatusOk(resp)
    assert resp.json == dict(status=2, statusString="RUNNING")

    # Create a 2nd tale to verify GET /run is doing the right thing...
    tale2 = Tale().createTale(
        image,
        dataset(user, 0),
        creator=user,
        save=True,
        title="Some tale with dataset and versions",
        description="Something something...",
        public=False,
        icon="https://icon-picture.com/icon.png",
        config={},
        authors=[
            {
                "firstName": "Kacper",
                "lastName": "Kowalik",
                "orcid": "https://orcid.org/0000-0003-1709-3744",
            }
        ],
        category="science",
    )
    assert tale["_id"] != tale2["_id"]

    resp = server.request(
        path="/run",
        method="GET",
        user=user,
        params={"taleId": tale2["_id"]},
    )
    assertStatusOk(resp)
    assert resp.json == []  # This tale doesn't have runs

    resp = server.request(
        path="/run",
        method="GET",
        user=user,
        params={"taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    assert len(resp.json) == 1
    assert resp.json[0]["_id"] == str(run["_id"])

    resp = server.request(path=f"/run/{run['_id']}", method="DELETE", user=user)
    assert not os.path.exists(os.path.join(run["fsPath"], "workspace", file1_name))
    assertStatusOk(resp)

    resp = server.request(path=f"/version/{version['_id']}", method="DELETE", user=user)
    assertStatusOk(resp)

    Tale().remove(tale2)


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr
def test_recorded_run(server, register_datasets, tale, user, mock_builder):
    mock.patch("gwvolman.tasks.recorded_run").start()
    workspace = Folder().load(tale["workspaceId"], force=True)

    file1_content = b"#!/bin/bash\nmkdir output\ndate > output/date.txt"
    file1_name = "entrypoint.sh"

    with open(os.path.join(workspace["fsPath"], file1_name), "wb") as f:
        f.write(file1_content)

    resp = server.request(
        path="/version",
        method="POST",
        user=user,
        params={"name": "v1", "taleId": tale["_id"]},
    )
    assertStatusOk(resp)
    version = resp.json

    resp = server.request(
        path="/run",
        method="POST",
        user=user,
        params={"versionId": version["_id"], "name": "r1"},
    )
    assertStatusOk(resp)
    run = resp.json

    with mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        mock_apply_async().job.return_value = json.dumps({"job": 1, "blah": 2})

        # Test default entrypoint
        resp = server.request(
            path="/run/%s/start" % run["_id"], method="POST", user=user
        )
        job_call = mock_apply_async.call_args_list[-1][-1]
        assert job_call["args"] == (str(run["_id"]), (str(tale["_id"])), "run.sh")
        assert job_call["headers"]["girder_job_title"] == "Recorded Run"
        assertStatusOk(resp)

    # Test default entrypoint
    with mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        mock_apply_async().job.return_value = json.dumps({"job": 1, "blah": 2})

        resp = server.request(
            path="/run/%s/start" % run["_id"],
            method="POST",
            user=user,
            params={"entrypoint": "entrypoint.sh"},
        )
        job_call = mock_apply_async.call_args_list[-1][-1]
        assert job_call["args"] == (str(run["_id"]), (str(tale["_id"])), "entrypoint.sh")
        assert job_call["headers"]["girder_job_title"] == "Recorded Run"
        assertStatusOk(resp)

    token = Token().createToken(user=user, days=60)
    job = Job().createJob(
        title="Recorded Run",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[str(run["_id"]), str(tale["_id"]), "entrypoint.sh"],
        kwargs={},
        otherFields={"token": token["_id"]},
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE

    token_id = job["jobInfoSpec"]["headers"]["Girder-Token"]
    token = Token().load(token_id, force=True, objectId=False)
    assert token["expires"] > datetime.utcnow() + timedelta(days=59)

    with mock.patch("celery.Celery") as celeryMock:
        celeryMock().send_task.return_value = FakeAsyncResult(tale["_id"])
        celeryMock().AsyncResult.return_value = FakeAsyncResult(tale["_id"])
        Job().scheduleJob(job)

        for _ in range(20):
            job = Job().load(job["_id"], force=True)
            if job["status"] == JobStatus.QUEUED:
                break
            time.sleep(0.1)
        assert job["status"] == JobStatus.QUEUED
        rfolder = Folder().load(job["args"][0], force=True)
        assert rfolder[FIELD_STATUS_CODE] == RunStatus.RUNNING.code

        # Set status to RUNNING
        job = Job().load(job["_id"], force=True)
        Job().updateJob(job, log="job running", status=JobStatus.RUNNING)
        rfolder = Folder().load(job["args"][0], force=True)
        assert rfolder[FIELD_STATUS_CODE] == RunStatus.RUNNING.code

        # Set status to SUCCESS
        job = Job().load(job["_id"], force=True)
        Job().updateJob(job, log="job successful", status=JobStatus.SUCCESS)
        rfolder = Folder().load(job["args"][0], force=True)
        assert rfolder[FIELD_STATUS_CODE] == RunStatus.COMPLETED.code

    token = Token().load(token_id, force=True, objectId=False)
    assert token["expires"] < datetime.utcnow() + timedelta(hours=2)


@pytest.mark.plugin("wholetale")
def test_run_heartbeat_no_active_queues(user):
    active_runs = [
        {
            "_id": "run_id",
            "creatorId": user["_id"],
            "meta": {
                "container_name": "my_container",
                "node_id": "my_node",
                "jobId": "jobId",
            },
            FIELD_STATUS_CODE: RunStatus.RUNNING.code,
        }
    ]
    with mock.patch.object(
        RunHierarchyModel(), "setStatus"
    ) as mock_setStatus, mock.patch(
        "girder_wholetale.models.run_hierarchy.getCeleryApp"
    ) as mock_celery, mock.patch.object(Folder(), "find", return_value=active_runs):
        mock_inspect = mock.MagicMock()
        mock_inspect.active_queues.return_value = None
        mock_celery.return_value.control.inspect.return_value = mock_inspect

        RunHierarchyModel().run_heartbeat(None)

        mock_setStatus.assert_called_once_with(active_runs[0], RunStatus.UNKNOWN)


@pytest.mark.plugin("wholetale")
def test_run_heartbeat_unknown_run(user):
    active_runs = [
        {
            "_id": "run_id",
            "creatorId": user["_id"],
            "meta": {
                "container_name": "my_container",
                "node_id": "my_node",
                "jobId": "jobId",
            },
            FIELD_STATUS_CODE: RunStatus.UNKNOWN.code,
        }
    ]
    with mock.patch.object(
        RunHierarchyModel(), "setStatus"
    ) as mock_setStatus, mock.patch(
        "girder_wholetale.models.run_hierarchy.check_on_run"
    ) as mock_check_on_run, mock.patch(
        "girder_wholetale.models.run_hierarchy.cleanup_run"
    ) as mock_cleanup_run, mock.patch.object(
        Job(), "load", return_value={"celeryTaskId": "my_task_id"}
    ) as mock_job_load, mock.patch.object(
        Folder(), "find", return_value=active_runs
    ), mock.patch(
        "girder_wholetale.models.run_hierarchy.getCeleryApp"
    ) as mock_celery:
        mock_inspect = mock.MagicMock()
        mock_inspect.active_queues.return_value = {"celery@my_node": {}}
        mock_inspect.active.return_value = {"celery@my_node": []}
        mock_celery.return_value.control.inspect.return_value = mock_inspect

        RunHierarchyModel().run_heartbeat(None)

        mock_setStatus.assert_not_called()
        mock_job_load.assert_called_once_with(
            active_runs[0]["meta"]["jobId"], force=True
        )
        mock_check_on_run.assert_not_called()
        mock_cleanup_run.signature.assert_called_once_with(
            args=[str(active_runs[0]["_id"])],
            girder_client_token=mock.ANY,
            queue="my_node",
        )
        mock_cleanup_run.signature().apply_async.assert_called_once()
    mock_celery.close()


@pytest.mark.plugin("wholetale")
def _test_run_heartbeat_container_dead(user):
    active_runs = [
        {
            "_id": "run_id",
            "creatorId": user["_id"],
            "meta": {
                "container_name": "my_container",
                "node_id": "my_node",
                "jobId": "jobId",
            },
            FIELD_STATUS_CODE: RunStatus.RUNNING.code,
        }
    ]
    with mock.patch.object(
        RunHierarchyModel(), "setStatus"
    ) as mock_setStatus, mock.patch(
        "girder_wholetale.models.run_hierarchy.check_on_run",
    ) as mock_check_on_run, mock.patch(
        "girder_wholetale.models.run_hierarchy.cleanup_run"
    ) as mock_cleanup_run, mock.patch.object(
        Job(), "load", return_value={"celeryTaskId": "my_task_id"}
    ), mock.patch.object(Folder(), "find", return_value=active_runs), mock.patch(
        "girder_wholetale.models.run_hierarchy.getCeleryApp"
    ) as mock_celery:
        mock_inspect = mock.MagicMock()
        mock_inspect.active_queues.return_value = {"celery@my_node": {}}
        mock_inspect.active.return_value = {"celery@my_node": [{"id": "my_task_id"}]}
        mock_celery.return_value.control.inspect.return_value = mock_inspect
        mock_check_on_run.signature.return_value.apply_async.return_value.get.return_value = False

        RunHierarchyModel().run_heartbeat(None)

        mock_setStatus.assert_not_called()
        mock_check_on_run.assert_has_calls(
            [
                mock.call.signature(
                    args=[active_runs[0]["meta"]],
                    queue=active_runs[0]["meta"]["node_id"],
                ),
                mock.call.signature().apply_async(),
                mock.call.signature().apply_async().get(timeout=60),
            ]
        )
        mock_cleanup_run.assert_has_calls(
            [
                mock.call.signature(
                    args=[str(active_runs[0]["_id"])],
                    girder_client_token=mock.ANY,
                    queue="my_node",
                ),
                mock.call.signature().apply_async(),
            ]
        )
