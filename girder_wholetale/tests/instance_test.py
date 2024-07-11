import json
import time
import urllib.parse
from datetime import datetime

import httmock
import mock
import pytest
from bson import ObjectId
from girder.exceptions import ValidationException
from girder.models.folder import Folder
from girder.models.notification import Notification
from girder.models.setting import Setting
from girder.utility import config
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from girder_worker.girder_plugin.status import CustomJobStatus
from pytest_girder.assertions import assertStatus, assertStatusOk
from pytest_girder.utils import getResponseBody

from girder_wholetale.constants import (
    ImageStatus,
    InstanceStatus,
    PluginSettings,
    SettingDefault,
)
from girder_wholetale.models.instance import Instance
from girder_wholetale.models.tale import Tale
from girder_wholetale.rest.instance import instanceCapErrMsg
from girder_wholetale.utils import init_progress

from .conftest import get_events, mockOtherRequests


@pytest.fixture
def enabledHeartbeat():
    cfg = config.getConfig()
    cfg["server"]["heartbeat"] = 10
    return cfg
    cfg["server"].pop("heartbeat")


@pytest.fixture
def instanceCap():
    Setting().set(PluginSettings.INSTANCE_CAP, "2")
    yield
    Setting().set(PluginSettings.INSTANCE_CAP, None)


@pytest.fixture
def private_folder(user):
    folder = Folder().createFolder(
        user, "PrivateFolder", parentType="user", public=False, creator=user
    )
    yield folder
    Folder().remove(folder)


@pytest.fixture
def public_folder(user):
    folder = Folder().createFolder(
        user, "PublicFolder", parentType="user", public=True, creator=user
    )
    yield folder
    Folder().remove(folder)


class FakeAsyncResult(object):
    def __init__(self, instanceId=None):
        self.task_id = "fake_id"
        self.instanceId = instanceId

    def get(self, timeout=None):
        return dict(
            digest="sha256:7a789bc20359dce987653",
            imageId="5678901234567890",
            nodeId="123456",
            name="tmp-xxx",
            mountPoint="/foo/bar",
            volumeName="blah_volume",
            sessionId="5ecece693fec11b4854a874d",
            instanceId=self.instanceId,
        )


class FakeAsyncResultForUpdate(object):
    def __init__(self, instanceId=None):
        self.task_id = "fake_update_id"
        self.instanceId = instanceId
        self.digest = "sha256:7a789bc20359dce987653"

    def get(self, timeout=None):
        return dict(digest=self.digest)


@pytest.fixture
def tale_one(user, image):
    data = []
    tale = Tale().createTale(
        image,
        data,
        creator=user,
        title="tale one",
        public=True,
        config={"memLimit": "2g"},
    )

    fake_imageInfo = {
        "digest": (
            "registry.local.wholetale.org/5c8fe826da39aa00013e9609/1552934951@"
            "sha256:4f604e6fab47f79e28251657347ca20ee89b737b4b1048c18ea5cf2fe9a9f098"
        ),
        "jobId": ObjectId("5c9009deda39aa0001d702b7"),
        "last_build": 1552943449,
        "repo2docker_version": "craigwillis/repo2docker:latest",
        "status": 3,
    }
    tale["imageInfo"] = fake_imageInfo
    tale = Tale().save(tale)
    yield tale
    Tale().remove(tale)


@pytest.fixture
def notification(user):
    notification = init_progress({}, user, "Fake", ".", 5)
    yield notification
    Notification().remove(notification)


@pytest.mark.plugin("wholetale")
def test_instance_cap(server, user, admin, tale_one):
    with pytest.raises(
        ValidationException, match="Instance Cap needs to be an integer.$"
    ):
        Setting().set(PluginSettings.INSTANCE_CAP, "a")

    setting = Setting()

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.INSTANCE_CAP, "value": ""},
    )
    assertStatusOk(resp)
    resp = server.request(
        "/system/setting",
        user=admin,
        method="GET",
        params={"key": PluginSettings.INSTANCE_CAP},
    )
    assertStatusOk(resp)
    assert resp.body[0].decode() == str(
        SettingDefault.defaults[PluginSettings.INSTANCE_CAP]
    )

    with mock.patch("celery.Celery") as celeryMock:
        instance = celeryMock.return_value
        instance.send_task.return_value = FakeAsyncResult()
        current_cap = setting.get(PluginSettings.INSTANCE_CAP)
        setting.set(PluginSettings.INSTANCE_CAP, "0")
        resp = server.request(
            path="/instance",
            method="POST",
            user=user,
            params={"taleId": str(tale_one["_id"])},
        )
        assertStatus(resp, 400)
        assert resp.json["message"] == instanceCapErrMsg.format("0")
        setting.set(PluginSettings.INSTANCE_CAP, current_cap)


@pytest.mark.plugin("wholetale")
def test_instance_flow(server, user, image, tale_one, mocker):
    mocker.patch("gwvolman.tasks.create_volume")
    mocker.patch("gwvolman.tasks.launch_container")
    mocker.patch("gwvolman.tasks.update_container")
    mocker.patch("gwvolman.tasks.shutdown_container")
    mocker.patch("gwvolman.tasks.remove_volume")
    since = datetime.utcnow().isoformat()
    with mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        resp = server.request(
            path="/instance",
            method="POST",
            user=user,
            params={"taleId": str(tale_one["_id"]), "name": "tale one"},
        )
        mock_apply_async.assert_called_once()

    assertStatusOk(resp)
    instance = resp.json

    # Create a job to be handled by the worker plugin
    job = Job().createJob(
        title="Spawn Instance",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[{"instanceId": instance["_id"]}],
        kwargs={},
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE

    # Schedule the job, make sure it is sent to celery
    with mock.patch("celery.Celery") as celeryMock:
        celeryMock().AsyncResult.return_value = FakeAsyncResult(instance["_id"])
        celeryMock().send_task.return_value = FakeAsyncResult(instance["_id"])

        Job().scheduleJob(job)
        for _ in range(20):
            job = Job().load(job["_id"], force=True)
            if job["status"] == JobStatus.QUEUED:
                break
            time.sleep(0.1)
        assert job["status"] == JobStatus.QUEUED
        events = get_events(server, since, user=user)
        assert len(events) == 1
        assert events[0]["data"]["event"] == "wt_instance_launching"

        instance = Instance().load(instance["_id"], force=True)
        assert instance["status"] == InstanceStatus.LAUNCHING

        # Make sure we sent the job to celery
        sendTaskCalls = celeryMock.return_value.send_task.mock_calls

        assert len(sendTaskCalls) == 1
        assert sendTaskCalls[0][1] == ("girder_worker.run", job["args"], job["kwargs"])

        assert "headers" in sendTaskCalls[0][2]
        assert "jobInfoSpec" in sendTaskCalls[0][2]["headers"]

        # Make sure we got and saved the celery task id
        job = Job().load(job["_id"], force=True)
        assert job["celeryTaskId"] == "fake_id"

        Job().updateJob(job, log="job running", status=JobStatus.RUNNING)
        since = datetime.utcnow().isoformat()
        Job().updateJob(job, log="job ran", status=JobStatus.SUCCESS)

        resp = server.request(
            path="/job/{_id}/result".format(**job), method="GET", user=user
        )
        assertStatusOk(resp)
        assert resp.json["nodeId"] == "123456"

        # Confirm event
        events = get_events(server, since, user=user)
        assert len(events) == 1
        assert events[0]["data"]["event"] == "wt_instance_running"

    # Check if set up properly
    resp = server.request(
        path="/instance/{_id}".format(**instance), method="GET", user=user
    )
    assert resp.json["containerInfo"]["imageId"] == str(image["_id"])
    assert resp.json["containerInfo"]["digest"] == tale_one["imageInfo"]["digest"]
    assert resp.json["containerInfo"]["nodeId"] == "123456"
    assert resp.json["containerInfo"]["volumeName"] == "blah_volume"
    assert resp.json["status"] == InstanceStatus.RUNNING

    # Save this response to populate containerInfo
    instance = resp.json

    # Check that the instance is a singleton
    resp = server.request(
        path="/instance",
        method="POST",
        user=user,
        params={"taleId": str(tale_one["_id"]), "name": "tale one"},
    )
    assertStatusOk(resp)
    assert resp.json["_id"] == str(instance["_id"])

    # Instance authorization checks
    # Missing forward auth headers
    resp = server.request(
        path="/user/authorize",
        params={"instance": True},
        method="GET",
        isJson=False,
        user=user,
    )
    # Assert 400 "Forward auth request required"
    assertStatus(resp, 400)

    # Valid user, invalid host
    resp = server.request(
        user=user,
        path="/user/authorize",
        params={"instance": True},
        method="GET",
        additionalHeaders=[
            ("X-Forwarded-Host", "blah.wholetale.org"),
            ("X-Forwarded_Uri", "/"),
        ],
        isJson=False,
    )
    # 403 "Access denied for instance"
    assertStatus(resp, 403)

    # Valid user, valid host
    resp = server.request(
        user=user,
        path="/user/authorize",
        params={"instance": True},
        method="GET",
        additionalHeaders=[
            ("X-Forwarded-Host", "tmp-xxx.wholetale.org"),
            ("X-Forwarded_Uri", "/"),
        ],
        isJson=False,
    )
    assertStatus(resp, 200)

    # No user
    resp = server.request(
        path="/user/authorize",
        params={"instance": True},
        method="GET",
        additionalHeaders=[
            ("X-Forwarded-Host", "tmp-xxx.wholetale.org"),
            ("X-Forwarded-Uri", "/"),
        ],
        isJson=False,
    )
    assertStatus(resp, 303)
    # Confirm redirect to https://girder.{domain}/api/v1/user/sign_in
    assert resp.headers["Location"] == (
        "https://girder.wholetale.org/api/v1/"
        "user/sign_in?redirect=https://tmp-xxx.wholetale.org/"
    )

    # Update/restart the instance
    job = Job().createJob(
        title="Update Instance",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[instance["_id"]],
        kwargs={},
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE
    with mock.patch("celery.Celery") as celeryMock, mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        celeryMock.send_task.return_value = FakeAsyncResultForUpdate(instance["_id"])
        mocker.patch(
            "girder_worker.girder_plugin.event_handlers.getCeleryApp",
            return_value=celeryMock,
        )
        resp = server.request(
            path="/instance/{_id}".format(**instance),
            method="PUT",
            user=user,
            body=json.dumps(
                {
                    # ObjectId is not serializable
                    "_id": str(instance["_id"]),
                    "iframe": instance["iframe"],
                    "name": instance["name"],
                    "status": instance["status"],
                    "taleId": instance["status"],
                    "sessionId": instance["status"],
                    "url": instance["url"],
                    "containerInfo": {
                        "digest": instance["containerInfo"]["digest"],
                        "imageId": instance["containerInfo"]["imageId"],
                        "mountPoint": instance["containerInfo"]["mountPoint"],
                        "name": instance["containerInfo"]["name"],
                        "nodeId": instance["containerInfo"]["nodeId"],
                    },
                }
            ),
        )
        assertStatusOk(resp)
        mock_apply_async.assert_called_once()

        Job().scheduleJob(job)
        for _ in range(20):
            job = Job().load(job["_id"], force=True)
            if job["status"] == JobStatus.QUEUED:
                break
            time.sleep(0.1)
        assert job["status"] == JobStatus.QUEUED

        instance = Instance().load(instance["_id"], force=True)
        assert instance["status"] == InstanceStatus.LAUNCHING

        # Make sure we sent the job to celery
        sendTaskCalls = celeryMock.send_task.mock_calls

        assert len(sendTaskCalls) == 1
        assert sendTaskCalls[0][1] == ("girder_worker.run", job["args"], job["kwargs"])

        assert "headers" in sendTaskCalls[0][2]
        assert "jobInfoSpec" in sendTaskCalls[0][2]["headers"]

        # Make sure we got and saved the celery task id
        job = Job().load(job["_id"], force=True)
        assert job["celeryTaskId"] == "fake_update_id"
        Job().updateJob(job, log="job running", status=JobStatus.RUNNING)

        Job().updateJob(job, log="job running", status=JobStatus.RUNNING)
        Job().updateJob(job, log="job ran", status=JobStatus.SUCCESS)

    resp = server.request(
        path="/instance/{_id}".format(**instance), method="GET", user=user
    )
    assertStatusOk(resp)
    assert resp.json["containerInfo"]["digest"] == "sha256:7a789bc20359dce987653"
    instance = resp.json

    # Update/restart the instance and fail
    job = Job().createJob(
        title="Update Instance",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[instance["_id"]],
        kwargs={},
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE
    with mock.patch("celery.Celery") as celeryMock, mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        celeryMock.send_task.return_value = FakeAsyncResultForUpdate(instance["_id"])
        mocker.patch(
            "girder_worker.girder_plugin.event_handlers.getCeleryApp",
            return_value=celeryMock,
        )
        # PUT /instance/:id (currently a no-op)
        resp = server.request(
            path="/instance/{_id}".format(**instance),
            method="PUT",
            user=user,
            body=json.dumps(
                {
                    # ObjectId is not serializable
                    "_id": str(instance["_id"]),
                    "iframe": instance["iframe"],
                    "name": instance["name"],
                    "status": instance["status"],
                    "taleId": instance["status"],
                    "sessionId": instance["status"],
                    "url": instance["url"],
                    "containerInfo": {
                        "digest": instance["containerInfo"]["digest"],
                        "imageId": instance["containerInfo"]["imageId"],
                        "mountPoint": instance["containerInfo"]["mountPoint"],
                        "name": instance["containerInfo"]["name"],
                        "nodeId": instance["containerInfo"]["nodeId"],
                    },
                }
            ),
        )
        assertStatusOk(resp)
        mock_apply_async.assert_called_once()

        Job().scheduleJob(job)
        for _ in range(20):
            job = Job().load(job["_id"], force=True)
            if job["status"] == JobStatus.QUEUED:
                break
            time.sleep(0.1)
        assert job["status"] == JobStatus.QUEUED

        instance = Instance().load(instance["_id"], force=True)
        assert instance["status"] == InstanceStatus.LAUNCHING

        Job().updateJob(job, log="job failed", status=JobStatus.ERROR)
        instance = Instance().load(instance["_id"], force=True)
        assert instance["status"] == InstanceStatus.ERROR

    # Delete the instance
    since = datetime.utcnow().isoformat()
    with mock.patch(
        "girder_worker.task.celery.Task.apply_async", spec=True
    ) as mock_apply_async:
        resp = server.request(
            path="/instance/{_id}".format(**instance),
            method="DELETE",
            user=user,
        )
        assertStatusOk(resp)
        assert mock_apply_async.call_count == 2

    resp = server.request(
        path="/instance/{_id}".format(**instance), method="GET", user=user
    )
    assertStatus(resp, 400)

    # Confirm notifications
    events = get_events(server, since, user=user)
    assert len(events) == 2
    assert events[0]["data"]["event"] == "wt_instance_deleting"
    assert events[1]["data"]["event"] == "wt_instance_deleted"


@pytest.mark.plugin("wholetale")
def test_build_fail(server, user, tale_one, notification):
    resp = server.request(
        path="/instance",
        method="POST",
        user=user,
        params={
            "taleId": str(tale_one["_id"]),
            "name": "tale that will fail",
            "spawn": False,
        },
    )
    assertStatusOk(resp)
    instance = resp.json
    assert instance["status"] != InstanceStatus.ERROR

    job = Job().createJob(
        title="Build Tale Image",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[str(tale_one["_id"]), False],
        kwargs={},
        otherFields={
            "wt_notification_id": str(notification["_id"]),
            "instance_id": instance["_id"],
        },
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE
    Job().updateJob(job, log="job queued", status=JobStatus.QUEUED)
    Job().updateJob(job, log="job running", status=JobStatus.RUNNING)
    with mock.patch(
        "girder_wholetale.lib.events.Instance.deleteInstance"
    ) as mock_delete:
        Job().updateJob(job, log="job failed", status=JobStatus.ERROR)
    mock_delete.assert_called_once()
    Instance().remove(instance)


@pytest.mark.plugin("wholetale")
def test_build_cancel(server, user, tale_one, notification):
    resp = server.request(
        path="/instance",
        method="POST",
        user=user,
        params={
            "taleId": str(tale_one["_id"]),
            "name": "tale that will fail",
            "spawn": False,
        },
    )
    assertStatusOk(resp)
    instance = resp.json
    assert instance["status"] != InstanceStatus.ERROR

    job = Job().createJob(
        title="Build Tale Image",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[str(tale_one["_id"]), False],
        kwargs={},
        otherFields={
            "wt_notification_id": str(notification["_id"]),
            "instance_id": instance["_id"],
        },
    )
    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE
    Job().updateJob(job, log="job queued", status=JobStatus.QUEUED)
    Job().updateJob(job, log="job running", status=JobStatus.RUNNING)

    with mock.patch(
        "girder_wholetale.lib.events.Instance.deleteInstance"
    ) as mock_delete:
        Job().updateJob(job, log="job canceling", status=CustomJobStatus.CANCELING)
        tale = Tale().load(tale_one["_id"], force=True)
        while tale["imageInfo"]["status"] != ImageStatus.UNAVAILABLE:
            time.sleep(1)
            tale = Tale().load(tale_one["_id"], force=True)
    mock_delete.assert_called_once()

    with mock.patch(
        "girder_wholetale.lib.events.Instance.deleteInstance"
    ) as mock_delete:
        Job().updateJob(job, log="job canceling", status=JobStatus.CANCELED)

    instance = Instance().load(instance["_id"], force=True)
    Instance().remove(instance)


@pytest.mark.plugin("wholetale")
def test_launch_fail(server, user, tale_one, notification):
    resp = server.request(
        path="/instance",
        method="POST",
        user=user,
        params={
            "taleId": str(tale_one["_id"]),
            "name": "tale that will fail",
            "spawn": False,
        },
    )
    assertStatusOk(resp)
    instance = resp.json

    job = Job().createJob(
        title="Spawn Instance",
        type="celery",
        handler="worker_handler",
        user=user,
        public=False,
        args=[{"instanceId": instance["_id"]}],
        kwargs={},
        otherFields={
            "wt_notification_id": str(notification["_id"]),
            "instance_id": instance["_id"],
        },
    )

    job = Job().save(job)
    assert job["status"] == JobStatus.INACTIVE
    Job().updateJob(job, log="job queued", status=JobStatus.QUEUED)
    Job().updateJob(job, log="job running", status=JobStatus.RUNNING)
    since = datetime.utcnow().isoformat()
    Job().updateJob(job, log="job failed", status=JobStatus.ERROR)
    instance = Instance().load(instance["_id"], force=True)
    assert instance["status"] == InstanceStatus.ERROR
    events = get_events(server, since, user=user)
    assert len(events) == 1
    assert events[0]["data"]["event"] == "wt_instance_error"


@pytest.mark.plugin("wholetale")
@pytest.mark.xfail(reason="Heartbeat needs to ported to celery")
def test_idle_instance(server, user, tale_one, image, enabledHeartbeat):
    instance = Instance().createInstance(
        tale_one, user, name="idle instance", spawn=False
    )

    instance["containerInfo"] = {"imageId": image["_id"]}
    Instance().updateInstance(instance)

    cfg = config.getConfig()
    assert cfg["server"]["heartbeat"] == 10

    # Wait for idle instance to be culled
    with mock.patch(
        "girder_wholetale.lib.events.Instance.deleteInstance"
    ) as mock_delete:
        time.sleep(0.1)
    mock_delete.assert_called_once()

    Instance().remove(instance)


@pytest.mark.plugin("wholetale")
def test_instance_logs(server, user, tale_one, image):
    instance = Instance().createInstance(tale_one, user, name="instance", spawn=False)
    instance["containerInfo"] = {"imageId": image["_id"]}
    Instance().updateInstance(instance)

    @httmock.urlmatch(
        scheme="http",
        netloc="logger:8000",
        path="^/$",
    )
    def logger_call(url, request):
        params = urllib.parse.parse_qs(url.query)
        if "name" not in params:
            return httmock.response(
                status_code=400, content={"detail": "Missing 'name' parameter"}
            )
        name = params["name"][0]
        assert name == "some_service"
        return httmock.response(
            status_code=200,
            content="blah",
            headers={"content-type": "text/plain; charset=utf-8"},
        )

    with httmock.HTTMock(logger_call, mockOtherRequests):
        resp = server.request(
            user=user,
            path=f"/instance/{instance['_id']}/log",
            method="GET",
            isJson=False,
        )
        assert (
            getResponseBody(resp)
            == f"Logs for instance {instance['_id']} are currently unavailable..."
        )
        instance["containerInfo"]["name"] = "some_service"
        Instance().updateInstance(instance)

        resp = server.request(
            user=user,
            path=f"/instance/{instance['_id']}/log",
            method="GET",
            isJson=False,
        )
        assert getResponseBody(resp) == "blah"

    Instance().remove(instance)

@pytest.mark.plugin("wholetale")
def test_logger_setting(db):
    with pytest.raises(ValidationException) as exc:
        Setting().set(PluginSettings.LOGGER_URL, "a")
    assert str(exc.value) == "Invalid Instance Logger URL"

    assert (
        Setting().get(PluginSettings.LOGGER_URL)
        == SettingDefault.defaults[PluginSettings.LOGGER_URL]
    )
