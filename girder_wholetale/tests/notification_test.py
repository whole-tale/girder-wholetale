import pytest
from girder.models.notification import Notification, ProgressState
from girder.models.setting import Setting
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from girder_plugin_worker.constants import PluginSettings as WorkerPluginSettings

from girder_wholetale.utils import init_progress


def assertNotification(notification_id, state, total, current, msg):
    notification = Notification().load(notification_id)
    assert notification["data"]["state"] == state
    assert notification["data"]["total"] == total
    assert notification["data"]["current"] == current
    assert notification["data"]["message"] == msg


@pytest.mark.xfail(reason="Needs master marged into v4-integration")
@pytest.mark.plugin("wholetale")
def test_single_job_notification(server, user):
    # TODO: Why do we need it here?
    Setting().set(WorkerPluginSettings.API_URL, "http://localhost:8080/api/v1")

    total = 2
    resource = {"type": "wt_test_build_image", "instance_id": "instance_id"}

    notification = init_progress(
        resource, user, "Test notification", "Creating job", total
    )

    # Job to test error path
    job = Job().createJob(
        title="Error test job",
        type="test",
        handler="my_handler",
        user=user,
        public=False,
        args=["tale_id"],
        kwargs={},
        otherFields={"wt_notification_id": str(notification["_id"])},
    )
    job = Job().updateJob(
        job, status=JobStatus.INACTIVE, progressTotal=2, progressCurrent=0
    )
    assert job["status"] == JobStatus.INACTIVE
    notification = Notification().load(notification["_id"])
    assertNotification(notification["_id"], ProgressState.QUEUED, 2, 0, "Creating job")
    assert notification["data"]["resource"]["jobs"][0] == job["_id"]

    # State change to ACTIVE
    job = Job().updateJob(job, status=JobStatus.RUNNING)
    assert job["status"] == JobStatus.RUNNING

    # Progress update
    job = Job().updateJob(
        job,
        status=JobStatus.RUNNING,
        progressCurrent=1,
        progressMessage="Error test message",
    )
    assertNotification(
        notification["_id"], ProgressState.ACTIVE, 2, 1, "Error test message"
    )

    # State change to ERROR
    job = Job().updateJob(job, status=JobStatus.ERROR)
    assertNotification(
        notification["_id"], ProgressState.ERROR, 2, 1, "Error test message"
    )

    # new notification
    notification = init_progress(
        resource, user, "Test notification", "Creating job", total
    )

    # New job to test success path
    job = Job().createJob(
        title="Test Job",
        type="test",
        handler="my_handler",
        user=user,
        public=False,
        args=["tale_id"],
        kwargs={},
        otherFields={"wt_notification_id": str(notification["_id"])},
    )

    # State change to ACTIVE
    job = Job().updateJob(job, status=JobStatus.RUNNING)
    assert job["status"] == JobStatus.RUNNING

    # Progress update
    job = Job().updateJob(
        job,
        status=JobStatus.RUNNING,
        progressCurrent=1,
        progressMessage="Success test message",
    )
    assertNotification(
        notification["_id"], ProgressState.ACTIVE, 2, 1, "Success test message"
    )

    job = Job().updateJob(job, status=JobStatus.SUCCESS, progressCurrent=2)
    assertNotification(
        notification["_id"], ProgressState.SUCCESS, 2, 2, "Success test message"
    )


@pytest.mark.xfail(reason="Needs master marged into v4-integration")
@pytest.mark.plugin("wholetale")
def testChainedJobNotification(server, user):
    Setting().set(WorkerPluginSettings.API_URL, "http://localhost:8080/api/v1")

    total = 5  # 2 + 2 + 1
    resource = {"type": "wt_test_build_image", "instance_id": "instance_id"}

    notification = init_progress(
        resource, user, "Test notification", "Creating job", total
    )

    # First Job
    job = Job().createJob(
        title="First Job",
        type="test",
        handler="my_handler",
        user=user,
        public=False,
        args=["tale_id"],
        kwargs={},
        otherFields={"wt_notification_id": str(notification["_id"])},
    )
    # State change to ACTIVE
    msg = "Some message 1"
    job = Job().updateJob(
        job,
        status=JobStatus.RUNNING,
        progressCurrent=1,
        progressTotal=2,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.RUNNING
    assertNotification(notification["_id"], ProgressState.ACTIVE, total, 1, msg)

    msg = "Success msg 1"
    job = Job().updateJob(
        job,
        status=JobStatus.SUCCESS,
        progressCurrent=2,
        progressTotal=2,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.SUCCESS
    assertNotification(notification["_id"], ProgressState.ACTIVE, total, 2, msg)

    # Second Job
    job = Job().createJob(
        title="Second Job",
        type="test",
        handler="my_handler",
        user=user,
        public=False,
        args=["tale_id"],
        kwargs={},
        otherFields={"wt_notification_id": str(notification["_id"])},
    )
    # State change to QUEUE
    msg = "Some message 1"
    job = Job().updateJob(
        job,
        status=JobStatus.INACTIVE,
        progressTotal=2,
        progressCurrent=0,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.INACTIVE
    assertNotification(notification["_id"], ProgressState.QUEUED, total, 2, msg)

    job = Job().updateJob(job, status=JobStatus.QUEUED)
    assertNotification(notification["_id"], ProgressState.QUEUED, total, 2, msg)

    job = Job().updateJob(job, status=JobStatus.RUNNING)
    assertNotification(notification["_id"], ProgressState.ACTIVE, total, 2, msg)

    msg = "Success msg 2"
    job = Job().updateJob(
        job,
        status=JobStatus.SUCCESS,
        progressCurrent=2,
        progressTotal=2,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.SUCCESS
    assertNotification(notification["_id"], ProgressState.ACTIVE, total, 4, msg)

    # Final Job
    job = Job().createJob(
        title="Final Job",
        type="test",
        handler="my_handler",
        user=user,
        public=False,
        args=["tale_id"],
        kwargs={},
        otherFields={"wt_notification_id": str(notification["_id"])},
    )
    msg = "Some message 1"
    job = Job().updateJob(
        job,
        status=JobStatus.INACTIVE,
        progressTotal=1,
        progressCurrent=0,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.INACTIVE
    assertNotification(notification["_id"], ProgressState.QUEUED, total, 4, msg)

    job = Job().updateJob(job, status=JobStatus.QUEUED)
    job = Job().updateJob(job, status=JobStatus.RUNNING)
    msg = "Success msg 3"
    job = Job().updateJob(
        job,
        status=JobStatus.SUCCESS,
        progressCurrent=1,
        progressTotal=1,
        progressMessage=msg,
    )
    assert job["status"] == JobStatus.SUCCESS
    assertNotification(notification["_id"], ProgressState.SUCCESS, total, 5, msg)
