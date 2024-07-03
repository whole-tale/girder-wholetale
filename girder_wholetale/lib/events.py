import datetime
import logging
import time

import requests
from bson import ObjectId
from girder.models.token import Token
from girder.models.user import User
from girder_jobs.constants import JobStatus
from girder_worker.girder_plugin.celery import getCeleryApp
from girder_worker.girder_plugin.status import CustomJobStatus

from ..constants import ImageStatus, InstanceStatus
from ..models.image import Image
from ..models.instance import Instance
from ..models.tale import Tale
from ..schema.misc import containerInfoSchema
from ..utils import notify_event
from .metrics import metricsLogger

DEFAULT_IDLE_TIMEOUT = 1440.0
logger = logging.getLogger(__name__)

def job_update_after_handler(event):
    job = event.info
    if job["title"] == "Build Tale Image" and job.get("status") is not None:
        update_build_status(event)
    elif job["title"] == "Publish Tale" and job.get("status") == JobStatus.SUCCESS:
        update_publish_status(event)
    elif job["title"] == "Update Instance" and job.get("status") is not None:
        update_instance_status(event)
    elif job["title"] == "Spawn Instance" and job.get("status") is not None:
        finalize_instance(event)


def update_build_status(event):
    """
    Event handler that updates the Tale object based on the build_tale_image task.
    """
    job = event.info["job"]
    if job["title"] == "Build Tale Image" and job.get("status") is not None:
        status = int(job["status"])
        tale = Tale().load(job["args"][0], force=True)

        if "imageInfo" not in tale:
            tale["imageInfo"] = {}

        # Store the previous status, if present.
        previousStatus = -1
        try:
            previousStatus = tale["imageInfo"]["status"]
        except KeyError:
            pass

        if status == JobStatus.SUCCESS:
            result = getCeleryApp().AsyncResult(job["celeryTaskId"]).get()
            tale["imageInfo"]["digest"] = result["image_digest"]
            tale["imageInfo"]["imageId"] = tale["imageId"]
            tale["imageInfo"]["repo2docker_version"] = result["repo2docker_version"]
            tale["imageInfo"]["last_build"] = result["last_build"]
            tale["imageInfo"]["status"] = ImageStatus.AVAILABLE
        elif status == JobStatus.ERROR:
            tale["imageInfo"]["status"] = ImageStatus.INVALID
        elif status in (JobStatus.CANCELED, CustomJobStatus.CANCELING):
            tale["imageInfo"]["status"] = ImageStatus.UNAVAILABLE
        elif status in (JobStatus.QUEUED, JobStatus.RUNNING):
            tale["imageInfo"]["jobId"] = job["_id"]
            tale["imageInfo"]["status"] = ImageStatus.BUILDING

        delete_instance = status in (
            JobStatus.ERROR,
            JobStatus.CANCELED,
            CustomJobStatus.CANCELING,
        )
        if delete_instance and "instance_id" in job:
            if instance := Instance().load(job["instance_id"], force=True):
                instance_creator = User().load(instance["creatorId"], force=True)
                Instance().deleteInstance(instance, instance_creator)

        # If the status changed, save the object
        if (
            "status" in tale["imageInfo"]
            and tale["imageInfo"]["status"] != previousStatus
        ):
            Tale().updateTale(tale)


def update_publish_status(event):
    job = event.info["job"]
    if not (job["title"] == "Publish Tale" and job.get("status") == JobStatus.SUCCESS):
        return
    publication_info = getCeleryApp().AsyncResult(job["celeryTaskId"]).get()

    metricsLogger.info(
        "tale.publish",
        extra={
            "details": {
                "id": ObjectId(job["args"][0]),
                "publishInfo": publication_info,
                "userId": ObjectId(job["userId"]),
            }
        },
    )


def update_instance_status(event):
    job = event.info["job"]
    if not (job["title"] == "Update Instance" and job.get("status") is not None):
        return

    status = int(job["status"])
    instance = Instance().load(job["args"][0], force=True)

    if status == JobStatus.SUCCESS:
        result = getCeleryApp().AsyncResult(job["celeryTaskId"]).get()
        instance["containerInfo"].update(result)
        instance["status"] = InstanceStatus.RUNNING
    elif status == JobStatus.ERROR:
        instance["status"] = InstanceStatus.ERROR
    elif status in (JobStatus.QUEUED, JobStatus.RUNNING):
        instance["status"] = InstanceStatus.LAUNCHING
    Instance().updateInstance(instance)


def finalize_instance(event):
    job = event.info["job"]

    if job.get("instance_id"):
        instance = Instance().load(job["instance_id"], force=True)
        if instance is None:
            return

        if (
            instance["status"] == InstanceStatus.LAUNCHING
            and job["status"] == JobStatus.ERROR  # noqa
        ):
            instance["status"] = InstanceStatus.ERROR
            Instance().updateInstance(instance)

    if job["title"] == "Spawn Instance" and job.get("status") is not None:
        status = int(job["status"])
        instance_id = job["args"][0]["instanceId"]
        instance = Instance().load(instance_id, force=True, exc=True)
        tale = Tale().load(instance["taleId"], force=True)
        update = True
        event_name = None

        if (
            status == JobStatus.SUCCESS
            and instance["status"] == InstanceStatus.LAUNCHING  # noqa
        ):
            # Get a url to the container
            service = getCeleryApp().AsyncResult(job["celeryTaskId"]).get()
            url = service.get("url", "https://girder.hub.yt/")

            # Generate the containerInfo
            valid_keys = set(containerInfoSchema["properties"].keys())
            containerInfo = {key: service.get(key, "") for key in valid_keys}
            # Preserve the imageId / current digest in containerInfo
            containerInfo["imageId"] = tale["imageId"]
            containerInfo["digest"] = tale["imageInfo"]["digest"]

            # Set the url and the containerInfo since they're used in /authorize
            new_fields = {"url": url, "containerInfo": containerInfo}
            if "sessionId" in service:
                new_fields["sessionId"] = ObjectId(service["sessionId"])
            Instance().update({"_id": instance["_id"]}, {"$set": new_fields})

            user = User().load(instance["creatorId"], force=True)
            token = Token().createToken(user=user, days=0.25)
            _wait_for_server(url, token["_id"])

            # Since _wait_for_server can potentially take some time,
            # we need to refresh the state of the instance
            # TODO: Why? What can modify instance status at this point?
            instance = Instance().load(instance_id, force=True, exc=True)
            if instance["status"] != InstanceStatus.LAUNCHING:
                return  # bail

            instance["status"] = InstanceStatus.RUNNING
            event_name = "wt_instance_running"
        elif (
            status == JobStatus.ERROR and instance["status"] != InstanceStatus.ERROR  # noqa
        ):
            instance["status"] = InstanceStatus.ERROR
        elif (
            status == JobStatus.ERROR and instance["status"] == InstanceStatus.ERROR  # noqa
        ):
            event_name = "wt_instance_error"
        elif (
            status in (JobStatus.QUEUED, JobStatus.RUNNING)
            and instance["status"] != InstanceStatus.LAUNCHING  # noqa
        ):
            instance["status"] = InstanceStatus.LAUNCHING
        else:
            update = False

        if update:
            msg = "Updating instance ({_id}) in finalizeInstance".format(**instance)
            msg += " for job(id={_id}, status={status})".format(**job)
            logger.debug(msg)
            Instance().updateInstance(instance)

            if event_name:
                notify_event(
                    [instance["creatorId"]],
                    event_name,
                    {"taleId": instance["taleId"], "instanceId": instance["_id"]},
                )


def cullIdleInstances(event):
    """
    Stop idle instances that have exceeded the configured timeout
    """

    logger.info("Culling idle instances")

    images = Image().find()
    for image in images:
        idleTimeout = image.get("idleTimeout", DEFAULT_IDLE_TIMEOUT)

        cullbefore = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=idleTimeout
        )

        instances = Instance().find(
            {"lastActivity": {"$lt": cullbefore}, "containerInfo.imageId": image["_id"]}
        )

        for instance in instances:
            logger.info(
                "Stopping instance {}: idle timeout exceeded.".format(instance["_id"])
            )
            user = User().load(instance["creatorId"], force=True)
            Instance().deleteInstance(instance, user)


def _wait_for_server(url, token, timeout=30, wait_time=0.5):
    """Wait for a server to show up within a newly launched instance."""
    tic = time.time()
    while time.time() - tic < timeout:
        try:
            r = requests.get(url, cookies={"girderToken": token}, timeout=1)
            r.raise_for_status()
            if int(r.headers.get("Content-Length", "0")) == 0:
                raise ValueError("HTTP server returns no content")
        except requests.exceptions.HTTPError as err:
            logger.info(
                "Booting server at [%s], getting HTTP status [%s]",
                url,
                err.response.status_code,
            )
            time.sleep(wait_time)
        except requests.exceptions.SSLError:
            logger.info("Booting server at [%s], getting SSLError", url)
            time.sleep(wait_time)
        except requests.exceptions.ConnectionError:
            logger.info("Booting server at [%s], getting ConnectionError", url)
            time.sleep(wait_time)
        except Exception as ex:
            logger.info('Booting server at [%s], getting "%s"', url, str(ex))
        else:
            break
