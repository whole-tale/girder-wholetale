import copy
import datetime
import logging
import os
import pathlib
import shutil
import time

import requests
from bson import ObjectId
from girder import events
from girder.constants import AccessType
from girder.models.folder import Folder
from girder.models.setting import Setting
from girder.models.token import Token
from girder.models.user import User
from girder_jobs.constants import JobStatus
from girder_plugin_worker.celery import getCeleryApp
from girder_plugin_worker.status import CustomJobStatus

from ..constants import (
    RUNS_ROOT_DIR_NAME,
    VERSIONS_ROOT_DIR_NAME,
    WORKSPACE_NAME,
    ImageStatus,
    InstanceStatus,
    PluginSettings,
)
from ..models.image import Image
from ..models.instance import Instance
from ..models.tale import Tale
from ..models.version_hierarchy import VersionHierarchyModel
from ..schema.misc import containerInfoSchema
from ..utils import get_tale_dir_root, notify_event
from .metrics import metricsLogger
from .path_mappers import HomePathMapper
from .manifest import Manifest

DEFAULT_IDLE_TIMEOUT = 1440.0
logger = logging.getLogger(__name__)


def job_update_after_handler(event):
    job = event.info.get("job")
    if not job:
        return
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

        cullbefore = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
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


def set_home_mapping(event: events.Event) -> None:
    user = event.info
    homes_dir_root = Setting().get(PluginSettings.HOME_DIRS_ROOT)
    home_folder = Folder().createFolder(
        user, "Home", parentType="user", public=False, creator=user, reuseExisting=True
    )
    Folder().setUserAccess(home_folder, user, AccessType.ADMIN, save=False)

    abs_dir = os.path.join(
        homes_dir_root, HomePathMapper().davToPhysical(user["login"])
    )
    abs_dir = pathlib.Path(abs_dir)
    abs_dir.mkdir(parents=True, exist_ok=True)
    home_folder["fsPath"] = abs_dir.as_posix()
    home_folder["isMapping"] = True
    # We don't want to trigger events here
    Folder().save(home_folder, validate=True, triggerEvents=False)


def set_tale_dirs_mapping(event: events.Event) -> None:
    tale = event.info

    def _create_aux_folder(tale, root_name, root_path_setting, creator):
        folder = Tale()._createAuxFolder(tale, root_name, creator=creator)
        folder.update({"seq": 0, "taleId": tale["_id"]})
        Folder().save(folder, validate=True, triggerEvents=False)

        abs_dir = get_tale_dir_root(tale, root_path_setting)
        abs_dir.mkdir(parents=True, exist_ok=True)
        return folder

    tale_dir_root = Setting().get(PluginSettings.TALE_DIRS_ROOT)
    creator = User().load(tale["creatorId"], force=True)
    workspace = Tale()._createAuxFolder(tale, WORKSPACE_NAME, creator)
    # TODO: abstract this
    abs_dir = pathlib.Path(
        os.path.join(tale_dir_root, str(tale["_id"])[0:1], str(tale["_id"]))
    )
    abs_dir.mkdir(parents=True, exist_ok=True)
    workspace["fsPath"] = abs_dir.as_posix()
    workspace["isMapping"] = True
    # We don't want to trigger events here
    Folder().save(workspace, validate=True, triggerEvents=False)
    tale["workspaceId"] = workspace["_id"]

    versions_root = _create_aux_folder(
        tale, VERSIONS_ROOT_DIR_NAME, PluginSettings.VERSIONS_DIRS_ROOT, creator
    )
    tale["versionsRootId"] = versions_root["_id"]

    runs_root = _create_aux_folder(
        tale, RUNS_ROOT_DIR_NAME, PluginSettings.RUNS_DIRS_ROOT, creator
    )
    tale["runsRootId"] = runs_root["_id"]

    tale = Tale().save(tale, validate=True, triggerEvents=False)
    event.addResponse(tale)


def delete_tale_dirs(event: events.Event) -> None:
    tale = event.info
    if (workspace := Folder().load(tale["workspaceId"], force=True)) is not None:
        if "fsPath" in workspace:
            shutil.rmtree(workspace["fsPath"])
        Folder().remove(workspace)

    for aux_folder in ("workspaceId", "runsRootId", "versionsRootId"):
        if aux_folder not in tale:
            continue
        if (folder := Folder().load(tale[aux_folder], force=True)) is not None:
            Folder().remove(folder)
            if "fsPath" in folder:
                shutil.rmtree(folder["fsPath"], ignore_errors=True)
    shutil.rmtree(
        get_tale_dir_root(tale, PluginSettings.VERSIONS_DIRS_ROOT), ignore_errors=True
    )
    shutil.rmtree(
        get_tale_dir_root(tale, PluginSettings.RUNS_DIRS_ROOT), ignore_errors=True
    )


def copy_versions_and_runs(event: events.Event) -> None:
    def get_dir_path(root_id_key, tale):
        if root_id_key == "versionsRootId":
            return get_tale_dir_root(tale, PluginSettings.VERSIONS_DIRS_ROOT)
        elif root_id_key == "runsRootId":
            return get_tale_dir_root(tale, PluginSettings.RUNS_DIRS_ROOT)

    old_tale, new_tale, target_version_id, shallow = event.info
    if shallow and not target_version_id:
        return
    creator = User().load(new_tale["creatorId"], force=True)
    versions_map = {}
    for root_id_key in ("versionsRootId", "runsRootId"):
        old_root = Folder().load(
            old_tale[root_id_key], user=creator, level=AccessType.READ
        )
        new_root = Folder().load(
            new_tale[root_id_key], user=creator, level=AccessType.WRITE
        )
        old_root_path = get_dir_path(root_id_key, old_tale)
        new_root_path = get_dir_path(root_id_key, new_tale)
        for src in Folder().childFolders(old_root, "folder", user=creator):
            if shallow and str(src["_id"]) != target_version_id:
                continue
            dst = Folder().createFolder(new_root, src["name"], creator=creator)
            if root_id_key == "versionsRootId":
                versions_map[str(src["_id"])] = str(dst["_id"])
            filtered_folder = Folder().filter(dst, creator)
            for key in src:
                if key not in filtered_folder and key not in dst:
                    dst[key] = copy.deepcopy(src[key])

            src_path = old_root_path / str(src["_id"])
            dst_path = new_root_path / str(dst["_id"])
            dst_path.mkdir(parents=True)
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True, symlinks=True)
            dst.update(
                {
                    "fsPath": dst_path.absolute().as_posix(),
                    "isMapping": True,
                    "created": src["created"],  # preserve timestamps
                    "updated": src["updated"],
                }
            )
            if root_id_key == "runsRootId":
                current_version = dst_path / "version"
                new_version_id = versions_map[current_version.resolve().name]
                new_version_path = (
                    "../../../../versions/"
                    f"{str(new_tale['_id'])[:2]}/{new_tale['_id']}/{new_version_id}"
                )
                current_version.unlink()
                current_version.symlink_to(new_version_path, True)
                dst["runVersionId"] = ObjectId(new_version_id)
            dst = Folder().save(dst, validate=False, triggerEvents=False)
        # update the time on root
        Folder().updateFolder(new_root)

    versions_root = Folder().load(
        new_tale["versionsRootId"], user=creator, level=AccessType.WRITE
    )
    for version in Folder().childFolders(versions_root, "folder", user=creator):
        tale = copy.deepcopy(new_tale)
        tale.update(VersionHierarchyModel().restoreTaleFromVersion(version))
        manifest = Manifest(
            tale, creator, versionId=version["_id"], expand_folders=False
        )
        dst_path = pathlib.Path(version["fsPath"])
        with open(dst_path / "manifest.json", "w") as fp:
            fp.write(manifest.dump_manifest())

    Folder().updateFolder(versions_root)
    if target_version_id:
        new_version_id = versions_map[str(target_version_id)]
        target_version = Folder().load(
            new_version_id, level=AccessType.READ, user=creator
        )
        VersionHierarchyModel().restore(new_tale, target_version, creator)
