#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import requests

from girder.constants import AccessType, SortDir, TokenScope
from girder.exceptions import ValidationException
from girder.models.model_base import AccessControlledModel
from girder.models.setting import Setting
from girder.models.token import Token
from girder.utility import JsonEncoder
from girder_jobs.constants import REST_CREATE_JOB_TOKEN_SCOPE
from gwvolman.tasks import \
    create_volume, launch_container, update_container, shutdown_container, \
    remove_volume, build_tale_image
from gwvolman.tasks_base import BUILD_TALE_IMAGE_STEP_TOTAL
from gwvolman.tasks_docker import (
    CREATE_VOLUME_STEP_TOTAL,
    LAUNCH_CONTAINER_STEP_TOTAL,
    UPDATE_CONTAINER_STEP_TOTAL,
)

from ..constants import InstanceStatus, PluginSettings
from ..lib.metrics import metricsLogger
from ..utils import init_progress, notify_event


TASK_TIMEOUT = 15.0
BUILD_TIMEOUT = 360.0


class Instance(AccessControlledModel):
    def initialize(self):
        self.name = "instance"
        compoundSearchIndex = (
            ("taleId", SortDir.ASCENDING),
            ("creatorId", SortDir.DESCENDING),
            ("name", SortDir.ASCENDING),
        )
        self.ensureIndices([(compoundSearchIndex, {})])

        self.exposeFields(
            level=AccessType.READ,
            fields={"_id", "created", "creatorId", "iframe", "name", "taleId"},
        )
        self.exposeFields(
            level=AccessType.WRITE,
            fields={"containerInfo", "lastActivity", "status", "url", "sessionId"},
        )

    def validate(self, instance):
        if not InstanceStatus.isValid(instance["status"]):
            raise ValidationException(
                "Invalid instance status %s." % instance["status"], field="status"
            )
        return instance

    def list(
        self, user=None, tale=None, limit=0, offset=0, sort=None, currentUser=None
    ):
        """
        List a page of jobs for a given user.

        :param user: The user who owns the job.
        :type user: dict or None
        :param limit: The page limit.
        :param offset: The page offset
        :param sort: The sort field.
        :param currentUser: User for access filtering.
        """
        cursor_def = {}
        if user is not None:
            cursor_def["creatorId"] = user["_id"]
        if tale is not None:
            cursor_def["taleId"] = tale["_id"]
        cursor = self.find(cursor_def, sort=sort)
        for r in self.filterResultsByPermission(
            cursor=cursor,
            user=currentUser,
            level=AccessType.READ,
            limit=limit,
            offset=offset,
        ):
            yield r

    def updateAndRestartInstance(self, instance, user, tale):
        """
        Updates and restarts an instance.

        :param image: The instance document to restart.
        :type image: dict
        :returns: The instance document that was edited.
        """
        token = Token().createToken(user=user, days=0.5)

        digest = tale["imageInfo"]["digest"]

        resource = {
            "type": "wt_update_instance",
            "instance_id": instance["_id"],
            "tale_title": tale["title"],
        }
        total = UPDATE_CONTAINER_STEP_TOTAL

        notification = init_progress(
            resource, user, "Updating instance", "Initializing", total
        )

        update_container.signature(
            args=[str(instance["_id"])],
            queue="manager",
            girder_job_other_fields={"wt_notification_id": str(notification["_id"])},
            girder_client_token=str(token["_id"]),
            kwargs={"digest": digest},
        ).apply_async()

    def updateInstance(self, instance):
        """
        Updates an instance.

        :param image: The instance document to restart.
        :type image: dict
        :returns: The instance document that was edited.
        """

        instance["updated"] = datetime.datetime.utcnow()
        return self.save(instance)

    def deleteInstance(self, instance, user):
        initial_status = instance["status"]
        if initial_status == InstanceStatus.DELETING:
            return
        instance["status"] = InstanceStatus.DELETING
        instance = self.updateInstance(instance)
        token = Token().createToken(user=user, days=0.5)

        task1 = shutdown_container.apply_async(
            args=[str(instance["_id"])],
            girder_client_token=str(token["_id"]),
            queue="manager",
            time_limit=TASK_TIMEOUT,
        )
        notify_event(
            [instance["creatorId"]],
            "wt_instance_deleting",
            {"taleId": instance["taleId"], "instanceId": instance["_id"]},
        )

        try:
            queue = instance["containerInfo"].get("nodeId", "celery")
            task2 = remove_volume.apply_async(
                args=[str(instance["_id"])],
                girder_client_token=str(token["_id"]),
                queue=queue,
                time_limit=TASK_TIMEOUT,
            )
        except KeyError:
            pass

        # TODO: handle errors
        # wait for tasks to finish
        task1.get(timeout=TASK_TIMEOUT)
        try:
            task2.get(timeout=TASK_TIMEOUT)
        except UnboundLocalError:
            pass
        self.remove(instance)

        notify_event(
            [instance["creatorId"]],
            "wt_instance_deleted",
            {"taleId": instance["taleId"], "instanceId": instance["_id"]},
        )

        metricsLogger.info(
            "instance.remove",
            extra={
                "details": {
                    "id": instance["_id"],
                    "taleId": instance["taleId"],
                    "status": initial_status,
                    "containerInfo": instance.get("containerInfo"),
                }
            },
        )

    def createInstance(self, tale, user, /, *, name=None, save=True, spawn=True):
        if not name:
            name = tale.get("title", "")

        now = datetime.datetime.utcnow()
        instance = {
            "created": now,
            "creatorId": user["_id"],
            "iframe": tale.get("iframe", False),
            "lastActivity": now,
            "name": name,
            "status": InstanceStatus.LAUNCHING,
            "taleId": tale["_id"],
        }

        self.setUserAccess(instance, user=user, level=AccessType.ADMIN)
        if save:
            instance = self.save(instance)

        if spawn:
            # Create single job
            token = Token().createToken(
                user=user,
                days=0.5,
                scope=(TokenScope.USER_AUTH, REST_CREATE_JOB_TOKEN_SCOPE),
            )

            resource = {
                "type": "wt_create_instance",
                "tale_id": tale["_id"],
                "instance_id": instance["_id"],
                "tale_title": tale["title"],
            }

            total = (
                BUILD_TALE_IMAGE_STEP_TOTAL
                + CREATE_VOLUME_STEP_TOTAL
                + LAUNCH_CONTAINER_STEP_TOTAL
            )

            notification = init_progress(
                resource, user, "Creating instance", "Initializing", total
            )

            user = json.loads(json.dumps(user, cls=JsonEncoder))

            buildTask = build_tale_image.signature(
                args=[str(tale["_id"]), False],
                girder_job_other_fields={
                    "wt_notification_id": str(notification["_id"]),
                    "instance_id": str(instance["_id"]),
                },
                girder_client_token=str(token["_id"]),
                girder_user=user,
                immutable=True,
            )
            volumeTask = create_volume.signature(
                args=[str(instance["_id"]), Setting().get(PluginSettings.MOUNTS)],
                girder_job_other_fields={
                    "wt_notification_id": str(notification["_id"]),
                    "instance_id": str(instance["_id"]),
                },
                girder_client_token=str(token["_id"]),
                girder_user=user,
                immutable=True,
            )
            serviceTask = launch_container.signature(
                girder_job_other_fields={
                    "wt_notification_id": str(notification["_id"]),
                    "instance_id": str(instance["_id"]),
                },
                girder_client_token=str(token["_id"]),
                girder_user=user,
            )

            (buildTask | volumeTask | serviceTask).apply_async()

            notify_event(
                [instance["creatorId"]],
                "wt_instance_launching",
                {"taleId": instance["taleId"], "instanceId": instance["_id"]},
            )

        metricsLogger.info(
            "instance.create",
            extra={
                "details": {
                    "id": instance["_id"],
                    "taleId": instance["taleId"],
                    "spawn": spawn,
                }
            },
        )

        return instance

    def get_logs(self, instance, tail):
        r = requests.get(
            Setting().get(PluginSettings.LOGGER_URL),
            params={"tail": tail, "name": instance["containerInfo"].get("name")},
        )
        try:
            r.raise_for_status()
            return r.text
        except requests.exceptions.HTTPError:
            return f"Logs for instance {instance['_id']} are currently unavailable..."
