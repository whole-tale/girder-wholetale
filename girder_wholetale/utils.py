import datetime
import json
import logging
import os
import pathlib
from urllib.parse import quote_plus

try:
    import influxdb_client as influxdb
except ImportError:
    influxdb = None

import cherrypy
from girder.models.notification import Notification
from girder.models.setting import Setting
from girder.models.user import User
from girder.utility.model_importer import ModelImporter

from .constants import PluginSettings

NOTIFICATION_EXP_HOURS = 1
WT_EVENT_EXP_SECONDS = int(os.environ.get("GIRDER_WT_EVENT_EXP_SECONDS", 5))


class InfluxHandler(logging.Handler):
    def __init__(self, bucket):
        if influxdb is None:
            raise ImportError("InfluxDB client not installed")

        super().__init__()
        self.client = influxdb.InfluxDBClient(
            url=Setting().get(PluginSettings.INFLUXDB_URL),
            token=Setting().get(PluginSettings.INFLUXDB_TOKEN),
            org=Setting().get(PluginSettings.INFLUXDB_ORG),
        )
        self.write_api = self.client.write_api()
        self.bucket = bucket

    @staticmethod
    def _document_create(record):
        return (
            influxdb.Point("document")
            .tag("level", record.levelname)
            .field("collection", record.details.get("collection"))
            .field("id", str(record.details.get("id")))
            .time(int(record.created * 1e9), influxdb.WritePrecision.NS)
        )

    @staticmethod
    def _download(record):
        return (
            influxdb.Point("download")
            .tag("level", record.levelname)
            .field("fileId", str(record.details.get("fileId")))
            .field("startBytes", record.details.get("startBytes"))
            .field("endBytes", record.details.get("endBytes"))
            .field("extraParameters", record.details.get("extraParameters"))
            .time(int(record.created * 1e9), influxdb.WritePrecision.NS)
        )

    @staticmethod
    def _rest_request(record):
        return (
            influxdb.Point("rest_request")
            .tag("level", record.levelname)
            .field("method", record.details.get("method"))
            .field("status", record.details.get("status"))
            .field("route", "/".join(record.details.get("route", "")))
            .field("params", json.dumps(record.details.get("params")))
            .field("message", record.getMessage())
            .time(int(record.created * 1e9), influxdb.WritePrecision.NS)
        )

    def emit(self, record):
        match record.getMessage():
            case "document.create":
                point = self._document_create(record)
            case "file.download":
                point = self._download(record)
            case "rest.request":
                point = self._rest_request(record)
        self.write_api.write(bucket=self.bucket, record=point)

    def close(self):
        self.write_api._write_options.write_scheduler.executor.shutdown(wait=False)
        self.write_api.close()
        self.client.close()
        super().close()


def add_influx_handler(logger, bucket):
    def stop_influx_client():
        for handler in logger.handlers:
            if isinstance(handler, InfluxHandler):
                logger.removeHandler(handler)
                handler.close()

    stop_influx_client()  # Remove any existing handlers
    logger.addHandler(InfluxHandler(bucket))
    stop_influx_client.priority = 10
    cherrypy.engine.subscribe(
        "stop", stop_influx_client
    )  # ensure we clean up on restart


def get_tale_dir_root(tale: dict, root_path_setting: str) -> pathlib.Path:
    root = Setting().get(root_path_setting)
    return pathlib.Path(root) / str(tale["_id"])[0:2] / str(tale["_id"])


def getOrCreateRootFolder(name, description=""):
    collection = ModelImporter.model("collection").createCollection(
        name, public=True, reuseExisting=True
    )
    folder = ModelImporter.model("folder").createFolder(
        collection,
        name,
        parentType="collection",
        public=True,
        reuseExisting=True,
        description=description,
    )
    return folder


def esc(value):
    """
    Escape a string so it can be used in a Solr query string
    :param value: The string that will be escaped
    :type value: str
    :return: The escaped string
    :rtype: str
    """
    return quote_plus(value)


def notify_event(users, event, affectedIds):
    """
    Notify multiple users of a particular WT event
    :param users: Arrayof user IDs
    :param event: WT Event name
    :param affectedIds: Map of affected object Ids
    """
    data = {
        "event": event,
        "affectedResourceIds": affectedIds,
        "resourceName": "WT event",
    }

    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=WT_EVENT_EXP_SECONDS
    )

    for user_id in users:
        user = User().load(user_id, force=True)
        Notification().createNotification(
            type="wt_event", data=data, user=user, expires=expires
        )


def init_progress(resource, user, title, message, total):
    resource["jobCurrent"] = 0
    resource["jobId"] = None
    data = {
        "title": title,
        "total": total,
        "current": 0,
        "state": "active",
        "message": message,
        "estimateTime": False,
        "resource": resource,
        "resourceName": "WT custom resource",
    }

    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        hours=NOTIFICATION_EXP_HOURS
    )

    return Notification().createNotification(
        type="wt_progress", data=data, user=user, expires=expires
    )


def deep_get(dikt, path):
    """Get a value located in `path` from a nested dictionary.

    Use a string separated by periods as the path to access
    values in a nested dictionary:

    deep_get(data, "data.files.0") == data["data"]["files"][0]

    Taken from jupyter/repo2docker
    """
    value = dikt
    for component in path.split("."):
        if component.isdigit():
            value = value[int(component)]
        else:
            value = value[component]
    return value


def diff_access(access1, access2):
    """Diff two access lists to identify which users
    were added or removed.
    """
    existing = {str(user["id"]) for user in access1["users"]}
    new = {str(user["id"]) for user in access2["users"]}
    added = list(new - existing)
    removed = list(existing - new)
    return (added, removed)
