import io

import cherrypy
import mock
import pytest
from girder import auditLogger
from girder.models.folder import Folder
from girder.models.setting import Setting
from girder.models.upload import Upload
from pytest_girder.assertions import assertStatusOk

from girder_wholetale.constants import PluginSettings
from girder_wholetale.utils import add_influx_handler


@pytest.fixture
def influxdb_client():
    return mock.Mock()


@pytest.fixture
def enable_influxlog_setting(influxdb_client):
    Setting().set(PluginSettings.INFLUXDB_BUCKET, "mock_bucket")
    Setting().set(PluginSettings.INFLUXDB_TOKEN, "mock_token")

    yield

    Setting().set(PluginSettings.INFLUXDB_BUCKET, "")
    Setting().set(PluginSettings.INFLUXDB_TOKEN, "")


@pytest.mark.plugin("wholetale")
def test_influxdb_logger(server, admin, enable_influxlog_setting, fsAssetstore):
    with mock.patch(
        "girder_wholetale.utils.influxdb.InfluxDBClient"
    ) as influxdb_client:
        add_influx_handler(auditLogger, Setting().get(PluginSettings.INFLUXDB_BUCKET))
        influx_logger = auditLogger.handlers[0]
        assert influx_logger.bucket == "mock_bucket"
        influxdb_client.assert_called_once_with(
            url=Setting().get(PluginSettings.INFLUXDB_URL),
            token="mock_token",
            org=Setting().get(PluginSettings.INFLUXDB_ORG),
        )

        resp = server.request(path="/user/me", method="GET", user=admin)
        assertStatusOk(resp)

        write = influx_logger.write_api.write
        assert write.call_count == 1
        assert write.call_args_list[0].kwargs["record"]._fields == {
            "method": "GET",
            "status": 200,
            "route": "user/me",
            "params": "{}",
            "message": "rest.request",
        }

        folder = Folder().createFolder(
            admin, "folder", parentType="user", public=True, creator=admin
        )
        assert write.call_count == 2
        assert write.call_args_list[1].kwargs["record"]._fields == {
            "collection": "folder",
            "id": str(folder["_id"]),
        }

        file = Upload().uploadFromFile(
            io.BytesIO(b"blah blah"),
            9,
            "test.txt",
            parentType="folder",
            parent=folder,
            user=admin,
            mimeType="text/plain",
        )

        resp = server.request(
            path="/file/%s/download" % file["_id"],
            method="GET",
            user=admin,
            isJson=False,
        )
        assertStatusOk(resp)
        assert resp.collapse_body() == b"blah blah"
        assert write.call_count == 7
        assert write.call_args_list[-2].kwargs["record"]._fields == {
            "fileId": str(file["_id"]),
            "startBytes": None,
            "endBytes": None,
            "extraParameters": None,
        }
        assert influx_logger.write_api.close.call_count == 0
        assert influxdb_client.return_value.close.call_count == 0
        cherrypy.engine.restart()
        assert influx_logger.write_api.close.call_count == 1
        assert influxdb_client.return_value.close.call_count == 1
        assert auditLogger.handlers == []
