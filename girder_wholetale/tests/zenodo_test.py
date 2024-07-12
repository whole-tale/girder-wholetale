import io
import json
import os
import zipfile
from urllib.parse import parse_qs, urlparse

import httmock
import mock
import pytest
from girder.models.folder import Folder
from girder.models.setting import Setting
from girder.models.user import User
from girder_oauth.settings import PluginSettings as OAuthSettings
from pytest_girder.assertions import assertStatus, assertStatusOk

from girder_wholetale.constants import PluginSettings, SettingDefault
from girder_wholetale.lib.zenodo.provider import ZenodoImportProvider
from girder_wholetale.models.tale import Tale
from girder_wholetale.models.image import Image


@httmock.all_requests
def mock_other_request(url, request):
    raise Exception("Unexpected url %s" % str(request.url))


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/records/430905$",
    method="GET",
)
def mock_get_record(url, request):
    return httmock.response(
        status_code=200,
        content={
            "id": 430905,
            "files": [
                {
                    "bucket": "111daf16-680a-48bb-bb85-5e251f3d7609",
                    "checksum": "md5:42c822247416fcf0ad9c9f7ee776bae4",
                    "key": "5df2752385bc9fc730ce423b.zip",
                    "links": {
                        "self": (
                            "https://sandbox.zenodo.org/api/files/"
                            "111daf16-680a-48bb-bb85-5e251f3d7609/"
                            "5df2752385bc9fc730ce423b.zip"
                        )
                    },
                    "size": 92599,
                    "type": "zip",
                }
            ],
            "doi": "10.5072/zenodo.430905",
            "links": {"doi": "https://doi.org/10.5072/zenodo.430905"},
            "created": "2019-12-12T17:13:35.820719+00:00",
            "metadata": {"keywords": ["Tale", "Astronomy"]},
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


def fake_urlopen(url):
    fname = os.path.join(
        os.path.dirname(__file__), "data", "5c92fbd472a9910001fbff72.zip"
    )
    return open(fname, "rb")


@pytest.fixture
def jupyter_image(user):
    img = Image().createImage(
        name="Jupyter Classic",
        creator=user,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="SomeBuildPack",
            user="someUser",
            port=8888,
            urlPath="",
        ),
    )
    yield img
    Image().remove(img)


@pytest.mark.vcr
@pytest.mark.plugin("wholetale")
def test_lookup(server, user):
    resolved_lookup = {
        "dataId": "https://zenodo.org/records/3459420",
        "doi": "doi:10.5281/zenodo.3459420",
        "name": "A global network of biomedical relationships derived from text_ver_7",
        "repository": "Zenodo",
        "size": 8037626747,
        "tale": False,
    }

    resp = server.request(
        path="/repository/lookup",
        method="GET",
        user=user,
        params={
            "dataId": json.dumps(
                [
                    "https://doi.org/10.5281/zenodo.3459420",
                    "https://zenodo.org/records/3459420",
                ]
            )
        },
    )
    assertStatus(resp, 200)
    assert resp.json == [resolved_lookup, resolved_lookup]

    resolved_listFiles = [
        {
            "jbferet_biodivMapR v1.0.1_ver_v1.0.1": {
                "jbferet": {"fileList": [{"biodivMapR-v1.0.1.zip": {"size": 24692383}}]}
            }
        }
    ]

    resp = server.request(
        path="/repository/listFiles",
        method="GET",
        user=user,
        params={"dataId": json.dumps(["https://zenodo.org/record/3463499"])},
    )
    assertStatus(resp, 200)
    assert resp.json == resolved_listFiles


@pytest.mark.plugin("wholetale")
def test_extra_hosts(server, admin):
    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.ZENODO_EXTRA_HOSTS,
            "value": "https://sandbox.zenodo.org/record",
        },
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "field": "value",
        "type": "validation",
        "message": "Zenodo extra hosts setting must be a list.",
    }

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.ZENODO_EXTRA_HOSTS,
            "value": json.dumps(["not a url"]),
        },
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "field": "value",
        "type": "validation",
        "message": "Invalid URL in Zenodo extra hosts",
    }

    # defaults
    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.ZENODO_EXTRA_HOSTS, "value": ""},
    )
    assertStatusOk(resp)
    resp = server.request(
        "/system/setting",
        user=admin,
        method="GET",
        params={"key": PluginSettings.ZENODO_EXTRA_HOSTS},
    )
    assertStatusOk(resp)
    assert resp.body[0].decode() == str(
        SettingDefault.defaults[PluginSettings.ZENODO_EXTRA_HOSTS]
    )

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "list": json.dumps(
                [
                    {
                        "key": PluginSettings.ZENODO_EXTRA_HOSTS,
                        "value": ["https://sandbox.zenodo.org/record/"],
                    }
                ]
            )
        },
    )
    assertStatusOk(resp)

    assert (
        "^http(s)?://(sandbox.zenodo.org/record/|zenodo.org/record).*$"
        == ZenodoImportProvider().regex[0].pattern
    )


@pytest.mark.vcr
@pytest.mark.plugin("wholetale")
def test_dataset_with_hierarchy(server, user):
    resp = server.request(
        path="/repository/listFiles",
        method="GET",
        user=user,
        params={"dataId": json.dumps(["https://zenodo.org/record/3463499"])},
    )
    assertStatus(resp, 200)
    assert resp.json[0] == {
        "jbferet_biodivMapR v1.0.1_ver_v1.0.1": {
            "jbferet": {"fileList": [{"biodivMapR-v1.0.1.zip": {"size": 24692383}}]}
        }
    }


@pytest.mark.vcr
@pytest.mark.plugin("wholetale")
def test_manifest_helpers(server, user):
    resp = server.request(
        path="/repository/lookup",
        method="GET",
        user=user,
        params={"dataId": json.dumps(["https://zenodo.org/record/3463499"])},
    )
    assertStatus(resp, 200)
    data_map = resp.json

    resp = server.request(
        path="/dataset/register",
        method="POST",
        params={"dataMap": json.dumps(data_map)},
        user=user,
    )
    assertStatusOk(resp)

    user = User().load(user["_id"], force=True)
    dataset_root_folder = Folder().load(user["myData"][0], user=user)
    child_folder = next(
        Folder().childFolders(
            parentType="folder", parent=dataset_root_folder, user=user
        )
    )
    child_item = next((item for item in Folder().childItems(folder=child_folder)))

    for obj in (dataset_root_folder, child_folder, child_item):
        assert (
            ZenodoImportProvider().getDatasetUID(obj, user)
            == "doi:10.5281/zenodo.3463499"
        )


@pytest.mark.plugin("wholetale")
def test_import_binder(server, user):
    resp = server.request(
        path="/integration/zenodo",
        method="GET",
        user=user,
        params={
            "record_id": "5092301",
            "resource_server": "zenodo.org",
        },
        isJson=False,
    )

    assert "Location" in resp.headers
    location = urlparse(resp.headers["Location"])
    assert location.netloc == "dashboard.wholetale.org"
    qs = parse_qs(location.query)
    assert qs["asTale"][0]
    assert qs["name"][0].startswith("antonninkov/ISSI2021")
    assert qs["uri"] == ["https://zenodo.org/record/5092301"]


@pytest.mark.plugin("wholetale")
@pytest.mark.xfail(reason="Need to port import to gwvolman and update dataset to non-DataONE")
def test_import_tale(server, user, jupyter_image, fsAssetstore):
    Setting().set(OAuthSettings.PROVIDERS_ENABLED, ["globus"])
    Setting().set(OAuthSettings.GLOBUS_CLIENT_ID, "client_id")
    Setting().set(OAuthSettings.GLOBUS_CLIENT_SECRET, "secret_id")

    resp = server.request(path="/integration/zenodo", method="GET")
    assertStatus(resp, 400)
    assert resp.json == {
        "type": "rest",
        "message": "You need to provide either 'doi' or 'record_id'",
    }

    resp = server.request(
        path="/integration/zenodo",
        method="GET",
        params={"doi": "10.5072/zenodo.430905"},
    )
    assertStatus(resp, 400)
    assert resp.json == {"type": "rest", "message": "resource_server not set"}

    resp = server.request(
        path="/integration/zenodo",
        method="GET",
        params={"doi": "10.5072/zenodo.430905"},
        additionalHeaders=[("Referer", "https://sandbox.zenodo.org")],
        isJson=False,
    )
    assertStatus(resp, 303)
    assert "Location" in resp.headers
    location = urlparse(resp.headers["Location"])
    assert location.netloc == "auth.globus.org"
    redirect = urlparse(parse_qs(location.query)["state"][0].split(".", 1)[-1])
    assert redirect.path == "/api/v1/integration/zenodo"
    assert parse_qs(redirect.query) == {
        "record_id": ["430905"],
        "resource_server": ["sandbox.zenodo.org"],
        "token": ["{girderToken}"],
        "force": ["False"],
    }

    with httmock.HTTMock(mock_get_record, mock_other_request):
        with mock.patch("girder_wholetale.lib.zenodo.provider.urlopen", fake_urlopen):
            resp = server.request(
                path="/integration/zenodo",
                method="GET",
                user=user,
                params={
                    "record_id": "430905",
                    "resource_server": "sandbox.zenodo.org",
                },
                isJson=False,
            )

    assert "Location" in resp.headers
    location = urlparse(resp.headers["Location"])
    assert location.netloc == "dashboard.wholetale.org"
    tale_id = location.path.rsplit("/")[-1]

    tale = Tale().load(tale_id, user=user)
    assert tale["title"] == "Water Tale"

    with httmock.HTTMock(mock_get_record, mock_other_request):
        with mock.patch("girder_wholetale.lib.zenodo.provider.urlopen", fake_urlopen):
            resp = server.request(
                path="/integration/zenodo",
                method="GET",
                user=user,
                params={
                    "record_id": "430905",
                    "resource_server": "sandbox.zenodo.org",
                },
                isJson=False,
            )
    assert "Location" in resp.headers
    location = urlparse(resp.headers["Location"])
    assert location.netloc == "dashboard.wholetale.org"
    existing_tale_id = location.path.rsplit("/")[-1]
    assert tale_id == existing_tale_id


@pytest.mark.plugin("wholetale")
def test_analyze_in_wt_failures(server, user, fsAssetstore):
    def not_a_zip(url):
        return io.BytesIO(b"blah")

    def no_manifest(url):
        fp = io.BytesIO()
        with zipfile.ZipFile(fp, mode="w") as zf:
            zf.writestr("blah", "blah")
        fp.seek(0)
        return fp

    def malformed_manifest(url):
        fp = io.BytesIO()
        with zipfile.ZipFile(fp, mode="w") as zf:
            zf.writestr("manifest.json", "blah")
        fp.seek(0)
        return fp

    def no_env(url):
        fp = io.BytesIO()
        with zipfile.ZipFile(fp, mode="w") as zf:
            zf.writestr(
                "manifest.json",
                json.dumps(
                    {
                        "@id": "https://data.wholetale.org",
                        "@type": "wt:Tale",
                    }
                ),
            )
        fp.seek(0)
        return fp

    funcs = [not_a_zip, no_manifest, malformed_manifest, no_env]
    errors = [
        "'Provided file is not a zipfile'",
        "'Provided file doesn't contain a Tale manifest'",
        (
            "'Couldn't read manifest.json or not a Tale: "
            "Expecting value: line 1 column 1 (char 0)'"
        ),
        (
            "'Couldn't read environment.json or not a Tale: "
            "'There is no item named None in the archive''"
        ),
    ]

    with httmock.HTTMock(mock_get_record, mock_other_request):
        for func, msg in zip(funcs, errors):
            with mock.patch("girder_wholetale.lib.zenodo.provider.urlopen", func):
                resp = server.request(
                    path="/integration/zenodo",
                    method="GET",
                    user=user,
                    params={
                        "record_id": "430905",
                        "resource_server": "sandbox.zenodo.org",
                    },
                )
                assertStatus(resp, 400)
                assert resp.json == {
                    "type": "rest",
                    "message": "Failed to import Tale. Server returned: " + msg,
                }
