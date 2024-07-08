#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import json
import os
import urllib.parse

import pytest
from girder.models.assetstore import Assetstore
from girder.models.collection import Collection
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.setting import Setting
from girder.models.upload import Upload
from girder_oauth.settings import PluginSettings as OAuthPluginSettings
from pytest_girder.assertions import assertStatus, assertStatusOk


@pytest.mark.plugin("wholetale")
def testListing(server, admin, fsAssetstore):
    c1 = Collection().createCollection("c1", admin)
    f1 = Folder().createFolder(c1, "f1", parentType="collection")
    i1 = Item().createItem("i1", admin, f1)

    fname = os.path.join(os.path.dirname(__file__), "data", "logo.png")
    size = os.path.getsize(fname)
    with open(fname, "rb") as f:
        Upload().uploadFromFile(f, size, "i1", "item", i1, admin)

    f2 = Folder().createFolder(f1, "f2", parentType="folder")
    i2 = Item().createItem("i2", admin, f2)
    with open(fname, "rb") as f:
        Upload().uploadFromFile(f, size, "i2", "item", i2, admin)

    resp = server.request(
        path="/folder/{_id}/listing".format(**f1), method="GET", user=admin
    )
    assertStatusOk(resp)
    current_dir = resp.json
    assert current_dir["name"] == "/"
    assert len(current_dir["children"]) == 2
    with open(fname, "rb") as f:
        chksum = hashlib.sha512(f.read()).hexdigest()

    host_path = os.path.join(
        Assetstore().getCurrent().get("root"), chksum[0:2], chksum[2:4], chksum
    )

    for child in current_dir["children"]:
        assert child["name"] in {"i1", "f2"}
        if child["name"] == "i1":
            assert child == {
                "children": [],
                "host_path": host_path,
                "name": "i1",
                "type": 1,
            }
        else:
            assert child == {
                "children": [
                    {
                        "children": [],
                        "host_path": host_path,
                        "name": "i2",
                        "type": 1,
                    }
                ],
                "name": "f2",
                "type": 0,
            }


@pytest.mark.plugin("wholetale")
def testHubRoutes(server):
    from girder_wholetale.constants import API_VERSION

    resp = server.request(path="/wholetale", method="GET")
    assertStatusOk(resp)
    assert resp.json["api_version"] == API_VERSION


@pytest.mark.plugin("wholetale")
def testUserSettings(server, user):
    resp = server.request(path="/user/settings", method="GET")
    assertStatus(resp, 401)

    resp = server.request(
        path="/user/settings",
        method="PUT",
        user=user,
        type="application/json",
        body=json.dumps({"key1": 1, "key2": "value2"}),
    )
    assertStatusOk(resp)
    assert resp.json["meta"]["key1"] == 1
    assert resp.json["meta"]["key2"] == "value2"

    resp = server.request(path="/user/settings", method="GET", user=user)
    assertStatusOk(resp)
    assert resp.json == {"key1": 1, "key2": "value2"}

    resp = server.request(
        path="/user/settings",
        method="PUT",
        user=user,
        type="application/json",
        body=json.dumps({"key1": 2, "key2": None}),
    )
    assertStatusOk(resp)
    assert resp.json["meta"]["key1"] == 2
    assert "key2" not in resp.json["meta"]


@pytest.mark.plugin("wholetale")
def testListingResources(server, admin):
    c1 = Collection().createCollection("c1", admin)
    f1 = Folder().createFolder(c1, "f1", parentType="collection")
    f2 = Folder().createFolder(c1, "f2", parentType="collection")
    i1 = Item().createItem("i1", admin, f1)
    i2 = Item().createItem("i2", admin, f1)

    data = {"item": [str(i1["_id"]), str(i2["_id"])]}
    items = []
    for item in (i1, i2):
        resp = server.request(path="/item/{_id}".format(**item), user=admin)
        items.append(resp.json)

    resp = server.request(
        path="/resource",
        method="GET",
        user=admin,
        params={"resources": json.dumps(data)},
    )
    assertStatusOk(resp)
    assert "folder" not in resp.json
    for iel, el in enumerate(resp.json["item"]):
        for key in el:
            if key in ("lowerName",):
                continue
            assert el[key] == items[iel][key]

    data = {
        "item": [str(i1["_id"])],
        "folder": [str(f1["_id"]), str(f2["_id"])],
    }
    folders = []
    for folder in (f1, f2):
        resp = server.request(path="/folder/{_id}".format(**folder), user=admin)
        folders.append(resp.json)

    resp = server.request(
        path="/resource",
        method="GET",
        user=admin,
        params={"resources": json.dumps(data)},
    )
    assertStatusOk(resp)
    assert "item" in resp.json
    for iel, el in enumerate(resp.json["folder"]):
        for key in el:
            if key in ("lowerName", "access"):
                continue
            assert el[key] == folders[iel][key]

    f3 = Folder().createFolder(f1, "f3", parentType="folder")
    Item().createItem("i1", admin, f3)
    Item().createItem("i2", admin, f3)

    resp = server.request(path="/folder/{_id}/dataset".format(**f1), user=admin)
    assertStatusOk(resp)

    assert {_["mountPath"] for _ in resp.json} == {"/i1", "/i2", "/f3"}


@pytest.fixture
def enabledGlobusAuth():
    """
    Side effect: enables Globus auth
    """
    Setting().set(OAuthPluginSettings.PROVIDERS_ENABLED, ["globus"])
    Setting().set(OAuthPluginSettings.GLOBUS_CLIENT_ID, "globus_test_client_id")
    Setting().set(OAuthPluginSettings.GLOBUS_CLIENT_SECRET, "globus_test_client_secret")

    yield

    Setting().set(OAuthPluginSettings.PROVIDERS_ENABLED, [])
    Setting().set(OAuthPluginSettings.GLOBUS_CLIENT_ID, "")
    Setting().set(OAuthPluginSettings.GLOBUS_CLIENT_SECRET, "")


@pytest.mark.plugin("wholetale")
def testSignIn(server, admin, user, enabledGlobusAuth):
    resp = server.request(
        path="/user/sign_in",
        method="GET",
        isJson=False,
        params={"redirect": "https://blah.wholetale.org"},
    )
    assertStatus(resp, 303)
    redirect = urllib.parse.urlparse(resp.headers["Location"])
    assert redirect.netloc == "auth.globus.org"

    resp = server.request(
        path="/user/sign_in",
        method="GET",
        user=user,
        isJson=False,
        params={"redirect": "https://blah.wholetale.org"},
    )
    assertStatus(resp, 303)
    assert resp.headers["Location"] == "https://blah.wholetale.org"


@pytest.mark.plugin("wholetale")
def testAuthorize(server, user):
    # Note: additional instance specific tests in instance_tests
    # Non-instance authorization tests
    resp = server.request(
        path="/user/authorize",
        method="GET",
        isJson=False,
        user=user,
    )
    # Assert 400 "Forward auth request required"
    assertStatus(resp, 400)

    # Non-instance host with valid user
    resp = server.request(
        user=user,
        path="/user/authorize",
        method="GET",
        additionalHeaders=[
            ("X-Forwarded-Host", "docs.wholetale.org"),
            ("X-Forwarded_Uri", "/"),
        ],
        isJson=False,
    )
    assertStatusOk(resp)

    # No user
    resp = server.request(
        path="/user/authorize",
        method="GET",
        additionalHeaders=[
            ("X-Forwarded-Host", "blah.wholetale.org"),
            ("X-Forwarded-Uri", "/"),
        ],
        isJson=False,
    )
    assertStatus(resp, 303)
    # Confirm redirect to https://girder.{domain}/api/v1/user/sign_in
    assert resp.headers["Location"] == (
        "https://girder.wholetale.org/api/v1/"
        "user/sign_in?redirect=https://blah.wholetale.org/"
    )


@pytest.mark.plugin("wholetale")
def testPluginSettings(server, admin, fsAssetstore):
    from girder_wholetale.constants import PluginSettings, SettingDefault

    # setup basic brand info
    core_settings = [
        {
            "key": "core.brand_name",
            "value": SettingDefault.defaults[PluginSettings.DASHBOARD_TITLE],
        },
        {
            "key": "core.banner_color",
            "value": SettingDefault.defaults[PluginSettings.HEADER_COLOR],
        },
    ]
    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"list": json.dumps(core_settings)},
    )
    assertStatusOk(resp)

    # test defaults
    default_settings = {
        PluginSettings.HEADER_COLOR: SettingDefault.defaults[
            PluginSettings.HEADER_COLOR
        ],
        PluginSettings.DASHBOARD_TITLE: SettingDefault.defaults[
            PluginSettings.DASHBOARD_TITLE
        ],
        PluginSettings.CATALOG_LINK_TITLE: SettingDefault.defaults[
            PluginSettings.CATALOG_LINK_TITLE
        ],
        PluginSettings.DASHBOARD_LINK_TITLE: SettingDefault.defaults[
            PluginSettings.DASHBOARD_LINK_TITLE
        ],
        PluginSettings.DASHBOARD_URL: "https://dashboard.wholetale.org",
        PluginSettings.ENABLE_DATA_CATALOG: SettingDefault.defaults[
            PluginSettings.ENABLE_DATA_CATALOG
        ],
        PluginSettings.LOGO: "",
        PluginSettings.WEBSITE_URL: SettingDefault.defaults[PluginSettings.WEBSITE_URL],
        PluginSettings.ABOUT_HREF: SettingDefault.defaults[PluginSettings.ABOUT_HREF],
        PluginSettings.CONTACT_HREF: SettingDefault.defaults[
            PluginSettings.CONTACT_HREF
        ],
        PluginSettings.BUG_HREF: SettingDefault.defaults[PluginSettings.BUG_HREF],
    }

    resp = server.request("/wholetale/settings", user=admin, method="GET")
    assertStatusOk(resp)
    assert resp.json == default_settings

    # test validation
    test_settings = {
        PluginSettings.WEBSITE_URL: ("not_a_url", "Invalid  URL"),
        PluginSettings.DASHBOARD_LINK_TITLE: (1, "The setting is not a string"),
        PluginSettings.CATALOG_LINK_TITLE: (1, "The setting is not a string"),
        PluginSettings.ENABLE_DATA_CATALOG: (
            "not_a_boolean",
            "The setting is not a boolean",
        ),
    }

    for key, value in test_settings.items():
        resp = server.request(
            "/system/setting",
            user=admin,
            method="PUT",
            params={"key": key, "value": value[0]},
        )
        assertStatus(resp, 400)
        assert resp.json == {
            "field": "value",
            "type": "validation",
            "message": value[1],
        }

    # test set default settings
    for key in test_settings.keys():
        resp = server.request(
            "/system/setting",
            user=admin,
            method="PUT",
            params={"key": key, "value": ""},
        )
        assertStatusOk(resp)

    resp = server.request("/wholetale/settings", user=admin, method="GET")
    assertStatusOk(resp)

    # test logo
    col = Collection().createCollection(
        "WholeTale Assets", admin, public=False, reuseExisting=True
    )
    folder = Folder().createFolder(
        col, "Logo", parentType="collection", public=True, reuseExisting=True
    )
    item = Item().createItem("logo.png", admin, folder)

    fname = os.path.join(os.path.dirname(__file__), "data", "logo.png")
    size = os.path.getsize(fname)

    with open(fname, "rb") as f:
        Upload().uploadFromFile(f, size, "logo.png", "item", item, admin)

    resp = server.request(
        "/resource/lookup",
        user=admin,
        method="GET",
        params={"path": "/collection/WholeTale Assets/Logo/logo.png/logo.png"},
    )
    assertStatusOk(resp)
    logoId = resp.json["_id"]

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.LOGO, "value": logoId},
    )
    assertStatusOk(resp)

    resp = server.request("/wholetale/settings", user=admin, method="GET")
    logoPath = resp.json["wholetale.logo"]
    assert logoPath == f"file/{logoId}/download?contentDisposition=inline"

    resp = server.request("/wholetale/assets", user=admin, method="GET")
    logoAssetFolderId = resp.json["wholetale.logo"]
    assert logoAssetFolderId == str(folder["_id"])
