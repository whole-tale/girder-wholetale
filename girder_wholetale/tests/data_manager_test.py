#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import time

import cherrypy
import pytest
from bson import ObjectId
from girder.models.assetstore import Assetstore
from girder.models.collection import Collection
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.setting import Setting
from pytest_girder.assertions import assertStatus, assertStatusOk
from pytest_girder.utils import getResponseBody

from girder_wholetale.models.lock import Lock
from girder_wholetale.models.session import Session
from girder_wholetale.models.tale import Tale
from girder_wholetale.models.transfer import Transfer

from .httpserver import Server

MB = 1024 * 1024


def createStructure(user, tmp_path_factory, prefix):
    tmpdir = tmp_path_factory.mktemp("data")
    collection = Collection().createCollection(
        f"{prefix}_wt_dm_test_col", creator=user, public=False, reuseExisting=True
    )
    folder = Folder().createFolder(
        collection, f"{prefix}_wt_dm_test_fldr", parentType="collection"
    )
    files = [createFile("%s_%s" % (prefix, n), 1 * MB, tmpdir) for n in range(1, 5)]
    Assetstore().importData(
        Assetstore().getCurrent(),
        folder,
        "folder",
        {"importPath": tmpdir},
        {},
        user,
        leafFoldersAsItems=False,
    )
    gfiles = [Item().findOne({"name": file}) for file in files]

    return (collection, folder, files, gfiles)


@pytest.fixture
def structure(server, user, tmp_path_factory, fsAssetstore):
    yield createStructure(user, tmp_path_factory, "test_")


@pytest.fixture
def structure2(server, user, tmp_path_factory, fsAssetstore):
    yield createStructure(user, tmp_path_factory, "test2_")


def createHttpFile(server, testServer, user, folder):
    params = {
        "parentType": "folder",
        "parentId": folder["_id"],
        "name": "httpitem1",
        "linkUrl": testServer.getUrl() + "/1M",
        "size": MB,
    }
    resp = server.request(path="/file", method="POST", user=user, params=params)
    assertStatusOk(resp)
    return Item().load(resp.json["itemId"], user=user)


def createFile(suffix, size, dirpath):
    name = "file" + str(suffix)
    with open(dirpath / name, "wb") as f:
        for i in range(size):
            f.write(b"\0")
    return name


def makeDataSet(items, objectids=True):
    if objectids:
        return [
            {"itemId": f["_id"], "mountPath": "/" + f["name"], "_modelType": "item"}
            for f in items
        ]
    else:
        return [
            {
                "itemId": str(f["_id"]),
                "mountPath": "/" + f["name"],
                "_modelType": "item",
            }
            for f in items
        ]


def _downloadFile(lock, item):
    stream = Lock().downloadItem(lock)
    sz = 0
    for chunk in stream():
        sz += len(chunk)
    assert sz == item["size"]


def waitForFile(server, user, item, rest=False, sessionId=None):
    max_iters = 300
    while max_iters > 0:
        if "cached" in item["dm"] and item["dm"]["cached"]:
            assert "psPath" in item["dm"]
            psPath = item["dm"]["psPath"]
            assert psPath is not None
            return psPath
        time.sleep(0.1)
        max_iters -= 1
        if rest:
            resp = server.request(path=f"/item/{item['_id']}", method="GET", user=user)
            assertStatusOk(resp)
            item = resp.json
        else:
            item = Item().load(item["_id"], user=user)
    raise ValueError("No file found after about 30s")


def _testItem(server, dataSet, item, user, tfiles, download=False):
    session = Session().createSession(user, dataSet=dataSet)
    _testItemWithSession(server, session, item, user, tfiles, download=download)
    Session().deleteSession(user, session)


def _testItemWithSession(server, session, item, user, transferredFiles, download=False):
    assert session is not None
    lock = Lock().acquireLock(user, session["_id"], item["_id"])

    locks = list(Lock().listLocks(user, session["_id"]))
    assert len(locks) == 1

    assert lock is not None

    item = Item().load(item["_id"], user=user)
    assert "dm" in item

    psPath = waitForFile(server, user, item)
    transferredFiles.add(psPath)

    transfers = Transfer().list(user, discardOld=False)
    transfers = list(transfers)
    assert len(transfers) == len(transferredFiles)

    if download:
        _downloadFile(lock, item)

    assert os.path.isfile(psPath)
    assert os.path.getsize(psPath) == item["size"]

    Lock().releaseLock(user, lock)

    item = Item().load(item["_id"], user=user)
    assert item["dm"]["lockCount"] == 0


@pytest.fixture
def httpServer():
    testServer = Server()
    testServer.start()
    yield testServer
    testServer.stop()


@pytest.fixture
def tfiles():
    return set()


@pytest.mark.plugin("wholetale")
def test01LocalFile(server, user, structure, tfiles):
    collection, folder, files, gfiles = structure
    dataSet = makeDataSet(gfiles)
    _testItem(server, dataSet, gfiles[0], user, tfiles, download=True)


@pytest.mark.plugin("wholetale")
def test02HttpFile(server, user, httpServer, structure, tfiles):
    collection, folder, files, gfiles = structure
    httpItem = createHttpFile(server, httpServer, user, folder)
    dataSet = makeDataSet([httpItem])
    _testItem(server, dataSet, httpItem, user, tfiles)
    Item().remove(httpItem)


@pytest.mark.plugin("wholetale")
def test03Caching(server, user, structure, tfiles):
    collection, folder, files, gfiles = structure
    dataSet = makeDataSet(gfiles)
    _testItem(server, dataSet, gfiles[0], user, tfiles)
    _testItem(server, dataSet, gfiles[0], user, tfiles)
    item = Item().load(gfiles[0]["_id"], user=user)
    assert item["dm"]["downloadCount"] == 1
    _testItem(server, dataSet, gfiles[1], user, tfiles)


@pytest.mark.plugin("wholetale")
def test04SessionApi(server, admin, user, extra_user, structure, structure2, tfiles):
    collection, folder, files, gfiles = structure
    collection2, folder2, files2, gfiles2 = structure2
    dataSet = makeDataSet(gfiles)
    item = gfiles[0]
    resp = server.request(
        path="/dm/session",
        method="POST",
        user=user,
        params={"dataSet": json.dumps(dataSet, default=str)},
    )
    assertStatusOk(resp)
    session = resp.json

    sessions = list(Session().list(user))
    assert len(sessions) == 1

    _testItemWithSession(server, session, item, user, tfiles)

    resp = server.request(
        path="/dm/session/{_id}/object".format(**session),
        method="GET",
        user=user,
        params={"path": "/non_existent_path"},
    )
    assertStatus(resp, 400)

    resp = server.request(
        path="/dm/session/{_id}/object".format(**session),
        method="GET",
        user=user,
        params={"path": "/filetest__4"},
    )
    assertStatusOk(resp)
    assert resp.json["object"]["_id"] == str(gfiles[3]["_id"])

    dataSet.append(
        {
            "itemId": str(folder2["_id"]),
            "mountPath": "/" + folder2["name"],
            "_modelType": "folder",
        }
    )
    dataSet = [
        {
            "itemId": str(_["itemId"]),
            "mountPath": _["mountPath"],
            "_modelType": _["_modelType"],
        }
        for _ in dataSet
    ]
    resp = server.request(
        path="/dm/session/{_id}".format(**session),
        method="PUT",
        user=user,
        params={"dataSet": json.dumps(dataSet)},
    )
    assertStatusOk(resp)
    session = resp.json
    assert session["seq"] == 1

    resp = server.request(
        path="/dm/session/{_id}/object".format(**session),
        method="GET",
        user=user,
        params={"path": "/" + folder2["name"], "children": True},
    )
    assertStatusOk(resp)
    children = resp.json["children"]
    leafFile = next((_ for _ in children if _["name"] == gfiles2[-1]["name"]), None)
    assert leafFile["_id"] == str(gfiles2[-1]["_id"])

    resp = server.request(
        path="/dm/session/{_id}/object".format(**session),
        method="GET",
        user=user,
        params={
            "path": "/" + folder2["name"] + "/" + leafFile["name"] + "_blah",
            "children": True,
        },
    )
    assertStatus(resp, 400)

    resp = server.request(
        path="/dm/session/{_id}/object".format(**session),
        method="GET",
        user=user,
        params={
            "path": "/" + folder2["name"] + "/" + leafFile["name"],
            "children": True,
        },
    )
    assertStatusOk(resp)

    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="DELETE", user=extra_user
    )
    assertStatus(resp, 403)

    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="DELETE", user=admin
    )
    assertStatusOk(resp)

    tale = Tale().createTale({"_id": ObjectId()}, dataSet, title="blah", creator=user)

    resp = server.request(
        path="/dm/session",
        method="POST",
        user=user,
        params={"taleId": str(tale["_id"])},
    )
    assertStatusOk(resp)
    session = resp.json
    assert session["dataSet"] == dataSet
    Tale().remove(tale)  # TODO: This should fail, since the session is up
    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="DELETE", user=user
    )
    assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def test05SessionDeleteById(server, user, structure):
    collection, folder, files, gfiles = structure
    dataSet = makeDataSet(gfiles)
    resp = server.request(
        path="/dm/session",
        method="POST",
        user=user,
        params={"dataSet": json.dumps(dataSet, default=str)},
    )
    assertStatusOk(resp)
    session = resp.json
    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="DELETE", user=user
    )
    assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def test06resources(server, user, structure, tfiles):
    collection, folder, files, gfiles = structure
    dataSet = makeDataSet(gfiles, objectids=False)

    resp = server.request(
        "/dm/session",
        method="POST",
        user=user,
        params={"dataSet": json.dumps(dataSet, default=str)},
    )
    assertStatusOk(resp)
    session = resp.json
    sessionId = str(session["_id"])

    # list sessions
    resp = server.request("/dm/session", method="GET", user=user)
    assertStatusOk(resp)

    # get session
    resp = server.request(
        f"/dm/session/{session['_id']}",
        method="GET",
        user=user,
        params={"loadObjects": "true"},
    )
    assertStatusOk(resp)
    assert sessionId == str(resp.json["_id"])

    item = gfiles[0]

    # This coverage business, as implemented, is wrong really. Both branches of
    # a condition should be tested, including a failing condition with no else block.
    resp = server.request(
        "/dm/lock",
        method="POST",
        user=user,
        params={
            "sessionId": sessionId,
            "itemId": str(item["_id"]),
            "ownerId": str(user["_id"]),
        },
    )
    assertStatusOk(resp)
    lockId = resp.json["_id"]

    resp = server.request(
        "/dm/lock", method="GET", user=user, params={"sessionId": sessionId}
    )
    assertStatusOk(resp)
    locks = resp.json
    assert len(locks) == 1

    # test list locks with params
    resp = server.request(
        "/dm/lock",
        method="GET",
        user=user,
        params={
            "sessionId": sessionId,
            "itemId": str(item["_id"]),
            "ownerId": str(user["_id"]),
        },
    )
    assertStatusOk(resp)

    # test list locks for session
    resp = server.request("/dm/session/%s/lock" % sessionId, method="GET", user=user)
    assertStatusOk(resp)

    # test get lock
    resp = server.request("/dm/lock/%s" % lockId, method="GET", user=user)
    assertStatusOk(resp)
    assert lockId == str(resp.json["_id"])

    item = reloadItemRest(server, user, item)
    assert "dm" in item

    psPath = waitForFile(server, user, item, rest=True, sessionId=sessionId)
    shouldHaveBeenTransferred = psPath in tfiles
    tfiles.add(psPath)

    resp = server.request(
        "/dm/transfer",
        method="GET",
        user=user,
        params={"sessionId": sessionId, "discardOld": "false"},
    )
    assertStatusOk(resp)
    transfers = resp.json
    assert len(transfers) == len(tfiles)

    # test list transfers for session
    resp = server.request(
        "/dm/session/%s/transfer" % sessionId, method="GET", user=user
    )
    assertStatusOk(resp)
    transfers = resp.json
    if shouldHaveBeenTransferred:
        assert len(transfers) == 1
    else:
        assert len(transfers) == 0

    assert os.path.isfile(psPath)
    assert os.path.getsize(psPath) == item["size"]

    resp = server.request(
        "/dm/lock/%s/download" % lockId, method="GET", user=user, isJson=False
    )
    assertStatusOk(resp)
    body = getResponseBody(resp)
    assert len(body) == item["size"]

    resp = server.request("/dm/lock/%s" % lockId, method="DELETE", user=user)
    assertStatusOk(resp)

    item = reloadItemRest(server, user, item)
    assert item["dm"]["lockCount"] == 0

    resp = server.request("/dm/session/%s" % sessionId, method="DELETE", user=user)
    assertStatusOk(resp)


def reloadItemRest(server, user, item):
    resp = server.request("/item/{_id}".format(**item), method="GET", user=user)
    assertStatusOk(resp)
    return resp.json


@pytest.mark.plugin("wholetale")
def test07FileGC(server, user, structure, tfiles):
    def _getCachedItems():
        return list(Item().find({"dm.cached": True}, user=user))

    apiroot = cherrypy.tree.apps["/api"].root.v1
    gc = apiroot.dm.getFileGC()
    gc.pause()

    collection, folder, files, gfiles = structure
    dataSet = makeDataSet(gfiles)
    _testItem(server, dataSet, gfiles[0], user, tfiles)
    _testItem(server, dataSet, gfiles[1], user, tfiles)

    cachedItems = _getCachedItems()
    assert len(cachedItems) == 2

    files = [x["dm"]["psPath"] for x in cachedItems]

    Setting().set("dm.private_storage_capacity", int(2.2 * MB))
    Setting().set("dm.gc_collect_start_fraction", 0.5)  # if over 1.1 MB
    Setting().set("dm.gc_collect_end_fraction", 0.5)  # if under 1.1 MB
    gc._collect()
    # should have cleaned one file
    remainingCount = 0
    for f in files:
        if os.path.exists(f):
            remainingCount += 1

    assert remainingCount == 1
    assert len(_getCachedItems()) == 1
    gc.resume()


@pytest.mark.plugin("wholetale")
def test08StructureAccess(server, user, structure, structure2):
    # mount the root collection and try to lock files
    _, folder, _, gfiles = structure
    _, _, _, gfiles2 = structure2
    dataSet = makeDataSet([{"_id": folder["_id"], "name": "fldr"}], objectids=False)

    resp = server.request(
        "/dm/session",
        method="POST",
        user=user,
        params={"dataSet": json.dumps(dataSet, default=str)},
    )
    assertStatusOk(resp)
    sessionId = resp.json["_id"]

    item = gfiles[0]

    resp = server.request(
        "/dm/lock",
        method="POST",
        user=user,
        params={
            "sessionId": sessionId,
            "itemId": str(item["_id"]),
            "ownerId": str(user["_id"]),
        },
    )
    assertStatusOk(resp)
    lockId = resp.json["_id"]

    resp = server.request("/dm/lock/%s" % lockId, method="DELETE", user=user)
    assertStatusOk(resp)

    item = reloadItemRest(server, user, item)
    assert item["dm"]["lockCount"] == 0

    item2 = gfiles2[0]

    resp = server.request(
        "/dm/lock",
        method="POST",
        user=user,
        params={
            "sessionId": sessionId,
            "itemId": str(item2["_id"]),
            "ownerId": str(user["_id"]),
        },
    )
    # not in the collection
    assertStatus(resp, 404)


@pytest.mark.plugin("wholetale")
def test09TaleUpdateEventHandler(server, user, structure):
    collection, folder, files, gfiles = structure
    dataSet = makeDataSet([{"_id": folder["_id"], "name": "fldr"}], objectids=False)
    dataSet[0]["_modelType"] = "folder"

    tale = Tale().createTale({"_id": ObjectId()}, dataSet, title="test09", creator=user)

    resp = server.request(
        path="/dm/session",
        method="POST",
        user=user,
        params={"taleId": str(tale["_id"])},
    )
    assertStatusOk(resp)
    session = resp.json
    assert session["dataSet"] == dataSet

    tale["dataSet"].pop(0)
    tale = Tale().save(tale)
    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="GET", user=user
    )
    assertStatusOk(resp)
    session = resp.json
    assert session["dataSet"] == tale["dataSet"]

    Tale().remove(tale)  # TODO: This should fail, since the session is up
    resp = server.request(
        path="/dm/session/{_id}".format(**session), method="DELETE", user=user
    )
    assertStatusOk(resp)
