import json
import os

import httmock
import pytest
from bson.objectid import ObjectId
from girder.constants import AccessType
from girder.models.collection import Collection
from girder.models.file import File
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.user import User
from girder.utility.path import lookUpPath
from pytest_girder.assertions import assertStatusOk
from girder_wholetale.models.tale import Tale


def restore_catalog(user, parent, current):
    for folder in current["folders"]:
        folderObj = Folder().createFolder(
            parent,
            folder["name"],
            parentType="folder",
            public=True,
            reuseExisting=True,
        )
        if "meta" in folder:
            Folder().setMetadata(folderObj, folder["meta"])
        restore_catalog(user, folderObj, folder)

    for obj in current["files"]:
        if obj["linkUrl"].startswith("globus"):
            continue
        item = Item().createItem(obj["name"], user, parent, reuseExisting=True)
        Item().setMetadata(item, obj["meta"])

        File().createLinkFile(
            obj["name"],
            item,
            "item",
            obj["linkUrl"],
            user,
            size=obj["size"],
            mimeType=obj["mimeType"],
            reuseExisting=True,
        )


@pytest.fixture
def mock_catalog(admin):
    data_collection = Collection().createCollection(
        "WholeTale Catalog", public=True, reuseExisting=True
    )
    catalog = Folder().createFolder(
        data_collection,
        "WholeTale Catalog",
        parentType="collection",
        public=True,
        reuseExisting=True,
    )
    with open(
        os.path.join(os.path.dirname(__file__), "data", "manifest_mock_catalog.json"),
        "r",
    ) as fp:
        data = json.load(fp)
        restore_catalog({"_id": ObjectId()}, catalog, data)

    yield

    Folder().remove(catalog)
    Collection().remove(data_collection)


@pytest.fixture
def tale_info(mock_catalog, user, admin):
    dataSet = []
    data_paths = [
        "Open Source at Harvard/data",  # Dataverse folder
        (
            "Replication Data for At-Large Elections and Minority "
            "Representation in Local Government/panel_agg.csv"
        ),  # Dataverse file
        "raw.githubusercontent.com/gwosc-tutorial/LOSC_Event_tutorial/master/BBH_events_v3.json",  # HTTP file
        "raw.githubusercontent.com/gwosc-tutorial/LOSC_Event_tutorial",  # HTTP folder
    ]
    root = "/collection/WholeTale Catalog/WholeTale Catalog"
    for path in data_paths:
        obj = lookUpPath(os.path.join(root, path))
        dataSet.append(
            {
                "itemId": obj["document"]["_id"],
                "mountPath": obj["document"]["name"],
                "_modelType": obj["model"],
            }
        )
    new_authors = [
        {
            "firstName": admin["firstName"],
            "lastName": admin["lastName"],
            "orcid": "https://orcid.org/1234",
        },
        {
            "firstName": user["firstName"],
            "lastName": user["lastName"],
            "orcid": "https://orcid.org/9876",
        },
    ]

    tale_info = {
        "name": "Main Tale",
        "description": "Tale Desc",
        "authors": new_authors,
        "creator": user,
        "public": True,
        "data": dataSet,
        "illustration": "linkToImage",
        "imageInfo": {
            "digest": (
                "registry.local.wholetale.org/5c8fe826da39aa00013e9609/1552934951@"
                "sha256:4f604e6fab47f79e28251657347ca20ee89b737b4b1048c18ea5cf2fe9a9f098"
            ),
            "jobId": ObjectId("5c9009deda39aa0001d702b7"),
            "last_build": 1552943449,
            "repo2docker_version": "craigwillis/repo2docker:latest",
        },
    }
    return tale_info


@pytest.fixture
def fancy_tale(server, user, tale_info, image):
    tale = Tale().createTale(
        image,
        tale_info["data"],
        creator=user,
        title=tale_info["name"],
        public=tale_info["public"],
        description=tale_info["description"],
        authors=tale_info["authors"],
    )
    tale = Tale().load(tale["_id"], force=True)  # to get aux dirs
    assert "workspaceId" in tale

    workspace = Folder().load(tale["workspaceId"], force=True)
    nb_file = os.path.join(workspace["fsPath"], "wt_quickstart.ipynb")
    with open(nb_file, "w") as fp:
        fp.write("Some content")

    tale["imageInfo"] = tale_info["imageInfo"]
    tale = Tale().save(tale)
    return Tale().load(tale["_id"], user=user, level=AccessType.WRITE)


@httmock.all_requests
def mockOtherRequests(url, request):
    raise Exception("Unexpected url %s" % str(request.url))


def get_events(server, since, user=None):
    if not user:
        user = User().findOne({"admin": False})

    resp = server.request(
        path="/notification", method="GET", user=user, params={"since": since}
    )
    assertStatusOk(resp)

    return [event for event in resp.json if event["type"] == "wt_event"]


def event_types(events, affected_resources):
    return {
        event["data"]["event"]
        for event in events
        if affected_resources == event["data"]["affectedResourceIds"]
    }


def _compare_tales(restored_tale, original_tale):
    # TODO: icon is a bug
    for key in restored_tale.keys():
        if key in (
            "created",
            "updated",
            "restoredFrom",
            "imageInfo",
            "workspaceId",
            "icon",
        ):
            continue
        try:
            if isinstance(restored_tale[key], ObjectId) or isinstance(
                original_tale[key], ObjectId
            ):
                assert str(restored_tale[key]) == str(original_tale[key])
            else:
                assert restored_tale[key] == original_tale[key]
        except AssertionError:
            import pprint

            pprint.pprint(restored_tale)
            pprint.pprint(original_tale)
            raise


@pytest.fixture
def extra_user(db, admin):
    u = User().createUser(
        "extrauser",
        "password",
        "Extra",
        "User",
        "extra_user@girder.test",
        admin=False,
    )
    yield u


@pytest.fixture()
def image(user):
    from girder_wholetale.models.image import Image

    img = Image().createImage(
        name="test my name",
        creator=user,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="SomeBuildPack",
            user="someUser",
            port=8888,
            urlPath="",
            targetMount="/mnt/whole-tale",
        ),
    )
    yield img
    Image().remove(img)


@pytest.fixture()
def image_two(user):
    from girder_wholetale.models.image import Image

    img = Image().createImage(
        name="test other name",
        creator=user,
        public=True,
        config=dict(
            template="base.tpl",
            buildpack="OtherBuildPack",
            user="someUser",
            port=8888,
            urlPath="",
        ),
    )
    yield img
    Image().remove(img)


@pytest.fixture()
def register_datasets(server, user):
    data_map = [
        {
            "dataId": (
                "https://dataverse.harvard.edu/dataset.xhtml?"
                "persistentId=doi:10.7910/DVN/Q5PV4U"
            ),
            "doi": "doi:10.7910/DVN/Q5PV4U",
            "name": (
                "Replication Data for: Misgovernance and Human Rights: "
                "The Case of Illegal Detention without Intent"
            ),
            "repository": "Dataverse",
            "size": 6_326_512,
            "tale": False,
        },
        {
            "dataId": (
                "https://dataverse.harvard.edu/dataset.xhtml?"
                "persistentId=doi:10.7910/DVN/26721"
            ),
            "doi": "doi:10.7910/DVN/26721",
            "name": (
                "Replication data for: Priming Predispositions "
                "and Changing Policy Positions"
            ),
            "repository": "Dataverse",
            "size": 44_382_520,
            "tale": False,
        },
    ]
    resp = server.request(
        path="/dataset/register",
        method="POST",
        params={"dataMap": json.dumps(data_map)},
        user=user,
    )
    assertStatusOk(resp)


@pytest.fixture
def dataset():
    def _get_dataset(user, i):
        user = User().load(user["_id"], force=True)
        folder = Folder().load(user["myData"][i], force=True)
        return [
            {
                "_modelType": "folder",
                "itemId": str(folder["_id"]),
                "mountPath": folder["name"],
            }
        ]

    return _get_dataset


@pytest.fixture
def tale(server, user, image, dataset):
    from girder_wholetale.models.tale import Tale

    tale = Tale().createTale(
        image,
        dataset(user, 0),
        creator=user,
        save=True,
        title="Some tale with dataset and versions",
        description="Something something...",
        public=False,
        icon="https://icon-picture.com/icon.png",
        config={},
        authors=[
            {
                "firstName": "Kacper",
                "lastName": "Kowalik",
                "orcid": "https://orcid.org/0000-0003-1709-3744",
            }
        ],
        category="science",
    )
    yield tale
