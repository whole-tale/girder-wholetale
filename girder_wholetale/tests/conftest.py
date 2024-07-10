import json

import pytest
from bson.objectid import ObjectId
from girder.models.folder import Folder
from girder.models.user import User
from pytest_girder.assertions import assertStatusOk


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
