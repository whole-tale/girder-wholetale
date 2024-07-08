import json
import pytest

from girder.constants import AccessType
from pytest_girder.assertions import assertStatus, assertStatusOk


class FakeAsyncResult(object):
    def __init__(self):
        self.task_id = "fake_id"

    def get(self):
        return {"image_digest": "registry/image_name@image_hash"}


@pytest.mark.plugin("wholetale")
def test_image_access(server, admin, user):
    # Create a new user image
    resp = server.request(
        path="/image",
        method="POST",
        user=user,
        params={"name": "test user image", "public": True},
    )
    assertStatusOk(resp)
    image_user = resp.json

    # Create a new admin image
    resp = server.request(
        path="/image",
        method="POST",
        user=admin,
        params={"name": "test admin image", "public": True},
    )
    assertStatusOk(resp)
    image_admin = resp.json

    # Retrieve access control list for the newly created image
    resp = server.request(
        path="/image/%s/access" % image_user["_id"], method="GET", user=user
    )
    assertStatusOk(resp)
    access = resp.json
    assert access == {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            }
        ],
        "groups": [],
    }
    assert image_user.get("public")

    # Update the access control list for the image by adding the admin
    # as a second user
    input_access = {
        "users": [
            {
                "login": user["login"],
                "level": AccessType.ADMIN,
                "id": str(user["_id"]),
                "flags": [],
                "name": "%s %s" % (user["firstName"], user["lastName"]),
            },
            {
                "login": admin["login"],
                "level": AccessType.ADMIN,
                "id": str(admin["_id"]),
                "flags": [],
                "name": "%s %s" % (admin["firstName"], admin["lastName"]),
            },
        ],
        "groups": [],
    }

    resp = server.request(
        path="/image/%s/access" % image_user["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_access)},
    )
    assertStatusOk(resp)
    # Check that the returned access control list for the image is as expected
    result_image_access = resp.json["access"]
    expected_image_access = {
        "groups": [],
        "users": [
            {"flags": [], "id": str(user["_id"]), "level": AccessType.ADMIN},
            {"flags": [], "id": str(admin["_id"]), "level": AccessType.ADMIN},
        ],
    }
    assert result_image_access == expected_image_access

    # Update the access control list of the admin image
    resp = server.request(
        path="/image/%s/access" % image_admin["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_access)},
    )
    assertStatus(resp, 403)

    # Check that the access control list was correctly set for the image
    resp = server.request(
        path="/image/%s/access" % image_admin["_id"], method="GET", user=admin
    )
    assertStatusOk(resp)
    access = resp.json
    assert access == {
        "users": [
            {
                "login": admin["login"],
                "level": AccessType.ADMIN,
                "id": str(admin["_id"]),
                "flags": [],
                "name": "%s %s" % (admin["firstName"], admin["lastName"]),
            }
        ],
        "groups": [],
    }

    # Setting the access list with bad json should throw an error
    resp = server.request(
        path="/image/%s/access" % image_user["_id"],
        method="PUT",
        user=user,
        params={"access": "badJSON"},
    )
    assertStatus(resp, 400)

    # Change the access to private
    resp = server.request(
        path="/image/%s/access" % image_user["_id"],
        method="PUT",
        user=user,
        params={"access": json.dumps(input_access), "public": False},
    )
    assertStatusOk(resp)
    resp = server.request(path="/image/%s" % image_user["_id"], method="GET", user=user)
    assertStatusOk(resp)
    assert not resp.json["public"]


@pytest.mark.plugin("wholetale")
def test_image_search(server, user):
    from girder_wholetale.models.image import Image

    images = []
    images.append(
        Image().createImage(
            name="Jupyter One",
            tags=["black"],
            creator=user,
            description="Blah",
            public=False,
        )
    )
    images.append(
        Image().createImage(
            name="Jupyter Two",
            tags=["orange"],
            creator=user,
            description="Blah",
            public=False,
            parent=images[0],
        )
    )
    images.append(
        Image().createImage(
            name="Fortran",
            tags=["black"],
            creator=user,
            description="Blah",
            public=True,
        )
    )

    resp = server.request(
        path="/image", method="GET", user=user, params={"text": "Jupyter"}
    )
    assertStatusOk(resp)
    assert {_["name"] for _ in resp.json} == {"Jupyter One", "Jupyter Two"}

    resp = server.request(
        path="/image", method="GET", user=user, params={"tag": "black"}
    )
    assertStatusOk(resp)
    assert {_["name"] for _ in resp.json} == {"Jupyter One", "Fortran"}

    resp = server.request(
        path="/image",
        method="GET",
        user=user,
        params={"tag": "black", "text": "Fortran"},
    )
    assertStatusOk(resp)
    assert {_["name"] for _ in resp.json} == {"Fortran"}

    resp = server.request(
        path="/image",
        method="GET",
        user=user,
        params={"parentId": str(images[0]["_id"])},
    )
    assertStatusOk(resp)
    assert {_["name"] for _ in resp.json} == {"Jupyter Two"}

    for image in images:
        Image().remove(image)


@pytest.mark.plugin("wholetale")
def test_create_update_image(server, user):
    from girder_wholetale.models.image import Image

    # Create the image
    params = {
        "name": "test user image",
        "iframe": True,
        "public": True,
        "description": "description",
        "icon": "icon",
        "idleTimeout": 1,
    }

    resp = server.request(path="/image", method="POST", user=user, params=params)
    assertStatusOk(resp)
    image = resp.json

    for param in params:
        assert params[param] == image[param]

    # Update the image
    new_params = {
        "name": "new test user image",
        "iframe": False,
        "public": False,
        "description": "new description",
        "icon": "new icon",
        "idleTimeout": 2,
    }

    resp = server.request(
        path="/image/{}".format(str(image["_id"])),
        method="PUT",
        user=user,
        params=new_params,
    )
    assertStatusOk(resp)
    new_image = resp.json

    for param in new_params:
        assert new_params[param] == new_image[param]

    Image().remove(image)
