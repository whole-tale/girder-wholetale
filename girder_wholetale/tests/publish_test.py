import json

import mock
import pytest
from girder.models.user import User
from pytest_girder.assertions import assertStatus, assertStatusOk

from girder_wholetale.constants import PluginSettings, SettingDefault
from girder_wholetale.models.tale import Tale


@pytest.fixture
def mock_builder(mocker):
    mock_builder = mocker.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = (
        "images.local.wholetale.org/digest123"
    )
    return mock_builder


@pytest.fixture
def simple_tale(image, user):
    tale = Tale().createTale(
        image,
        [],
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
    Tale().remove(tale)


class FakeJob:
    job = {}

    def delay(self, *args, **kwargs):
        return self.job


@pytest.mark.plugin("wholetale")
def test_config_validators(server, admin):
    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.PUBLISHER_REPOS, "value": "random_string"},
    )
    assertStatus(resp, 400)
    assert resp.json["message"].startswith("Invalid Repository to Auth Provider map")

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.PUBLISHER_REPOS,
            "value": json.dumps(
                SettingDefault.defaults[PluginSettings.PUBLISHER_REPOS]
            ),
        },
    )
    assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def test_publish_zenodo(server, user, simple_tale, mock_builder):
    tale = simple_tale
    with mock.patch("gwvolman.tasks.publish.apply_async"), mock.patch(
        "gwvolman.tasks.publish.delay"
    ) as dl:
        dl.return_value = FakeJob()

        repository = "sandbox.zenodo.org"
        resp = server.request(
            path="/tale/{_id}/publish".format(**tale),
            method="PUT",
            user=user,
            params={"repository": repository},
        )
        assertStatus(resp, 400)
        assert resp.json["message"] == "Missing a token for publisher (zenodo)."

        token = {
            "access_token": "zenodo_key",
            "provider": "zenodod",
            "resource_server": "sandbox.zenodo.org",
            "token_type": "apikey",
        }
        user["otherTokens"] = [token]
        user = User().save(user)

        resp = server.request(
            path="/tale/{_id}/publish".format(**tale),
            method="PUT",
            user=user,
            params={"repository": repository},
        )
        assertStatusOk(resp)

        job_kwargs = dl.call_args_list[-1][1]
        job_args = dl.call_args_list[-1][0]
        assert job_args[0] == str(tale["_id"])
        assert job_args[1] == token
        assert job_args[2] is not None
        assert job_kwargs["repository"] == repository
