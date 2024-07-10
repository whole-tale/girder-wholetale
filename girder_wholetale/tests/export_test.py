import json
import mock
import pytest

from pytest_girder.assertions import assertStatusOk

import httmock


@httmock.all_requests
def mockOtherRequests(url, request):
    raise Exception("Unexpected url %s" % str(request.url))


@pytest.fixture
def image(admin):
    from girder_wholetale.models.image import Image

    yield Image().createImage(name="test image", creator=admin, public=True)


@pytest.mark.plugin("wholetale")
def test_export_template(server, user, image):
    resp = server.request(
        path="/tale",
        method="POST",
        user=user,
        type="application/json",
        body=json.dumps({"imageId": str(image["_id"]), "dataSet": []}),
    )
    assertStatusOk(resp)
    tale = resp.json

    with mock.patch("girder_wholetale.lib.manifest.ImageBuilder") as mock_builder:
        mock_builder.return_value.container_config.repo2docker_version = (
            "craigwillis/repo2docker:latest"
        )
        mock_builder.return_value.get_tag.return_value = (
            "registry.local.wholetale.org/tale/hash:tag"
        )
        resp = server.request(
            path=f"/tale/{tale['_id']}/manifest",
            method="GET",
            user=user,
        )
        assertStatusOk(resp)
        manifest = resp.json

    from girder_wholetale.lib.exporters.bag import BagTaleExporter

    exporter = BagTaleExporter(user, manifest, {})

    @httmock.urlmatch(
        scheme="https",
        netloc="images.local.wholetale.org",
        path="^/v2/tale/hash/tags/list$",
        method="GET",
    )
    def mockImageFoundResponse(url, request):
        return json.dumps(
            {
                "name": "tale/hash",
                "tags": ["tag"],
            }
        )

    with httmock.HTTMock(mockImageFoundResponse, mockOtherRequests):
        tmpl = exporter.format_run_file(
            {"port": 80, "targetMount": "/srv", "user": "user"},
            "path?param",
            "token",
        )
        assert "jupyter-repo2docker" not in tmpl

    @httmock.urlmatch(
        scheme="https",
        netloc="images.local.wholetale.org",
        path="^/v2/tale/hash/tags/list$",
        method="GET",
    )
    def mockImageNotFoundResponse(url, request):
        return httmock.response(
            status_code=404,
            content=json.dumps(
                {
                    "errors": [
                        {
                            "code": "NAME_UNKNOWN",
                            "message": "repository name not known to registry",
                            "detail": {"name": "tale/hash"},
                        }
                    ]
                }
            ),
        )

    with httmock.HTTMock(mockImageNotFoundResponse, mockOtherRequests):
        tmpl = exporter.format_run_file(
            {"port": 80, "targetMount": "/srv", "user": "user"},
            "path?param",
            "token",
        )
        assert "jupyter-repo2docker" in tmpl
