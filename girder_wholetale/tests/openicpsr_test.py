import json
import os
import time

import httmock
from bson import ObjectId
from girder.models.user import User

DATA_PATH = os.path.join(os.path.dirname(__file__), "data")


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
    fname = os.path.join(DATA_PATH, "5c92fbd472a9910001fbff72.zip")
    return open(fname, "rb")


def _testLookup(self):
    resolved_lookup = {
        "dataId": "https://www.openicpsr.org/openicpsr/project/132081/version/V1/view",
        "doi": "doi:10.3886/E132081V1",
        "name": (
            "Data and Code for: Intrahousehold Consumption Allocation and Demand for Agency: "
            "A Triple Experimental Investigation"
        ),
        "repository": "OpenICPSR",
        "size": -1,
        "tale": False,
    }
    resp = self.request(
        path="/repository/lookup",
        method="GET",
        user=self.user,
        params={"dataId": json.dumps(["doi:10.3886/E132081V1"])},
    )
    self.assertStatusOk(resp)
    self.assertEqual(resp.json, [resolved_lookup])


def _test_setting_password(self):
    resp = self.request(
        path="/account/icpsr/key",
        method="POST",
        user=self.user,
        params={
            "resource_server": "www.openicpsr.org",
            "key": "definitely_not_a_password",
            "key_type": "apikey",
        },
    )
    self.assertStatus(resp, 400)
    self.assertEqual(
        resp.json, {"type": "rest", "message": "Invalid key/password for icpsr"}
    )

    resp = self.request(
        path="/account/icpsr/key",
        method="POST",
        user=self.admin,
        params={
            "resource_server": "www.openicpsr.org",
            "key": "realPassGoHere",
            "key_type": "apikey",
        },
    )
    self.assertStatusOk(resp)


def _test_import_binder(self):
    from girder.plugins.jobs.constants import JobStatus
    from girder.plugins.jobs.models.job import Job
    from girder.plugins.wholetale.models.tale import Tale

    resp = self.request(
        path="/tale/import",
        method="POST",
        user=self.admin,
        params={
            "git": False,
            "url": "https://www.openicpsr.org/openicpsr/project/132081/version/V1/view",
            "spawn": False,
            "imageId": str(self.image["_id"]),
        },
    )
    self.assertStatus(resp, 400)
    self.assertEqual(
        resp.json,
        {
            "type": "rest",
            "message": "To register data from OpenICPSR you need to provide credentials.",
        },
    )

    self.admin["otherTokens"] = [
        {
            "access_token": "5E0B060046B720165F1C3C92A7C50E1E",
            "provider": "icpsr",
            "resource_server": "www.openicpsr.org",
            "token_type": "apikey",
        }
    ]
    self.admin = User().save(self.admin)
    resp = self.request(
        path="/tale/import",
        method="POST",
        user=self.admin,
        params={
            "git": False,
            "url": "https://www.openicpsr.org/openicpsr/project/132081/version/V1/view",
            "spawn": False,
            "imageId": str(self.image["_id"]),
        },
    )
    self.assertStatusOk(resp)
    tale = resp.json
    job = Job().findOne(
        {"type": "wholetale.import_binder", "taleId": ObjectId(tale["_id"])}
    )
    for _ in range(600):
        if job["status"] in {JobStatus.SUCCESS, JobStatus.ERROR}:
            break
        time.sleep(0.1)
        job = Job().load(job["_id"], force=True)
    self.assertEqual(job["status"], JobStatus.SUCCESS)

    Tale().remove(tale)
