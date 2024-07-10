#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib.parse import urlparse, parse_qs
import pytest

from girder.models.setting import Setting
from pytest_girder.assertions import assertStatus


#@vcr.use_cassette(os.path.join(DATA_PATH, "dataverse_integration.txt"))
@pytest.mark.plugin("wholetale")
def test_dataverse_integration(server, user):
    error_handling_cases = [
        (
            {"fileId": "1234", "siteUrl": "definitely not a URL"},
            "Not a valid URL: siteUrl",
        ),
        ({"siteUrl": "https://dataverse.someplace"}, "No data Id provided"),
        (
            {"fileId": "not_a_number", "siteUrl": "https://dataverse.someplace"},
            "Invalid fileId (should be integer)",
        ),
        (
            {"datasetId": "not_a_number", "siteUrl": "https://dataverse.someplace"},
            "Invalid datasetId (should be integer)",
        ),
    ]

    for params, errmsg in error_handling_cases:
        resp = server.request(
            "/integration/dataverse", method="GET", params=params, user=user
        )
        assertStatus(resp, 400)
        assert resp.json == {"message": errmsg, "type": "rest"}

    def dv_dataset(flag):
        uri = "https://dataverse.harvard.edu"
        if flag == "dataset_pid":
            uri += "/dataset.xhtml?persistentId=doi:10.7910/DVN/TJCLKP"
        elif flag == "datafile":
            uri += "/api/access/datafile/3371438"
        elif flag == "datafile_pid":
            uri += "/file.xhtml?persistentId=doi:10.7910/DVN/TJCLKP/3VSTKY"
        elif flag == "dataset_id":
            uri += "/api/datasets/3035124"

        return {
            "uri": [uri],
            "name": ["Open Source at Harvard"],
            "asTale": ["True"],
        }

    valid_cases = [
        #(
        #    {"fileId": "3371438", "siteUrl": "https://dataverse.harvard.edu"},
        #    dv_dataset("dataset_pid"),
        #),
        #(
        #    {
        #        "fileId": "3371438",
        #        "siteUrl": "https://dataverse.harvard.edu",
        #        "fullDataset": False,
        #    },
        #    dv_dataset("datafile"),
        #),
        (
            {
                "filePid": "doi:10.7910/DVN/TJCLKP/3VSTKY",
                "siteUrl": "https://dataverse.harvard.edu",
                "fullDataset": False,
            },
            dv_dataset("datafile_pid"),
        ),
        (
            {
                "filePid": "doi:10.7910/DVN/TJCLKP/3VSTKY",
                "siteUrl": "https://dataverse.harvard.edu",
                "fullDataset": True,
            },
            dv_dataset("dataset_pid"),
        ),
        (
            {
                "datasetPid": "doi:10.7910/DVN/TJCLKP",
                "siteUrl": "https://dataverse.harvard.edu",
                "fullDataset": False,
            },
            dv_dataset("dataset_pid"),
        ),
        (
            {
                "datasetId": "3035124",
                "siteUrl": "https://dataverse.harvard.edu",
                "fullDataset": False,
            },
            dv_dataset("dataset_pid"),
        ),
    ]

    for params, response in valid_cases:
        resp = server.request(
            "/integration/dataverse", method="GET", params=params, user=user
        )
        assertStatus(resp, 303)
        assert parse_qs(urlparse(resp.headers["Location"]).query) == response


def _testAutoLogin(self):
    from girder.plugins.oauth.constants import PluginSettings as OAuthSettings

    Setting().set(OAuthSettings.PROVIDERS_ENABLED, ["globus"])
    Setting().set(OAuthSettings.GLOBUS_CLIENT_ID, "client_id")
    Setting().set(OAuthSettings.GLOBUS_CLIENT_SECRET, "secret_id")

    resp = server.request(
        "/integration/dataverse",
        method="GET",
        params={"fileId": "3371438", "siteUrl": "https://dataverse.harvard.edu"},
        isJson=False,
    )
    self.assertStatus(resp, 303)
    query = parse_qs(urlparse(resp.headers["Location"]).query)
    self.assertIn("state", query)
    redirect = query["state"][0].split(".", 1)[-1]
    query = parse_qs(urlparse(redirect).query)
    self.assertEqual(query["fileId"][0], "3371438")
    self.assertEqual(query["force"][0], "False")
    self.assertEqual(query["siteUrl"][0], "https://dataverse.harvard.edu")

def _testSingletonDataverse(self):
    from girder.plugins.wholetale.models.tale import Tale
    from bson import ObjectId

    tale = Tale().createTale(
        {"_id": ObjectId()},
        [],
        creator=user,
        title="Some Tale",
        relatedIdentifiers=[
            {"identifier": "doi:10.7910/DVN/TJCLKP", "relation": "IsDerivedFrom"}
        ],
    )

    resp = server.request(
        "/integration/dataverse",
        method="GET",
        params={
            "datasetId": "3035124",
            "siteUrl": "https://dataverse.harvard.edu",
            "fullDataset": False,
        },
        user=user,
        isJson=False,
    )
    self.assertStatus(resp, 303)
    self.assertEqual(
        urlparse(resp.headers["Location"]).path, "/run/{}".format(tale["_id"])
    )
    Tale().remove(tale)
