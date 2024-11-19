import json
import time

import mock
import pytest
import responses
from girder.models.folder import Folder
from girder.models.item import Item
from pytest_girder.assertions import assertStatus, assertStatusOk


@pytest.mark.vcr()
@pytest.mark.plugin("wholetale")
@responses.activate
def test_dataverse_lookup(server, user, fsAssetstore):
    responses.add_passthru("https://dataverse.harvard.edu/api/access")
    responses.add_passthru("https://dataverse.harvard.edu/api/datasets")
    responses.add_passthru("https://dataverse.harvard.edu/dataset.xhtml")
    responses.add_passthru("https://dataverse.harvard.edu/file.xhtml")
    responses.add_passthru("https://dvn-cloud.s3.amazonaws.com/")
    responses.add_passthru(
        "https://dataverse.harvard.edu/api/search?q=filePersistentId"
    )
    responses.add_passthru("https://dataverse.harvard.edu/citation")
    responses.add_passthru("https://doi.org")
    responses.add(
        responses.GET,
        "https://dataverse.harvard.edu/api/search?q=entityId:3040230",
        json={
            "status": "OK",
            "data": {
                "q": "entityId:3040230",
                "total_count": 1,
                "start": 0,
                "spelling_alternatives": {},
                "items": [
                    {
                        "name": "2017-07-31.tab",
                        "type": "file",
                        "url": "https://dataverse.harvard.edu/api/access/datafile/3040230",
                        "file_id": "3040230",
                        "published_at": "2017-07-31T22:27:23Z",
                        "file_type": "Tab-Delimited",
                        "file_content_type": "text/tab-separated-values",
                        "size_in_bytes": 12025,
                        "md5": "e7dd2f725941b978d45fed3f33ff640c",
                        "checksum": {
                            "type": "MD5",
                            "value": "e7dd2f725941b978d45fed3f33ff640c",
                        },
                        "unf": "UNF:6:6wGE3C5ragT8A0qkpGaEaQ==",
                        "dataset_citation": (
                            'Durbin, Philip, 2017, "Open Source at Harvard", '
                            "https://doi.org/10.7910/DVN/TJCLKP, Harvard Dataverse, "
                            " V2, UNF:6:6wGE3C5ragT8A0qkpGaEaQ== [fileUNF]"
                        ),
                    }
                ],
                "count_in_response": 1,
            },
        },
    )

    resp = server.request(
        path="/repository/lookup",
        method="GET",
        user=user,
        params={
            "dataId": json.dumps(
                [
                    "https://doi.org/10.7910/DVN/RLMYMR",
                    "https://doi.org/10.7910/DVN/RLMYMR/WNKD3W",
                    "https://dataverse.harvard.edu/api/access/datafile/3040230",
                ]
            )
        },
    )
    assertStatusOk(resp)
    assert resp.json == [
        {
            "dataId": "https://dataverse.harvard.edu/dataset.xhtml"
            "?persistentId=doi:10.7910/DVN/RLMYMR",
            "doi": "doi:10.7910/DVN/RLMYMR",
            "name": "Karnataka Diet Diversity and Food Security for "
            "Agricultural Biodiversity Assessment",
            "repository": "Dataverse",
            "size": 495885,
            "tale": False,
        },
        {
            "dataId": "https://dataverse.harvard.edu/file.xhtml"
            "?persistentId=doi:10.7910/DVN/RLMYMR/WNKD3W",
            "doi": "doi:10.7910/DVN/RLMYMR",
            "name": "Karnataka Diet Diversity and Food Security for "
            "Agricultural Biodiversity Assessment",
            "repository": "Dataverse",
            "size": 2321,
            "tale": False,
        },
        {
            "dataId": "https://dataverse.harvard.edu/api/access/datafile/3040230",
            "doi": "doi:10.7910/DVN/TJCLKP",
            "name": "Open Source at Harvard",
            "repository": "Dataverse",
            "size": 12025,
            "tale": False,
        },
    ]

    resp = server.request(
        path="/repository/listFiles",
        method="GET",
        user=user,
        params={
            "dataId": json.dumps(
                [
                    "https://doi.org/10.7910/DVN/RLMYMR",
                    "https://doi.org/10.7910/DVN/RLMYMR/WNKD3W",
                    "https://dataverse.harvard.edu/api/access/datafile/3040230",
                ]
            )
        },
    )
    assertStatusOk(resp)
    assert resp.json == [
        {
            "Karnataka Diet Diversity and Food Security for "
            "Agricultural Biodiversity Assessment": {
                "fileList": [
                    {"Karnataka_DDFS_Data-1.tab": {"size": 2408}},
                    {"Karnataka_DDFS_Data-1.xlsx": {"size": 700840}},
                    {"Karnataka_DDFS_Questionnaire.pdf": {"size": 493564}},
                ]
            }
        },
        {
            "Karnataka Diet Diversity and Food Security for "
            "Agricultural Biodiversity Assessment": {
                "fileList": [
                    {"Karnataka_DDFS_Data-1.tab": {"size": 2408}},
                    {"Karnataka_DDFS_Data-1.xlsx": {"size": 700840}},
                ]
            }
        },
        {
            "Open Source at Harvard": {
                "fileList": [
                    {"2017-07-31.csv": {"size": 11684}},
                    {"2017-07-31.tab": {"size": 12100}},
                ]
            }
        },
    ]


@pytest.mark.plugin("wholetale")
def test_config_validators(server, admin):
    from girder_wholetale.constants import PluginSettings, SettingDefault

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.DATAVERSE_URL, "value": "random_string"},
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "field": "value",
        "type": "validation",
        "message": "Invalid Dataverse URL",
    }

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.DATAVERSE_URL,
            "value": SettingDefault.defaults[PluginSettings.DATAVERSE_URL],
        },
    )
    assertStatusOk(resp)

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.DATAVERSE_URL, "value": ""},
    )
    assertStatusOk(resp)
    resp = server.request(
        "/system/setting",
        user=admin,
        method="GET",
        params={"key": PluginSettings.DATAVERSE_URL},
    )
    assertStatusOk(resp)
    assert resp.body[0].decode() == '"{}"'.format(
        SettingDefault.defaults[PluginSettings.DATAVERSE_URL]
    )


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr()
def test_single_dataverse_instance(server, admin, user):
    from girder_wholetale.constants import PluginSettings, SettingDefault

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.DATAVERSE_URL,
            "value": "https://demo.dataverse.org/",
        },
    )
    assertStatusOk(resp)

    resp = server.request(
        path="/repository/lookup",
        method="GET",
        user=user,
        params={
            "dataId": json.dumps(
                [
                    "https://demo.dataverse.org/dataset.xhtml?persistentId=doi:10.70122/FK2/P1TO3S"
                ]
            )
        },
    )
    assertStatusOk(resp)
    assert resp.json == [
        {
            "dataId": "https://demo.dataverse.org/dataset.xhtml?persistentId=doi:10.70122/FK2/P1TO3S",
            "doi": "doi:10.70122/FK2/P1TO3S",
            "name": "title",
            "repository": "Dataverse",
            "size": 493696,
            "tale": False,
        }
    ]

    resp = server.request(
        path="/repository/listFiles",
        method="GET",
        user=user,
        params={
            "dataId": json.dumps(
                [
                    "https://demo.dataverse.org/dataset.xhtml?persistentId=doi:10.70122/FK2/P1TO3S"
                ]
            )
        },
    )
    assertStatusOk(resp)
    assert resp.json == [
        {
            "title": {
                "Project": {
                    "Data": {
                        "fileList": [
                            {
                                "Test Data Alpha.dta": {
                                    "size": 9090,
                                },
                            },
                            {
                                "Test Data Alpha.tab": {
                                    "size": 6586,
                                },
                            },
                            {
                                "Test Data Beta.dta": {
                                    "size": 31175,
                                },
                            },
                            {
                                "Test Data Beta.tab": {
                                    "size": 35663,
                                },
                            },
                        ],
                    },
                    "Images": {
                        "fileList": [
                            {
                                "BarChart.jpg": {
                                    "size": 339679,
                                },
                            },
                            {
                                "ImageChart.png": {
                                    "size": 56083,
                                },
                            },
                        ],
                    },
                    "fileList": [
                        {
                            "Test Document 1.pdf": {
                                "size": 40226,
                            },
                        },
                        {
                            "Test Document 2.docx": {
                                "size": 15873,
                            },
                        },
                    ],
                }
            }
        }
    ]

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.DATAVERSE_URL,
            "value": SettingDefault.defaults[PluginSettings.DATAVERSE_URL],
        },
    )
    assertStatusOk(resp)


@pytest.mark.plugin("wholetale")
def test_extra_hosts(server, admin):
    from girder_wholetale.constants import PluginSettings, SettingDefault

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.DATAVERSE_EXTRA_HOSTS, "value": "dataverse.org"},
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "field": "value",
        "type": "validation",
        "message": "Dataverse extra hosts setting must be a list.",
    }

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.DATAVERSE_EXTRA_HOSTS,
            "value": json.dumps(["not a domain"]),
        },
    )
    assertStatus(resp, 400)
    assert resp.json == {
        "field": "value",
        "type": "validation",
        "message": "Invalid domain in Dataverse extra hosts",
    }

    # defaults
    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={"key": PluginSettings.DATAVERSE_EXTRA_HOSTS, "value": ""},
    )
    assertStatusOk(resp)
    resp = server.request(
        "/system/setting",
        user=admin,
        method="GET",
        params={"key": PluginSettings.DATAVERSE_EXTRA_HOSTS},
    )
    assertStatusOk(resp)
    assert resp.body[0].decode() == str(
        SettingDefault.defaults[PluginSettings.DATAVERSE_EXTRA_HOSTS]
    )

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "list": json.dumps(
                [
                    {
                        "key": PluginSettings.DATAVERSE_EXTRA_HOSTS,
                        "value": ["random.d.org", "random2.d.org"],
                    },
                    {
                        "key": PluginSettings.DATAVERSE_URL,
                        "value": "https://demo.dataverse.org",
                    },
                ]
            )
        },
    )
    assertStatusOk(resp)
    from girder_wholetale.lib.dataverse.provider import DataverseImportProvider

    assert (
        "^https?://(demo.dataverse.org|random.d.org|random2.d.org).*$"
        == DataverseImportProvider().regex[-1].pattern
    )

    resp = server.request(
        "/system/setting",
        user=admin,
        method="PUT",
        params={
            "key": PluginSettings.DATAVERSE_URL,
            "value": SettingDefault.defaults[PluginSettings.DATAVERSE_URL],
        },
    )


# @vcr.use_cassette(os.path.join(DATA_PATH, "dataverse_hierarchy.txt"))
@pytest.mark.plugin("wholetale")
def test_dataverse_dataset_with_hierarchy(server, user):
    from girder_jobs.constants import JobStatus
    from girder_jobs.models.job import Job

    from girder_wholetale.lib.manifest import Manifest
    from girder_wholetale.lib.manifest_parser import ManifestParser
    from girder_wholetale.models.image import Image
    from girder_wholetale.models.tale import Tale

    doi = "doi:10.7910/DVN/Q5PV4U"
    dataMap = [
        {
            "dataId": (
                "https://dataverse.harvard.edu/dataset.xhtml?" "persistentId=" + doi
            ),
            "doi": doi,
            "name": (
                "Replication Data for: Misgovernance and Human Rights: "
                "The Case of Illegal Detention without Intent"
            ),
            "repository": "Dataverse",
            "size": 6326512,
            "tale": False,
        }
    ]

    resp = server.request(
        path="/dataset/register",
        method="POST",
        params={"dataMap": json.dumps(dataMap)},
        user=user,
    )
    assertStatusOk(resp)
    registration_job = resp.json

    for _ in range(100):
        job = Job().load(registration_job["_id"], force=True)
        if job["status"] > JobStatus.RUNNING:
            break
        time.sleep(0.1)
    assert job["status"] == JobStatus.SUCCESS

    ds_root = Folder().findOne({"meta.identifier": doi})
    ds_subfolder = Folder().findOne({"name": "Source Data", "parentId": ds_root["_id"]})
    ds_item = Item().findOne({"name": "03_Analysis_Code.R", "folderId": ds_root["_id"]})

    dataSet = [
        {
            "_modelType": "folder",
            "itemId": str(ds_root["_id"]),
            "mountPath": ds_root["name"],
        },
        {
            "_modelType": "folder",
            "itemId": str(ds_subfolder["_id"]),
            "mountPath": ds_subfolder["name"],
        },
        {
            "_modelType": "item",
            "itemId": str(ds_item["_id"]),
            "mountPath": ds_item["name"],
        },
    ]

    image = Image().createImage(name="test my name", creator=user, public=True)
    tale = Tale().createTale(image, dataSet, creator=user, title="Blah", public=True)
    with mock.patch("girder_wholetale.lib.manifest.ImageBuilder") as mock_builder:
        mock_builder.return_value.container_config.repo2docker_version = (
            "craigwillis/repo2docker:latest"
        )
        mock_builder.return_value.get_tag.return_value = "some_digest"
        manifest = Manifest(tale, user, expand_folders=True).manifest

    restored_dataset = ManifestParser(manifest).get_dataset()
    assert restored_dataset == dataSet

    Tale().remove(tale)
    Image().remove(image)


@pytest.mark.vcr()
def test_dataverse_proto_tale(user):
    from girder_wholetale.lib.data_map import DataMap
    from girder_wholetale.lib.dataverse.provider import DataverseImportProvider

    provider = DataverseImportProvider()

    datamap = {
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
        "size": 44382520,
        "tale": False,
    }
    dataMap = DataMap.fromDict(datamap)

    tale = provider.proto_tale_from_datamap(dataMap, user, False)
    assert set(tale.keys()) == {"title", "relatedIdentifiers", "category"}
    tale = provider.proto_tale_from_datamap(dataMap, user, True)
    assert tale["authors"][0]["lastName"] == "Tesler"

    datamap = {
        "dataId": (
            "http://dataverse.icrisat.org/dataset.xhtml?"
            "persistentId=doi:10.21421/D2/TCCVS7"
        ),
        "doi": "doi:10.21421/D2/TCCVS7",
        "name": (
            "Phenotypic evaluation data of International Chickpea "
            "Varietal Trials (ICVTs) – Desi for Year 2016-17"
        ),
        "repository": "Dataverse",
        "size": 99504,
        "tale": False,
    }
    tale = provider.proto_tale_from_datamap(DataMap.fromDict(datamap), user, True)
    assert tale["authors"][0]["firstName"] == "Pooran"
