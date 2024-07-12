import copy
import json
import os
from operator import itemgetter

import pytest
from bson import ObjectId
from girder.constants import AccessType
from girder.exceptions import AccessException, ValidationException
from girder.models.collection import Collection
from girder.models.file import File
from girder.models.folder import Folder
from girder.models.item import Item
from girder.utility.path import lookUpPath

from girder_wholetale import WholeTalePlugin
from girder_wholetale.lib.manifest import Manifest, get_folder_identifier
from girder_wholetale.lib.manifest_parser import ManifestParser
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
def tale_one(user, tale_info, image):
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

    tale["imageInfo"] = tale_info["imageInfo"]
    tale = Tale().save(tale)
    return Tale().load(tale["_id"], user=user, level=AccessType.WRITE)


@pytest.fixture
def tale_two(user, tale_info, image):
    tale = Tale().createTale(
        image,
        [],
        creator=tale_info["creator"],
        title=tale_info["name"],
        public=tale_info["public"],
        description=tale_info["description"],
        authors=tale_info["authors"],
    )
    return Tale().load(
        tale["_id"], user=user, level=AccessType.WRITE
    )  # to get aux dirs


@pytest.fixture
def mock_builder(tale_info, mocker):
    mock_builder = mocker.patch("girder_wholetale.lib.manifest.ImageBuilder")
    mock_builder.return_value.container_config.repo2docker_version = (
        "craigwillis/repo2docker:latest"
    )
    mock_builder.return_value.get_tag.return_value = tale_info["imageInfo"][
        "digest"
    ].replace("registry", "images", 1)
    return mock_builder


@pytest.mark.plugin("wholetale", WholeTalePlugin)
def test_create_basic_attributes(server, user, image, tale_one, mock_builder):
    # Test that the basic attributes are correct
    tale = tale_one
    manifest_doc = Manifest(tale, user)

    attributes = manifest_doc.create_basic_attributes()
    assert attributes["wt:identifier"] == str(tale["_id"])
    assert attributes["schema:name"] == tale["title"]
    assert attributes["schema:description"] == tale["description"]
    assert attributes["schema:keywords"] == tale["category"]
    assert attributes["schema:schemaVersion"] == tale["format"]
    assert attributes["schema:image"] == tale["illustration"]

    Tale().remove(tale)


@pytest.mark.plugin("wholetale", WholeTalePlugin)
def test_related_identifiers(server, user, tale_one, mock_builder):
    tale = copy.deepcopy(tale_one)
    tale.pop("_id")
    tale["relatedIdentifiers"] = [{"identifier": "urn:some_urn", "relation": "cites"}]
    with pytest.raises(ValidationException) as exc:
        tale = Tale().save(tale)
    assert str(exc.value).startswith("'cites' is not one of")

    tale["relatedIdentifiers"] = [
        {"identifier": "urn:some_urn", "relation": "Cites"},
        {"identifier": "doi:some_doi", "relation": "IsDerivedFrom"},
        {"identifier": "https://some.url", "relation": "IsIdenticalTo"},
    ]
    tale = Tale().save(tale)
    manifest = Manifest(tale, user)
    attrs = manifest.create_related_identifiers()
    assert "datacite:relatedIdentifiers" in attrs
    assert attrs["datacite:relatedIdentifiers"] == [
        {
            "datacite:relatedIdentifier": {
                "@id": "urn:some_urn",
                "datacite:relationType": "datacite:Cites",
                "datacite:relatedIdentifierType": "datacite:URN",
            }
        },
        {
            "datacite:relatedIdentifier": {
                "@id": "doi:some_doi",
                "datacite:relationType": "datacite:IsDerivedFrom",
                "datacite:relatedIdentifierType": "datacite:DOI",
            }
        },
        {
            "datacite:relatedIdentifier": {
                "@id": "https://some.url",
                "datacite:relationType": "datacite:IsIdenticalTo",
                "datacite:relatedIdentifierType": "datacite:URL",
            }
        },
    ]
    Tale().remove(tale)


@pytest.mark.plugin("wholetale", WholeTalePlugin)
def test_add_tale_creator(server, user, tale_one, mock_builder):
    manifest_doc = Manifest(tale_one, user)
    assert len(manifest_doc.manifest["schema:author"])
    manifest_creator = manifest_doc.manifest["createdBy"]
    assert manifest_creator["schema:givenName"] == user["firstName"]
    assert manifest_creator["schema:familyName"] == user["lastName"]
    assert manifest_creator["schema:email"] == user["email"]
    assert manifest_creator["@id"] == f"mailto:{user['email']}"


@pytest.mark.plugin("wholetale")
def test_create_context(server, user, tale_one, mock_builder):
    # Rather than check the contents of the context (subject to change), check that we
    # get a dict back
    manifest_doc = Manifest(tale_one, user)
    context = manifest_doc.create_context()
    assert isinstance(context, dict)


@pytest.mark.plugin("wholetale")
def test_create_aggregation_record(server, user, tale_one, mock_builder):
    # Test without a bundle
    manifest_doc = Manifest(tale_one, user)
    uri = "doi:xx.xxxx.1234"
    agg = manifest_doc.create_aggregation_record(uri)
    assert agg["uri"] == uri

    # Test with a bundle
    folder_name = "research_data"
    filename = "data.csv"
    bundle = {"folder": folder_name, "filename": filename}

    agg = manifest_doc.create_aggregation_record(uri, bundle)
    assert agg["uri"] == uri
    assert agg["bundledAs"]["folder"] == folder_name
    assert agg["bundledAs"]["filename"] == filename

    # Test with a parent dataset
    parent_dataset = "urn:uuid:100.99.xx"
    agg = manifest_doc.create_aggregation_record(uri, bundle, parent_dataset)
    assert agg["schema:isPartOf"] == parent_dataset


@pytest.mark.plugin("wholetale")
def test_get_folder_identifier(server, user, tale_one):
    folder_identifier = get_folder_identifier(tale_one["dataSet"][0]["itemId"], user)
    assert folder_identifier == "doi:10.7910/DVN/TJCLKP"


@pytest.mark.plugin("wholetale")
def test_workspace(server, user, tale_one, mock_builder):
    workspace = Folder().load(tale_one["workspaceId"], force=True)
    fspath = workspace["fsPath"]
    with open(os.path.join(fspath, "file1.csv"), "w") as f:
        f.write("1,2,3,4\n")

    manifest_doc = Manifest(tale_one, user)
    aggregates_section = manifest_doc.manifest["aggregates"]

    # Search for workspace file1.csv
    expected_path = "./workspace/" + "file1.csv"
    file_check = any(x for x in aggregates_section if (x["uri"] == expected_path))
    assert file_check
    os.remove(os.path.join(fspath, "file1.csv"))


@pytest.mark.plugin("wholetale")
def test_data_set(server, user, tale_one, mock_builder):
    # Test that all of the files in the dataSet are added
    with open(
        os.path.join(os.path.dirname(__file__), "data", "reference_dataset.json"), "r"
    ) as fp:
        reference_aggregates = json.load(fp)

    reference_aggregates = sorted(reference_aggregates, key=itemgetter("uri"))
    for d in reference_aggregates:
        if "wt:identifier" in d:
            d.pop("wt:identifier")
    manifest_doc = Manifest(tale_one, user, expand_folders=True)
    for i, aggregate in enumerate(
        sorted(manifest_doc.manifest["aggregates"], key=itemgetter("uri"))
    ):
        if "wt:identifier" in aggregate:
            aggregate.pop("wt:identifier")
        assert aggregate == reference_aggregates[i]

    # Check the datasets
    reference_datasets = [
        {
            "@id": "doi:10.7910/DVN/0MXT0H",
            "@type": "schema:Dataset",
            "schema:name": (
                "Replication Data for At-Large Elections and "
                "Minority Representation in Local Government"
            ),
            "schema:identifier": "doi:10.7910/DVN/0MXT0H",
        },
        {
            "@id": "doi:10.7910/DVN/TJCLKP",
            "@type": "schema:Dataset",
            "schema:name": "Open Source at Harvard",
            "schema:identifier": "doi:10.7910/DVN/TJCLKP",
        },
    ]

    reference_datasets = sorted(reference_datasets, key=itemgetter("@id"))
    for i, dataset in enumerate(
        sorted(manifest_doc.manifest["wt:usesDataset"], key=itemgetter("@id"))
    ):
        assert dataset == reference_datasets[i]


@pytest.mark.plugin("wholetale")
@pytest.mark.xfail(reason="Should it fail or is that just obsolete test?")
def test_different_user(server, user, tale_one, tale_two, mock_builder, extra_user):
    with pytest.raises(AccessException):
        Manifest(tale_one, extra_user)


@pytest.mark.plugin("wholetale")
def test_validate(server, user, image, tale_info, mock_builder):
    missing_orcid = {"firstName": "Lord", "lastName": "Kelvin"}
    blank_orcid = {"firstName": "Isaac", "lastName": "Newton", "orcid": ""}

    tale_missing_orcid = Tale().createTale(
        image,
        [],
        creator=tale_info["creator"],
        title=tale_info["name"],
        public=tale_info["public"],
        description=tale_info["description"],
        authors=[missing_orcid],
    )

    with pytest.raises(ValueError):
        Manifest(tale_missing_orcid, user)
    Tale().remove(tale_missing_orcid)

    tale_blank_orcid = Tale().createTale(
        image,
        [],
        creator=tale_info["creator"],
        title=tale_info["name"],
        public=tale_info["public"],
        description=tale_info["description"],
        authors=[blank_orcid],
    )
    with pytest.raises(ValueError):
        Manifest(tale_blank_orcid, user)
    Tale().remove(tale_blank_orcid)


@pytest.mark.plugin("wholetale")
def test_create_image_info(server, user, tale_one, mock_builder):
    manifest = Manifest(tale_one, user).manifest
    assert len(manifest["schema:hasPart"]) > 0

    r2d_block = manifest["schema:hasPart"][0]
    assert (
        r2d_block["schema:softwareVersion"]
        == tale_one["imageInfo"]["repo2docker_version"]
    )
    assert r2d_block["@id"] == "https://github.com/whole-tale/repo2docker_wholetale"
    assert r2d_block["@type"] == "schema:SoftwareApplication"

    digest_block = manifest["schema:hasPart"][1]
    assert digest_block["@id"] == tale_one["imageInfo"]["digest"].replace(
        "registry", "images", 1
    )
    assert digest_block["schema:applicationCategory"] == "DockerImage"
    assert digest_block["@type"] == "schema:SoftwareApplication"


@pytest.mark.plugin("wholetale")
def test_dataset_roundtrip(server, user, tale_one, mock_builder):
    manifest = Manifest(tale_one, user).manifest
    dataset = ManifestParser(manifest).get_dataset()
    assert [_["itemId"] for _ in dataset] == [
        str(_["itemId"]) for _ in tale_one["dataSet"]
    ]

    # test it still works if schema:identifier is not present
    aggregates = []
    for obj in manifest["aggregates"]:
        if "schema:identifier" in obj:
            obj.pop("schema:identifier")
        aggregates.append(obj)
    manifest["aggregates"] = aggregates
    dataset = ManifestParser(manifest).get_dataset()
    assert [_["itemId"] for _ in dataset] == [
        str(_["itemId"]) for _ in tale_one["dataSet"]
    ]
