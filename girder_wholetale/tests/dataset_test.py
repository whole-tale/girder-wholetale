import json
import pytest
from pytest_girder.assertions import assertStatusOk


@pytest.mark.plugin("wholetale")
@pytest.mark.vcr()
def test_dataset_rest(server, admin, user):
    user_data_map = [
        {
            "dataId": "https://dataverse.harvard.edu/dataset.xhtml"
            "?persistentId=doi:10.7910/DVN/TJCLKP",
            "doi": "10.7910/DVN/TJCLKP",
            "name": "Open Source at Harvard",
            "repository": "Dataverse",
            "size": 518379,
            "tale": False,
        },
        {
            "dataId": "http://use.yt/upload/9241131f",
            "doi": None,
            "name": "illustris.jpg",
            "repository": "HTTP",
            "size": 781665,
            "tale": False,
        },
    ]

    admin_data_map = [
        {
            "dataId": (
                "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/CR02PG"
            ),
            "doi": "doi:10.7910/DVN/CR02PG",
            "name": (
                "Oral History of the Tenured Women in the Faculty of Arts and Sciences at "
                "Harvard University, 1981"
            ),
            "repository": "Dataverse",
            "size": 566309963,
            "tale": False,
        }
    ]

    resp = server.request(
        path="/dataset/register",
        method="POST",
        params={"dataMap": json.dumps(user_data_map)},
        user=user,
    )
    assertStatusOk(resp)

    resp = server.request(
        path="/dataset/register",
        method="POST",
        params={"dataMap": json.dumps(admin_data_map)},
        user=admin,
    )
    assertStatusOk(resp)

    resp = server.request(path="/dataset", method="GET", user=user)
    assertStatusOk(resp)
    ds = resp.json
    assert len(ds) == 3

    resp = server.request(
        path="/dataset", method="GET", user=user, params={"myData": True}
    )
    assertStatusOk(resp)
    ds = resp.json
    assert len(ds) == 2

    ds = next((_ for _ in ds if _["provider"] == "HTTP"))
    resp = server.request(
        path="/dataset/{_id}".format(**ds), method="DELETE", user=user
    )
    assertStatusOk(resp)

    resp = server.request(
        path="/dataset", method="GET", user=user, params={"myData": True}
    )
    assertStatusOk(resp)
    ds = resp.json
    assert len(ds) == 1

    resp = server.request(
        path="/dataset",
        method="GET",
        user=user,
        params={
            "identifiers": json.dumps(["doi:10.7910/DVN/CR02PG"])
        },
    )
    assertStatusOk(resp)
    ds = resp.json
    assert len(ds) == 23  # TODO: Is it correct? It shows all individual files...
    assert ds[0]["name"].startswith("Oral History")  # Folder will be first anyway
