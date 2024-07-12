import json
from girder_client import GirderClient

gc = GirderClient(apiUrl="https://girder.local.wholetale.org/api/v1")

root_collection = gc.get("/collection", parameters={"text": "WholeTale Catalog"})[0]
root_folder = gc.get(
    "/folder",
    parameters={
        "parentId": root_collection["_id"],
        "parentType": "collection",
        "text": "WholeTale Catalog",
    },
)[0]

def walk_folder(folder, path=""):
    data = {"name": folder["name"], "meta": folder.get("meta", {}), "files": [], "folders": []}
    for item in gc.listItem(folder["_id"]):
        for file in gc.listFile(item["_id"]):
            data["files"].append(
                {
                    "name": file["name"],
                    "meta": item["meta"],
                    "linkUrl": file["linkUrl"],
                    "size": file["size"],
                    "mimeType": file["mimeType"],
                }
            )

    for child in gc.listFolder(folder["_id"]):
        data["folders"].append(walk_folder(child))
    return data

with open("data/manifest_mock_catalog.json", "w") as fp:
    json.dump(walk_folder(root_folder), fp)
