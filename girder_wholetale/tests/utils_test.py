import pytest
from girder_wholetale.utils import getOrCreateRootFolder


@pytest.mark.plugin("wholetale")
def test_getOrCreateRootFolder(server):
    folder_name = "folder_name"
    folder_desc = "folder_description"
    folder = getOrCreateRootFolder(folder_name, folder_desc)

    assert folder["name"] == folder_name
    assert folder["description"] == folder_desc
