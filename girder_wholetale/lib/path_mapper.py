from girder.models.setting import Setting
from .. import constants


class PathMapper:
    def getPSPath(self, itemId):
        root = Setting().get(constants.PluginSettings.PRIVATE_STORAGE_PATH)
        sItemId = str(itemId)
        return root + "/" + sItemId[0] + "/" + sItemId[1] + "/" + sItemId
