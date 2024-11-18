from girder.constants import AccessType
from girder.models.model_base import Model
from girder.models.setting import Setting

from .. import constants


# Holds information about the private storage
class PSInfo(Model):
    def initialize(self):
        self.name = 'psinfo'
        self.exposeFields(level=AccessType.READ, fields={'_id', 'used'})

    def validate(self, psinfo):
        return psinfo

    def updateInfo(self, used=0):
        self.update({}, {'$set': {'used': used}})

    def getInfo(self):
        obj = self.findOne()
        if obj is None:
            obj = {'used': 0}
        obj['capacity'] = \
            Setting().get(constants.PluginSettings.PRIVATE_STORAGE_CAPACITY)
        return obj

    def totalSize(self):
        return Setting().get(constants.PluginSettings.PRIVATE_STORAGE_CAPACITY)

    def sizeUsed(self):
        return self.getInfo()['capacity']
