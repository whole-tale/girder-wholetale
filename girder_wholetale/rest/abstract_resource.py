import os

from girder import events
from girder.api import access
from girder.api.v1.resource import Resource
from girder.constants import AccessType, TokenScope
from girder.exceptions import RestException
from girder.models.folder import Folder

from ..models.tale import Tale


class AbstractVRResource(Resource):
    root_tale_field = None
    model = None

    def __init__(self, resourceName, rootDirName):
        Resource.__init__(self)
        self.resourceName = resourceName
        self.rootDirName = rootDirName
        self.route("POST", (), self.create)
        self.route("GET", (), self.list)
        self.route("GET", ("exists",), self.exists)
        # Resource has its own handler here and whoever designed girder figured that if somebody
        # says route() in a subclass, that's totally OK to ignore and use the superclass stuff
        # instead.
        self.removeRoute("GET", (":id",))
        self.route("GET", (":id",), self.load)
        self.route("PUT", (":id",), self.rename)
        self.route("DELETE", (":id",), self.delete)

    def rename(self, vrfolder: dict, newName: str, allow_rename: bool = False) -> dict:
        user = self.getCurrentUser()
        if not newName:
            raise RestException("New name cannot be empty.", code=400)
        root = Folder().load(vrfolder["parentId"], user=user, level=AccessType.WRITE)
        newName = self.model.checkNameSanity(newName, root, allow_rename=allow_rename)
        vrfolder.update({"name": newName})
        os.utime(vrfolder["fsPath"])
        os.utime(os.path.dirname(vrfolder["fsPath"]))
        return Folder().updateFolder(
            vrfolder
        )  # Filtering done by non abstract resource

    def load(self, vrfolder: dict) -> dict:
        raise NotImplementedError

    @access.user(scope=TokenScope.DATA_WRITE)
    def update_parents(self, event: events.Event):
        vrfolder_id = event.info.get("id")
        user = self.getCurrentUser()
        vrfolder = Folder().load(vrfolder_id, user=user, level=AccessType.WRITE)
        if vrfolder:
            root = Folder().load(
                vrfolder["parentId"], user=user, level=AccessType.WRITE
            )
            Folder().updateFolder(root)
            tale = Tale().load(
                root["meta"]["taleId"], user=user, level=AccessType.WRITE
            )
            Tale().updateTale(tale)

    def list(
        self,
        tale: dict,
        user=None,
        limit=0,
        offset=0,
        sort=None,
        filters=None,
        **kwargs,
    ):
        root = self.model.getRootFromTale(tale, user=user, level=AccessType.READ)
        return Folder().childFolders(
            root,
            "folder",
            user=user,
            limit=limit,
            offset=offset,
            sort=sort,
            filters=filters,
            **kwargs,
        )

    def exists(self, tale: dict, name: str):
        user = self.getCurrentUser()
        root = self.model.getRootFromTale(tale, user=user, level=AccessType.READ)
        obj = Folder().findOne({"parentId": root["_id"], "name": name})
        if obj is None:
            return {"exists": False}
        else:
            return {"exists": True, "obj": Folder().filter(obj, self.getCurrentUser())}
