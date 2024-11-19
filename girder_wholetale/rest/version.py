from pathlib import Path

from girder import events
from girder.api import access
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import filtermodel
from girder.constants import AccessType, TokenScope
from girder.exceptions import RestException
from girder.models.folder import Folder
# from girder.plugins.wt_data_manager.models.session import Session
from girder_virtual_resources.rest import VirtualObject

from ..constants import VERSIONS_ROOT_DIR_NAME, PluginSettings
from ..lib.manifest import Manifest
from ..lib.manifest_parser import ManifestParser
from ..models.tale import Tale
from ..models.version_hierarchy import VersionHierarchyModel
from ..utils import get_tale_dir_root
from .abstract_resource import AbstractVRResource


class Version(AbstractVRResource):
    def __init__(self, tale_node):
        super().__init__("version", VERSIONS_ROOT_DIR_NAME)
        self.route("GET", (":id", "dataSet"), self.getDataset)
        tale_node.route("GET", (":id", "restore"), self.restoreView)
        tale_node.route("PUT", (":id", "restore"), self.restore)
        events.bind(
            "rest.get.tale/:id/export.before", "wt_versioning", self.ensure_version
        )
        events.bind(
            "rest.put.tale/:id/publish.before", "wt_versioning", self.ensure_version
        )
        events.bind("rest.put.version/:id.after", "wt_versioning", self.update_parents)
        events.bind(
            "rest.delete.version/:id.before", "wt_versioning", self.update_parents
        )
        events.bind("tale.view_restored", "wt_versioning", self.restoreViewEvent)
        self.model = VersionHierarchyModel()

    @access.user(TokenScope.DATA_WRITE)
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Rename a version. Returns the renamed version " "folder")
        .modelParam(
            "id",
            "The ID of version",
            model=Folder,
            level=AccessType.WRITE,
            destName="vfolder",
        )
        .param("name", "The new name", required=True, dataType="string")
        .param(
            "allowRename",
            'Allow to modify "name" if object with the same name' "already exists.",
            required=False,
            dataType="boolean",
            default=False,
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
        .errorResponse("Illegal file name", 400)
        .errorResponse("Name already exists", 409)
    )
    def rename(self, vfolder: dict, name: str, allowRename: bool) -> dict:
        renamed_version = super().rename(vfolder, name, allow_rename=allowRename)
        user = self.getCurrentUser()

        root = Folder().load(vfolder["parentId"], user=user, level=AccessType.WRITE)
        tale = Tale().load(root["taleId"], user=user, level=AccessType.WRITE)
        manifest = Manifest(
            tale, user, versionId=renamed_version["_id"], expand_folders=False
        )
        version_path = Path(renamed_version["fsPath"])
        with open((version_path / "manifest.json").as_posix(), "w") as fp:
            fp.write(manifest.dump_manifest())
        Tale().updateTale(Tale().load(root["taleId"], force=True))
        return renamed_version

    @access.user(TokenScope.DATA_READ)
    @autoDescribeRoute(
        Description(
            "Returns the dataset associated with a version, but with some additional "
            "entries such as the type of object (folder/item) and the object dictionaries."
        )
        .modelParam(
            "id",
            "The ID of a version",
            model=Folder,
            level=AccessType.READ,
            destName="version",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to the "
            "respective version folder.",
            403,
        )
    )
    def getDataset(self, version: dict) -> dict:
        mp = ManifestParser(Path(version["fsPath"]) / "manifest.json")
        dataSet = mp.get_dataset()
        # Session().loadObjects(dataSet)  # TODO: This is a temporary solution
        return dataSet

    @access.public
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Returns a version.")
        .modelParam(
            "id",
            "The ID of a version",
            model=Folder,
            level=AccessType.READ,
            destName="vfolder",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to the "
            "respective version folder.",
            403,
        )
    )
    def load(self, vfolder: dict) -> dict:
        return vfolder

    @access.user(TokenScope.DATA_WRITE)
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Creates a new version of a tale. Returns the new version folder.")
        .modelParam(
            "taleId",
            "A tale requesting the creation of a new version.",
            model=Tale,
            level=AccessType.WRITE,
            destName="tale",
            paramType="query",
        )
        .param(
            "name",
            "An optional name for the version. If not specified, a name will be "
            "generated from the current date and time.",
            required=False,
            dataType="string",
        )
        .param(
            "force",
            "Force creation of a version even if no files were modified in the "
            "workspace since the last version was created.",
            required=False,
            dataType="boolean",
            default=False,
        )
        .param(
            "allowRename",
            'Allow to modify "name" if object with the same name' "already exists.",
            required=False,
            dataType="boolean",
            default=False,
        )
        .errorResponse(
            "Access was denied (if current user does not have write access"
            " to this tale)",
            403,
        )
        .errorResponse("Another version is being created. Try again later.", 409)
        .errorResponse("Illegal file name", 400)
        .errorResponse(
            "See other (if tale workspace has not been changed since the last "
            "checkpoint). The response will contain a typical error message body, which "
            'is a JSON object. This object will have an "extra" attribute containing'
            "the id of the version that represents this last checkpoint.",
            303,
        )
    )
    def create(
        self,
        tale: dict,
        name: str = None,
        force: bool = False,
        allowRename: bool = False,
    ) -> dict:
        if not name:
            name = self.model.generateName()
        user = self.getCurrentUser()

        root = self.model.getRootFromTale(tale, user=user, level=AccessType.WRITE)
        name = self.model.checkNameSanity(name, root, allow_rename=allowRename)

        if not self.model.setCriticalSectionFlag(root):
            raise RestException(
                "Another operation is in progress. Try again later.", 409
            )
        try:
            rootDir = get_tale_dir_root(tale, PluginSettings.VERSIONS_DIRS_ROOT)
            return self.model.create(tale, name, rootDir, root, user=user, force=force)
        finally:
            # probably need a better way to deal with hard crashes here
            self.model.resetCriticalSectionFlag(root)
            Tale().updateTale(tale)

    @access.user(TokenScope.DATA_WRITE)
    @filtermodel(model="tale", plugin="wholetale")
    @autoDescribeRoute(
        Description("Restores a version.")
        .modelParam(
            "id",
            "The ID of the Tale to be modified.",
            model=Tale,
            level=AccessType.WRITE,
            destName="tale",
        )
        .modelParam(
            "versionId",
            "The ID of version folder",
            model=Folder,
            level=AccessType.READ,
            destName="version",
            paramType="query",
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
        .errorResponse("Version is in use by a run and cannot be deleted.", 461)
    )
    def restore(self, tale: dict, version: dict):
        return self.model.restore(tale, version, self.getCurrentUser())

    @access.user(TokenScope.DATA_READ)
    @filtermodel(model="tale", plugin="wholetale")
    @autoDescribeRoute(
        Description("Returns a Tale object based on a version.")
        .notes("It does not modify the state of the Tale. It is just a 'mocked' view.")
        .modelParam(
            "id",
            "The ID of the Tale",
            model=Tale,
            level=AccessType.READ,
            destName="tale",
        )
        .modelParam(
            "versionId",
            "The ID of version folder",
            model=Folder,
            level=AccessType.READ,
            destName="version",
            paramType="query",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to this "
            "tale)",
            403,
        )
    )
    def restoreView(self, tale: dict, version: dict):
        return self._restoreView(tale, version)

    def restoreViewEvent(self, event: events.Event):
        tale = self._restoreView(event.info["tale"], event.info["version"])
        event.preventDefault().addResponse(tale)

    def _restoreView(self, tale: dict, version: dict):
        version = Folder().load(version["_id"], force=True, fields=["fsPath"])
        tale.update(self.model.restoreTaleFromVersion(version))
        tale["workspaceId"] = VirtualObject().generate_id(
            Path(version["fsPath"]) / "workspace", version["_id"]
        )
        return tale

    @access.user(TokenScope.DATA_OWN)
    @autoDescribeRoute(
        Description("Deletes a version.")
        .modelParam(
            "id",
            "The ID of version folder",
            model=Folder,
            level=AccessType.ADMIN,
            destName="version",
        )
        .errorResponse(
            "Access was denied (if current user does not have write access to this "
            "tale)",
            403,
        )
        .errorResponse("Version is in use by a run and cannot be deleted.", 461)
    )
    def delete(self, version: dict) -> None:
        self.model.remove(version, self.getCurrentUser())

    @access.public
    @filtermodel("folder")
    @autoDescribeRoute(
        Description("Lists versions.")
        .modelParam(
            "taleId",
            "The ID of a tale for which versions are to be listed.",
            model=Tale,
            plugin="wholetale",
            level=AccessType.READ,
            destName="tale",
            paramType="query",
        )
        .pagingParams(defaultSort="created")
        .errorResponse(
            "Access was denied (if current user does not have read access to this tale)",
            403,
        )
    )
    def list(self, tale: dict, limit, offset, sort):
        return super().list(
            tale, user=self.getCurrentUser(), limit=limit, offset=offset, sort=sort
        )

    @access.user(TokenScope.DATA_READ)
    @autoDescribeRoute(
        Description("Check if a version with the given name exists.")
        .modelParam(
            "taleId",
            "The ID of versions root folder.",
            model=Tale,
            level=AccessType.READ,
            destName="tale",
            paramType="query",
        )
        .param(
            "name",
            "Return the folder with this name or nothing if no such folder exists.",
            required=False,
            dataType="string",
        )
        .errorResponse(
            "Access was denied (if current user does not have read access to this tale)",
            403,
        )
    )
    def exists(self, tale: dict, name: str):
        return super().exists(tale, name)

    @access.user(scope=TokenScope.DATA_WRITE)
    def ensure_version(self, event: events.Event):
        params = event.info.get("params", {})
        taleId = event.info.get("id")
        version_id = params.get("versionId")
        if not version_id:
            try:
                version = self.create(taleId=taleId, allowRename=True, params={})
                # Above obj is filtered model so we need to reload it...
                version = Folder().load(version["_id"], force=True)
            except RestException as exc:
                if exc.code == 303:
                    version = Folder().load(exc.extra, force=True)
                else:
                    raise
            # We're using 'updated' field to bump the version to the top of
            # Folder().list(). We're gonna update it either way later on.
            Folder().updateFolder(version)
