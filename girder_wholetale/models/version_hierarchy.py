from bson import ObjectId
import json
import logging
import shutil
from pathlib import Path
import pymongo
from typing import Optional

from girder.constants import AccessType
from girder.exceptions import RestException
from girder.models.folder import Folder
from .tale import Tale
from .hierarchy import AbstractHierarchyModel


logger = logging.getLogger(__name__)


class VersionHierarchyModel(AbstractHierarchyModel):
    root_tale_field = "versionsRootId"
    field_critical_section_flag = "versionsCriticalSectionFlag"
    field_reference_counter = "versionsRefCount"

    def create(
        self,
        tale: dict,
        name: Optional[str],
        versionsDir: Path,
        versionsRoot: dict,
        user=None,
        force=False,
    ) -> dict:
        last = self.getLastVersion(versionsRoot)
        last_restore = Folder().load(tale.get("restoredFrom", ObjectId()), force=True)
        workspace = Folder().load(tale["workspaceId"], force=True)
        crtWorkspace = Path(workspace["fsPath"])

        # NOTE: order is important, we want oldWorkspace -> last.workspace
        for version in (last_restore, last):
            oldWorkspace = (
                None if version is None else Path(version["fsPath"]) / "workspace"
            )
            if (
                not force
                and self.is_same(tale, version, user)
                and self.sameTree(oldWorkspace, crtWorkspace)
            ):
                assert version is not None
                raise RestException("Not modified", code=303, extra=str(version["_id"]))

        new_version = self.createSubdir(versionsDir, versionsRoot, name, user=user)

        try:
            self.snapshot(last, tale, new_version, user=user, force=force)
            return new_version
        except Exception:  # NOQA
            try:
                shutil.rmtree(new_version["fsPath"])
                Folder().remove(new_version)
            except Exception as ex:  # NOQA
                logger.warning(
                    "Exception caught while rolling back version ckeckpoint.", ex
                )
            raise

    def getLastVersion(self, versionsFolder: dict) -> Optional[dict]:
        # The versions root folder is kept as a pure Girder folder.
        # This is because there is no efficient way to
        # say "give me the latest subdir" on a POSIX filesystem.
        return Folder().findOne(
            {"parentId": versionsFolder["_id"]}, sort=[("created", pymongo.DESCENDING)]
        )

    def restore(self, tale: dict, version: dict, user: dict):
        version_root = Folder().load(
            version["parentId"], user=user, level=AccessType.READ
        )

        workspace = Folder().load(tale["workspaceId"], force=True)
        workspace_path = Path(workspace["fsPath"])
        version = Folder().load(version["_id"], force=True, fields=["fsPath"])
        version_workspace_path = Path(version["fsPath"]) / "workspace"

        if not self.setCriticalSectionFlag(version_root):
            raise RestException(
                "Another operation is in progress. Try again later.", 409
            )
        try:
            # restore workspace
            shutil.rmtree(workspace_path)
            workspace_path.mkdir()
            self.snapshotRecursive(None, version_workspace_path, workspace_path)
            # restore Tale
            tale.update(self.restoreTaleFromVersion(version))
            return Tale().save(tale)
        finally:
            # probably need a better way to deal with hard crashes here
            self.resetCriticalSectionFlag(version_root)

    @staticmethod
    def restoreTaleFromVersion(version, annotate=True):
        version_path = Path(version["fsPath"])
        with open((version_path / "manifest.json").as_posix(), "r") as fp:
            manifest = json.load(fp)
        with open((version_path / "environment.json").as_posix(), "r") as fp:
            env = json.load(fp)
        restored_tale = Tale().restoreTale(manifest, env)
        if annotate:
            restored_tale["restoredFrom"] = version["_id"]
        return restored_tale

    def resetCrashedCriticalSections(self):
        Folder().update(
            {self.field_critical_section_flag: True},
            {"$set": {self.field_critical_section_flag: False}},
        )
