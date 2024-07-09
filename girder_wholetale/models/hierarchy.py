import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pathvalidate
from girder.constants import AccessType
from girder.exceptions import RestException
from girder.models.folder import Folder

from ..lib.manifest import Manifest
from .tale import Tale


logger = logging.getLogger(__name__)


class AbstractHierarchyModel(object):
    root_tale_field = None
    field_sequence_number = "seq"
    name_format = "%c"
    field_critical_section_flag = None
    field_reference_counter = None

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(AbstractHierarchyModel, cls).__new__(cls)
        return cls.instance

    def getRootFromTale(self, tale: dict, user=None, level=AccessType.READ) -> dict:
        if user:
            kwargs = dict(user=user, level=level)
        else:
            kwargs = dict(force=True)
        return Folder().load(tale[self.root_tale_field], exc=True, **kwargs)

    @staticmethod
    def checkNameSanity(
        name: Optional[str],
        parentFolder: dict,
        allow_rename: bool = False,
    ) -> str:
        if not name:
            raise RestException("Name cannot be empty.", code=400)

        try:
            pathvalidate.validate_filename(name, platform="Linux")
        except pathvalidate.ValidationError:
            raise RestException("Invalid file name: " + name, code=400)

        q = {"parentId": parentFolder["_id"], "name": name}
        if not allow_rename and Folder().findOne(q, fields=["_id"]):
            raise RestException("Name already exists: " + name, code=409)

        n = 0
        while Folder().findOne(q, fields=["_id"]):
            n += 1
            q["name"] = f"{name} ({n})"
            if n > 100:
                break
        return q["name"]

    @staticmethod
    def createSubdir(rootDir: Path, rootFolder: dict, name: str, user=None) -> dict:
        """Create both Girder folder and corresponding directory. The name is stored in the Girder
        folder, whereas the name of the directory is taken from the folder ID. This is a
        deliberate step to discourage renaming of directories directly on disk, which would mess
        up the mapping between Girder folders and directories
        """
        folder = Folder().createFolder(rootFolder, name, creator=user)
        dirname = str(folder["_id"])
        directory = rootDir / dirname
        directory.mkdir(parents=True)
        folder.update({"fsPath": directory.absolute().as_posix(), "isMapping": True})
        folder = Folder().save(folder, validate=False, triggerEvents=False)

        # update the time
        Folder().updateFolder(rootFolder)
        return folder

    def snapshot(
        self,
        version: Optional[dict],
        tale: dict,
        new_version: dict,
        user=None,
        force=False,
    ) -> None:
        """Creates a new version from the current state and an old version. The implementation
        here differs a bit from
        https://docs.google.com/document/d/1b2xZtIYvgVXz7EVeV-C18So_a7QLGg59dPQMxvBcA5o since
        the document assumes traditional use of Girder objects to simulate a filesystem, whereas
        the current reality is that we are moving more towards Girder being a thin layer on top
        of an actual FS (i.e. virtual_resources). In particular, the data folder remains in the
        domain of the DMS, meaning that actual files are only downloaded on demand. The current
        implementation of the virtual objects does not seem to have a straightforward way of
        embedding pure girder folders inside a virtual tree. The solution currently adopted to
        address this issue involves storing the dataset in the version folder itself, which, for
        efficiency reasons remains a Girder folder (but also a virtual_resources root).

        It may be relevant to note that this implementation uses option (b) in the above document
        with respect to the meaning of "copy" in step 4.1.2.1. To be more precise, when a file is
        changed in the current workspace, the new version will hard link to the file in question
        instead of doing an actual copy. This allows for O(1) equality comparisons between files,
        but requires that modifications to files in the workspace always create a new file (which
        is the case if files are only modified through the WebDAV FS mounted in a tale container).
        """
        new_version_path = Path(new_version["fsPath"])
        manifest = Manifest(
            tale, user, versionId=new_version["_id"], expand_folders=False
        )
        with open((new_version_path / "manifest.json").as_posix(), "w") as fp:
            fp.write(manifest.dump_manifest())

        with open((new_version_path / "environment.json").as_posix(), "w") as fp:
            fp.write(manifest.dump_environment())

        oldWorkspace = (
            None if version is None else Path(version["fsPath"]) / "workspace"
        )
        workspace = Folder().load(tale["workspaceId"], force=True)
        crtWorkspace = Path(workspace["fsPath"])
        newWorkspace = new_version_path / "workspace"
        newWorkspace.mkdir()
        self.snapshotRecursive(oldWorkspace, crtWorkspace, newWorkspace)

    def is_same(self, tale, version, user):
        workspace = Folder().load(tale["workspaceId"], force=True)
        tale_workspace_path = Path(workspace["fsPath"])

        version_path = None if version is None else Path(version["fsPath"])
        version_workspace_path = (
            None if version_path is None else version_path / "workspace"
        )

        manifest_obj = Manifest(tale, user)
        manifest = json.loads(manifest_obj.dump_manifest())
        environment = json.loads(manifest_obj.dump_environment())
        tale_restored_from_wrk = Tale().restoreTale(manifest, environment)
        tale_restored_from_ver = (
            self.restoreTaleFromVersion(version, annotate=False) if version else None
        )

        if self.sameTaleMetadata(
            tale_restored_from_ver, tale_restored_from_wrk
        ) and self.sameTree(version_workspace_path, tale_workspace_path):
            raise RestException("Not modified", code=303, extra=str(version["_id"]))

    def snapshotRecursive(self, old: Optional[Path], crt: Path, new: Path) -> None:
        for c in crt.iterdir():
            newc = new / c.name
            oldc = None if old is None else old / c.name
            crtc = crt / c.name

            if oldc is not None:
                if not oldc.exists():
                    oldc = None
                else:
                    if crtc.is_dir() != oldc.is_dir():
                        # either oldc was a dir and is now a file or the other way around
                        oldc = None

            if c.is_dir():
                newc.mkdir()
                self.snapshotRecursive(oldc, crtc, newc)
            else:
                crtcstr = crtc.absolute()
                newcstr = newc.absolute()
                try:
                    os.link(crtcstr.as_posix(), newcstr.as_posix())
                except:  # noqa: E722
                    logger.warn("link %s -> %s" % (crtcstr, newcstr))
                    raise
                shutil.copystat(crtcstr, newcstr)

    def incrementReferenceCount(self, vfolder):
        if self.field_reference_counter not in vfolder:
            vfolder[self.field_reference_counter] = 0
        self.updateReferenceCount(vfolder, 1)

    def decrementReferenceCount(self, vfolder):
        self.updateReferenceCount(vfolder, -1)

    def updateReferenceCount(self, vfolder: dict, n: int):
        root = Folder().load(vfolder["parentId"], force=True)
        self.setCriticalSectionFlag(root)
        try:
            vfolder[self.field_reference_counter] += n
            vfolder = Folder().save(vfolder)
        except KeyError:
            pass
        finally:
            self.resetCriticalSectionFlag(root)

    def setCriticalSectionFlag(self, root: dict) -> bool:
        return self.updateCriticalSectionFlag(root, True)

    def resetCriticalSectionFlag(self, root: dict) -> bool:
        return self.updateCriticalSectionFlag(root, False)

    def updateCriticalSectionFlag(self, root: dict, value: bool) -> bool:
        result = Folder().update(
            query={
                "_id": root["_id"],
                self.field_critical_section_flag: {"$ne": value},
            },
            update={
                "$set": {self.field_critical_section_flag: value},
                "$inc": {self.field_sequence_number: 1},
            },
            multi=False,
        )
        return result.matched_count > 0

    def generateName(self):
        now = datetime.now()
        return now.strftime(self.name_format)

    @staticmethod
    def sameTaleMetadata(old: Optional[dict], crt: dict):
        if old is None:
            return False
        return old == crt

    def sameTree(self, old: Optional[Path], crt: Path) -> bool:
        if old is None:
            return False
        for c in crt.iterdir():
            oldc = old / c.name
            crtc = crt / c.name

            if not oldc.exists():
                return False

            if crtc.is_dir() != oldc.is_dir():
                return False

            if crtc.is_dir():
                if not self.sameTree(oldc, crtc):
                    return False
            else:
                if not oldc.samefile(crtc):
                    return False
        return True

    def remove(self, version: dict, user: dict) -> None:
        root = Folder().load(version["parentId"], user=user, level=AccessType.WRITE)
        self.setCriticalSectionFlag(root)
        try:
            # make sure we use information protected by the critical section
            version = Folder().load(version["_id"], user=user, level=AccessType.ADMIN)
            if version.get(self.field_reference_counter, 0) > 0:
                raise RestException(
                    "Version is in use by a run and cannot be deleted.", 461
                )
        finally:
            self.resetCriticalSectionFlag(root)

        path = Path(version["fsPath"])
        trashDir = path.parent / ".trash"
        Folder().remove(version)

        shutil.move(path.as_posix(), trashDir)
        Tale().updateTale(Tale().load(root["taleId"], force=True))
