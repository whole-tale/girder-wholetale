# -*- coding: utf-8 -*-

import datetime
import json
import tempfile
import zipfile

import jsonschema
from bson.objectid import ObjectId
from girder import events
from girder.constants import AccessType
from girder.exceptions import GirderException, ValidationException
from girder.models.assetstore import Assetstore
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.model_base import AccessControlledModel
from girder.models.user import User
from girder.models.token import Token
from girder_jobs.models.job import Job
from girder.utility import assetstore_utilities
from gwvolman.constants import BUILD_TALE_IMAGE_STEP_TOTAL
from gwvolman.tasks import build_tale_image

from ..constants import TaleStatus
from ..lib.license import WholeTaleLicense
from ..lib.manifest_parser import ManifestParser
from ..lib.metrics import metricsLogger
from ..schema.misc import dataSetSchema, related_identifiers_schema
from ..utils import diff_access, getOrCreateRootFolder, init_progress, notify_event
from .image import Image as imageModel

# Whenever the Tale object schema is modified (e.g. fields are added or
# removed) increase `_currentTaleFormat` to retroactively apply those
# changes to existing Tales.
_currentTaleFormat = 9


class Tale(AccessControlledModel):

    def initialize(self):
        self.name = 'tale'
        self.ensureIndices(('imageId', ([('imageId', 1)], {})))
        self.ensureTextIndex({
            'title': 10,
            'description': 1
        })

        # Fields that can be modified via PUT /tale/:id
        self.modifiableFields = {
            "authors",
            "category",
            "config",
            "description",
            "dataSet",
            "icon",
            "iframe",
            "illustration",
            "imageId",
            "licenseSPDX",
            "public",
            "publishInfo",  # This shouldn't be here
            "relatedIdentifiers",  # This shouldn't be her
            "title",
        }

        self.exposeFields(
            level=AccessType.READ,
            fields={
                "_id",
                "authors",
                "category",
                "config",
                "copyOfTale",
                "created",
                "creatorId",
                "dataSet",
                "dataSetCitation",
                "description",
                "format",
                "icon",
                "iframe",
                "illustration",
                "imageId",
                "imageInfo",
                "licenseSPDX",
                "public",
                "publishInfo",
                "relatedIdentifiers",
                "restoredFrom",
                "runsRootId",
                "status",
                "title",
                "updated",
                "versionsRootId",
                "workspaceId",
            }
        )

    @staticmethod
    def _validate_dataset(tale):
        try:
            jsonschema.validate(tale["dataSet"], dataSetSchema)
        except jsonschema.exceptions.ValidationError as exc:
            raise ValidationException(str(exc))

        creator = User().load(tale["creatorId"], force=True)
        for obj in tale["dataSet"]:
            if obj["_modelType"] == "folder":
                model = Folder()
            else:
                model = Item()
            model.load(
                obj["itemId"],
                level=AccessType.READ,
                user=creator,
                exc=True
            )

    @staticmethod
    def _validate_related_identifiers(tale):
        try:
            jsonschema.validate(tale["relatedIdentifiers"], related_identifiers_schema)
        except jsonschema.exceptions.ValidationError as exc:
            raise ValidationException(str(exc))

    def validate(self, tale):
        if 'status' not in tale:
            tale['status'] = TaleStatus.READY

        if 'iframe' not in tale:
            tale['iframe'] = False

        for array_key in (
            'publishInfo', 'dataSet', 'dataSetCitation', 'relatedIdentifiers', 'authors',
        ):
            if not isinstance(tale.get(array_key), list):
                tale[array_key] = []

        if 'licenseSPDX' not in tale:
            tale['licenseSPDX'] = WholeTaleLicense.default_spdx()
        tale_licenses = WholeTaleLicense()
        if tale['licenseSPDX'] not in tale_licenses.supported_spdxes():
            tale['licenseSPDX'] = WholeTaleLicense.default_spdx()

        if tale.get('config') is None:
            tale['config'] = {}

        if 'copyOfTale' not in tale:
            tale['copyOfTale'] = None

        tale['format'] = _currentTaleFormat

        self._validate_dataset(tale)
        self._validate_related_identifiers(tale)
        return tale

    def list(self, user=None, data=None, image=None, limit=0, offset=0,
             sort=None, currentUser=None, level=AccessType.READ):
        """
        List a page of jobs for a given user.

        :param user: The user who created the tale.
        :type user: dict or None
        :param data: The object array that's being used by the tale.
        :type data: dict or None
        :param image: The Image that's being used by the tale.
        :type image: dict or None
        :param limit: The page limit.
        :param offset: The page offset
        :param sort: The sort field.
        :param currentUser: User for access filtering.
        """
        cursor_def = {}
        if user is not None:
            cursor_def['creatorId'] = user['_id']
        if data is not None:
            cursor_def['dataSet'] = data
        if image is not None:
            cursor_def['imageId'] = image['_id']

        cursor = self.find(cursor_def, sort=sort)
        for r in self.filterResultsByPermission(
                cursor=cursor, user=currentUser, level=level,
                limit=limit, offset=offset):
            yield r

    def createTale(self, image, data, creator=None, save=True, title=None,
                   description=None, public=None, config=None, authors=None,
                   icon=None, category=None, illustration=None,
                   licenseSPDX=None,
                   status=TaleStatus.READY, publishInfo=None,
                   relatedIdentifiers=None, imageInfo=None):

        if creator is None:
            creatorId = None
        else:
            creatorId = creator.get('_id', None)

        if title is None:
            title = f"A tale based on {image['name']}"

        if description is None:
            description = f'This Tale, {title}, represents a computational experiment. ' \
                          f'It contains code, data, and metadata relevant to the experiment.'
        # if illustration is None:
            # Get image from SILS

        now = datetime.datetime.now(datetime.timezone.utc)
        tale = {
            'authors': authors,
            'category': category,
            'config': config or {},
            'copyOfTale': None,
            'creatorId': creatorId,
            'dataSet': data or [],
            'description': description,
            'format': _currentTaleFormat,
            'created': now,
            'icon': icon,
            'iframe': image.get('iframe', False),
            'imageId': ObjectId(image['_id']),
            'imageInfo': imageInfo or {},
            'illustration': illustration,
            'title': title,
            'public': public,
            'publishInfo': publishInfo or [],
            'relatedIdentifiers': relatedIdentifiers or [],
            'updated': now,
            'licenseSPDX': licenseSPDX or WholeTaleLicense.default_spdx(),
            'status': status,
        }
        if public is not None and isinstance(public, bool):
            self.setPublic(tale, public, save=False)
        else:
            public = False

        if creator is not None:
            self.setUserAccess(tale, user=creator, level=AccessType.ADMIN,
                               save=False)

        if save:
            tale = self.save(tale)
            notify_event([creator["_id"]], "wt_tale_created", {"taleId": tale['_id']})

        if tale['dataSet']:
            events.trigger(
                "tale.update_citation",
                info={"tale": tale, "user": creator}
            )

        metricsLogger.info(
            "tale.create",
            extra={
                "details": {
                    "id": tale["_id"],
                    "imageId": tale["imageId"],
                    "imageInfo": tale["imageInfo"],
                }
            },
        )

        return tale

    def _createAuxFolder(self, tale, rootFolderName, creator=None):
        if creator is None:
            creator = User().load(tale['creatorId'], force=True)

        if tale['public'] is not None and isinstance(tale['public'], bool):
            public = tale['public']
        else:
            public = False

        rootFolder = getOrCreateRootFolder(rootFolderName)
        auxFolder = Folder().createFolder(
            rootFolder, str(tale['_id']), parentType='folder',
            public=public, reuseExisting=True)
        Folder().setUserAccess(
            auxFolder, user=creator, level=AccessType.ADMIN,
            save=True)
        auxFolder = Folder().setMetadata(
            auxFolder, {'taleId': str(tale['_id'])})
        return auxFolder

    def updateTale(self, tale):
        """
        Updates a tale.

        :param tale: The tale document to update.
        :type tale: dict
        :returns: The tale document that was edited.
        """
        tale['updated'] = datetime.datetime.now(datetime.timezone.utc)
        ret = self.save(tale)
        users = [user['id'] for user in tale['access']['users']]
        notify_event(users, "wt_tale_updated", {"taleId": tale['_id']})
        return ret

    def setAccessList(self, doc, access, save=False, user=None, force=False,
                      setPublic=None, publicFlags=None):
        """
        Overrides AccessControlledModel.setAccessList to encapsulate ACL
        functionality for a tale.

        :param doc: the tale to set access settings on
        :type doc: girder.models.tale
        :param access: The access control list
        :type access: dict
        :param save: Whether the changes should be saved to the database
        :type save: bool
        :param user: The current user
        :param force: Set this to True to set the flags regardless of the passed in
            user's permissions.
        :type force: bool
        :param setPublic: Pass this if you wish to set the public flag on the
            resources being updated.
        :type setPublic: bool or None
        :param publicFlags: Pass this if you wish to set the public flag list on
            resources being updated.
        :type publicFlags: flag identifier str, or list/set/tuple of them,
            or None
        """
        if setPublic is not None:
            self.setPublic(doc, setPublic, save=False)

        if publicFlags is not None:
            doc = self.setPublicFlags(doc, publicFlags, user=user, save=False,
                                      force=force)

        added, removed = diff_access(doc["access"], access)
        doc = super().setAccessList(
            doc, access, user=user, save=save, force=force)

        if save:
            notify_event(added, "wt_tale_shared", {"taleId": str(doc["_id"])})
            notify_event(removed, "wt_tale_unshared", {"taleId": str(doc["_id"])})

            for folder in Folder().find({"meta.taleId": str(doc["_id"])}):
                Folder().setAccessList(
                    folder, access, user=user, save=save, force=force, recurse=True,
                    setPublic=setPublic, publicFlags=publicFlags)

        return doc

    def setUserAccess(
        self, doc, user, level, save=False, flags=None, currentUser=None, force=False
    ):
        if level < AccessType.READ:
            event_type = "wt_tale_unshared"
        else:
            event_type = "wt_tale_shared"

        if level == AccessType.NONE:
            level = None

        doc = super().setUserAccess(
            doc, user, level, save=save, flags=flags, currentUser=currentUser, force=force
        )

        if save and "_id" in doc:  # During creation of Tale it's not there yet.
            notify_event([user["_id"]], event_type, {"taleId": str(doc["_id"])})
            for folder in Folder().find({"meta.taleId": str(doc["_id"])}):
                Folder().setUserAccess(
                    folder,
                    user,
                    level,
                    save=save,
                    flags=flags,
                    currentUser=currentUser,
                    force=force
                )
        return doc

    def buildImage(self, tale, user, force=False):
        """
        Build the image for the tale
        """

        resource = {
            'type': 'wt_build_image',
            'tale_id': tale['_id'],
            'tale_title': tale['title']
        }

        token = Token().createToken(user=user, days=0.5)

        notification = init_progress(
            resource, user, 'Building image',
            'Initializing', BUILD_TALE_IMAGE_STEP_TOTAL)

        buildTask = build_tale_image.signature(
            args=[str(tale['_id']), force],
            girder_job_other_fields={
                'wt_notification_id': str(notification['_id']),
            },
            girder_client_token=str(token['_id']),
        ).apply_async()

        return buildTask.job

    @staticmethod
    def _extractZipPayload(stream):
        assetstore = Assetstore().getCurrent()
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore)
        tempDir = adapter.tempDir

        with tempfile.NamedTemporaryFile(dir=tempDir) as fp:
            for chunk in stream(2 * 1024 ** 3):
                fp.write(chunk)
            fp.seek(0)
            if not zipfile.is_zipfile(fp):
                raise GirderException("Provided file is not a zipfile")

            with zipfile.ZipFile(fp) as z:
                manifest_file = next(
                    (_ for _ in z.namelist() if _.endswith('manifest.json')),
                    None
                )
                if not manifest_file:
                    raise GirderException("Provided file doesn't contain a Tale manifest")

                try:
                    mp = ManifestParser(json.loads(z.read(manifest_file).decode()))
                    assert mp.is_valid()
                except Exception as e:
                    raise GirderException(
                        "Couldn't read manifest.json or not a Tale: {}".format(str(e))
                    )

                env_file = next(
                    (_ for _ in z.namelist() if _.endswith("environment.json")),
                    None
                )
                try:
                    environment = json.loads(z.read(env_file).decode())
                except Exception as e:
                    raise GirderException(
                        "Couldn't read environment.json or not a Tale: {}".format(str(e))
                    )

                # Extract files to tmp on workspace assetstore
                temp_dir = tempfile.mkdtemp(dir=tempDir)
                # In theory malicious content like: abs path for a member, or relative path with
                # ../.. etc., is taken care of by zipfile.extractall, but in the end we're still
                # unzipping an untrusted content. What could possibly go wrong...?
                z.extractall(path=temp_dir)
        return temp_dir, manifest_file, mp.manifest, environment

    def createTaleFromStream(
        self, stream, user=None, publishInfo=None, relatedIdentifiers=None
    ):
        temp_dir, manifest_file, manifest, environment = self._extractZipPayload(
            stream
        )

        mp = ManifestParser(manifest)
        new_tale = mp.get_tale_fields_from_environment(environment)
        image = imageModel().load(new_tale.pop("imageId"), user=user, level=AccessType.READ)
        new_tale.update(mp.get_tale_fields())

        if relatedIdentifiers is None:
            relatedIdentifiers = []

        all_related_ids = relatedIdentifiers + new_tale["relatedIdentifiers"]
        all_related_ids = [
            json.loads(rel_id)
            for rel_id in {json.dumps(_, sort_keys=True) for _ in all_related_ids}
        ]
        new_tale["relatedIdentifiers"] = all_related_ids

        new_tale.update(
            dict(
                creator=user,
                save=True,
                public=False,
                status=TaleStatus.PREPARING,
                publishInfo=publishInfo,
            )
        )

        # We don't call mp.get_dataset now, cause it might require
        # a registration step. It's going to be called inside import_tale job.

        tale = self.createTale(
            image,
            [],
            **new_tale
        )

        resource = {
            "type": "wt_zip_import",
            "tale_id": tale["_id"],
            "tale_title": tale["title"]
        }
        notification = init_progress(
            resource, user, "Importing Tale", "Initializing", 3
        )

        job = Job().createLocalJob(
            title='Import Tale from zip', user=user,
            type='wholetale.import_tale', public=False, asynchronous=True,
            module='girder_wholetale.tasks.import_tale',
            args=(temp_dir, manifest_file),
            kwargs={'taleId': tale["_id"]},
            otherFields={
                "taleId": tale["_id"],
                "wt_notification_id": str(notification["_id"])
            },
        )
        Job().scheduleJob(job)
        return tale

    def addGitRepo(self, tale, url, user=None, spawn=False, change_status=False, title=None):
        resource = {
            "type": "wt_git_import",
            "tale_id": tale["_id"],
            "tale_title": tale["title"]
        }
        notification = init_progress(
            resource, user, "Importing from Git", "Initializing", 1
        )

        job = Job().createLocalJob(
            title="Import a git repository as a Tale",
            user=user,
            type="wholetale.import_git_repo",
            public=False,
            asynchronous=True,
            module="girder_wholetale.tasks.import_git_repo",
            args=(url,),
            kwargs={
                "taleId": tale["_id"],
                "spawn": spawn,
                "change_status": change_status
            },
            otherFields={
                "taleId": tale["_id"],
                "wt_notification_id": str(notification["_id"])
            },
        )
        Job().scheduleJob(job)
        return tale

    def restoreTale(self, manifest: dict, environment: dict):
        """
        Restore a Tale from manifest/environment JSON.

        NOTE: it will be missing a lot of keywords that makes it a model,
        e.g. _id, acls etc.
        """
        mp = ManifestParser(manifest)
        restored_tale = mp.get_tale_fields()
        restored_tale.update(mp.get_tale_fields_from_environment(environment))
        restored_tale["dataSet"] = mp.get_dataset()
        return restored_tale
