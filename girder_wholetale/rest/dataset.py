#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import tempfile

import cherrypy
from bson import ObjectId
from girder.api import access
from girder.api.docs import addModel
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import Resource, filtermodel, iterBody
from girder.constants import AccessType, SortDir, TokenScope
from girder.exceptions import ValidationException, RestException
from girder.models.item import Item
from girder.models.folder import Folder
from girder.models.user import User
from girder.utility.model_importer import ModelImporter
from girder_jobs.models.job import Job

from ..constants import CATALOG_NAME
from ..lib import IMPORT_PROVIDERS
from ..lib.data_map import DataMap
from ..schema.misc import dataMapListSchema
from ..utils import getOrCreateRootFolder, init_progress


datasetModel = {
    "description": "Object representing registered data.",
    "required": [
        "_id",
        "_modelType"
    ],
    "properties": {
        "_id": {
            "type": "string",
            "description": "internal unique identifier"
        },
        "name": {
            "type": "string",
            "description": "A user-friendly name"
        },
        "description": {
            "type": "string"
        },
        "_modelType": {
            "type": "string",
            "description": "Model of the object.",
            "enum": [
                "folder",
                "item"
            ]
        },
        "created": {
            "type": "string",
            "format": "date-time",
            "description": "The time when the tale was created."
        },
        "creatorId": {
            "type": "string",
            "description": "A unique identifier of the user that created the tale."
        },
        "updated": {
            "type": "string",
            "format": "date-time",
            "description": "The last time when the tale was modified."
        },
        "size": {
            "type": "integer",
            "description": "Total size of the dataset in bytes."
        },
        "identifier": {
            "type": ["string", "null"],
            "description": "External, unique identifier"
        },
        "provider": {
            "type": "string",
            "description": "Name of the provider",
            "enum": [
                "HTTP",
                "Globus"
            ]
        }
    }
}
datasetModelKeys = set(datasetModel['properties'].keys())
addModel('dataset', datasetModel, resources='dataset')


def _itemOrFolderToDataset(obj):
    ds = {key: obj[key] for key in obj.keys() & datasetModelKeys}
    ds['provider'] = obj['meta'].get('provider', 'unknown')
    ds['identifier'] = obj['meta'].get('identifier', 'unknown')
    return ds


class Dataset(Resource):

    def __init__(self):
        super(Dataset, self).__init__()
        self.resourceName = 'dataset'

        self.route('GET', (), self.listDatasets)
        self.route('GET', (':id',), self.getDataset)
        self.route('DELETE', (':id',), self.deleteUserDataset)
        self.route('POST', ('register',), self.importData)
        self.route('POST', ('importBag',), self.importBDBag)

    @access.public
    @autoDescribeRoute(
        Description(('Returns all registered datasets from the system '
                     'that user has access to'))
        .param('myData', 'If True, filters results to datasets registered by the user.'
               'Defaults to False.',
               required=False, dataType='boolean', default=False)
        .jsonParam('identifiers', 'Filter datasets by an identifier', required=False,
                   dataType='string', requireArray=True)
        .responseClass('dataset', array=True)
        .pagingParams(defaultSort='lowerName',
                      defaultSortDir=SortDir.ASCENDING)
    )
    def listDatasets(self, myData, identifiers, limit, offset, sort):
        user = self.getCurrentUser()
        datasets = []

        filters = {}
        if myData and user:
            filters = {'_id': {'$in': user.get('myData', [])}}

        if identifiers:
            filters.update(
                {
                    "$or": [
                        {"meta.directIdentifier": {"$in": identifiers}},
                        {"meta.identifier": {"$in": identifiers}},
                    ]
                }
            )

            for modelType in ('folder', 'item'):
                for obj in ModelImporter.model(modelType).find(filters):
                    obj['_modelType'] = modelType
                    datasets.append(_itemOrFolderToDataset(obj))
            return datasets

        parent = getOrCreateRootFolder(CATALOG_NAME)
        for folder in Folder().childFolders(
                parentType='folder', parent=parent, user=user,
                limit=limit, offset=offset, sort=sort, filters=filters):
            folder['_modelType'] = 'folder'
            datasets.append(_itemOrFolderToDataset(folder))

        if myData:
            cursor = Item().find(filters)
            for item in Item().filterResultsByPermission(
                    cursor, user, AccessType.READ, limit=limit, offset=offset
            ):
                item['_modelType'] = 'item'
                datasets.append(_itemOrFolderToDataset(item))
        return datasets

    def _getResource(self, id, type):
        model = self._getResourceModel(type)
        return model.load(id=id, user=self.getCurrentUser(), level=AccessType.READ)

    @access.public
    @autoDescribeRoute(
        Description('Get any registered dataset by ID.')
        .param('id', 'The ID of the Dataset.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the resource.', 403)
    )
    def getDataset(self, id, params):
        user = self.getCurrentUser()
        try:
            doc = Folder().load(id=id, user=user, level=AccessType.READ, exc=True)
            doc['_modelType'] = 'folder'
        except ValidationException:
            doc = Item().load(id=id, user=user, level=AccessType.READ, exc=True)
            doc['_modelType'] = 'item'
        if 'meta' not in doc or 'provider' not in doc['meta']:
            raise ValidationException('No such item: %s' % str(doc['_id']), 'id')
        return _itemOrFolderToDataset(doc)

    @access.user
    @autoDescribeRoute(
        Description("Remove user's reference to a registered dataset")
        .param('id', 'The ID of the Dataset.', paramType='path')
    )
    def deleteUserDataset(self, id):
        user = self.getCurrentUser()
        user_data = set(user.get('myData', []))
        user['myData'] = list(user_data.difference({ObjectId(id)}))
        user = User().save(user)

    @access.user(scope=TokenScope.DATA_WRITE)
    @filtermodel(model=Job)
    @autoDescribeRoute(
        Description('Create a folder containing references to an external data')
        .param('parentId', 'Parent ID for the new parent of this folder.',
               required=False)
        .param('parentType', "Type of the folder's parent", required=False,
               enum=['folder', 'user', 'collection'], strip=True, default='folder')
        .param('public', 'Whether the folder should be publicly visible. '
               'Defaults to True.',
               required=False, dataType='boolean', default=True)
        .jsonParam('dataMap', 'A list of data mappings',
                   paramType='body', schema=dataMapListSchema)
        .errorResponse('Write access denied for parent collection.', 403)
    )
    def importData(self,
                   parentId,
                   parentType,
                   public,
                   dataMap,
                   params):
        user = self.getCurrentUser()
        if not parentId or parentType not in ('folder', 'item'):
            parent = getOrCreateRootFolder(CATALOG_NAME)
            parentType = 'folder'
        else:
            parent = ModelImporter.model(parentType).load(
                parentId, user=user, level=AccessType.WRITE, exc=True)

        try:
            for data in DataMap.fromList(dataMap):
                provider = IMPORT_PROVIDERS.getFromDataMap(data)
                provider.check_auth(user)
        except ValueError:
            raise RestException(
                f"To register data from {provider.name} you need to provide credentials."
            )

        resource = {
            'type': 'wt_register_data',
            'dataMap': dataMap,
        }
        notification = init_progress(
            resource, user, 'Registering Data',
            'Initialization', 2)

        job = self._createImportJob(dataMap, parent, parentType, user, notification)
        Job().scheduleJob(job)
        return job

    @access.user(scope=TokenScope.DATA_WRITE)
    @autoDescribeRoute(
        Description('Imports a BDBag.')
        .notes('Files that exist in the bag are imported directly, whereas references are only '
               'stored by reference and fetched on-demand.')
        .param('parentId', 'Parent ID for the new parent of this folder.',
               required=False)
        .param('parentType', "Type of the folder's parent", required=False,
               enum=['folder', 'user', 'collection'], strip=True, default='folder')
        .param('public', 'Whether the folder should be publicly visible. '
               'Defaults to True.',
               required=False, dataType='boolean', default=True)
        .errorResponse('Write access denied for parent collection.', 403)
    )
    def importBDBag(self, parentId, parentType, public, params):
        user = self.getCurrentUser()
        if not parentId or parentType not in ('folder', 'item'):
            parent = getOrCreateRootFolder(CATALOG_NAME)
            parentType = 'folder'
        else:
            parent = ModelImporter.model(parentType).load(
                parentId, user=user, level=AccessType.WRITE, exc=True)

        if cherrypy.request.headers.get('Content-Type') == 'application/zip':
            return self._importBDBagFromStream(iterBody, parent, parentType, public, user)
        else:
            raise RestException('Missing bag data')

    def _importBDBagFromStream(self, stream, parent, parentType, public, user=None):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            for data in stream():
                f.write(data)
            f.seek(0)

            path = f.name

        try:
            dataMap = {'dataId': path, 'repository': 'BDBag'}
            resource = {
                'type': 'wt_register_data',
                'dataMap': dataMap,
            }
            notification = init_progress(
                resource, user, 'Importing BDBag',
                'Initialization', 2)

            job = self._createImportJob([dataMap], parent, parentType, user, notification)
            Job().scheduleJob(job)
            return job
        finally:
            # This isn't right; the removal needs to be done after the job completes, which may be
            # after control reaches this line.
            # It happens to work if the job is a synchronous job, which it seems to be for now.
            os.unlink(path)

    def _createImportJob(self, data_maps, parent, parentType, user, notification):
        job = Job().createLocalJob(
            title='Registering Data', user=user,
            type='wholetale.register_data', public=False, asynchronous=False,
            module='girder_wholetale.tasks.register_dataset',
            args=(data_maps, parent, parentType, user),
            otherFields={'wt_notification_id': str(notification['_id'])},
        )
        return job
