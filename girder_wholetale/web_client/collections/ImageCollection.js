import Collection from '@girder/core/collections/Collection';

import ImageModel from '../models/ImageModel';

var ImageCollection = Collection.extend({
    resourceName: 'image',
    model: ImageModel
});

export default ImageCollection;
