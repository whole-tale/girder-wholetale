import ImageModel from '../models/ImageModel';

const Collection = girder.collections.Collection;

var ImageCollection = Collection.extend({
    resourceName: 'image',
    model: ImageModel
});

export default ImageCollection;
