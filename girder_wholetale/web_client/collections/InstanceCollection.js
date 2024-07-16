import InstanceModel from '../models/InstanceModel';

const Collection = girder.collections.Collection;

var InstanceCollection = Collection.extend({
    resourceName: 'instance',
    model: InstanceModel
});

export default InstanceCollection;
