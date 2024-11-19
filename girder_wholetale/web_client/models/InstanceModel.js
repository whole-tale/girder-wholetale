const AccessControlledModel = girder.models.AccessControlledModel;

var InstanceModel = AccessControlledModel.extend({
    resourceName: 'instance'
});

export default InstanceModel;
