import CollectionsViewTemplate from '../templates/collectionsView.pug';

import 'bootstrap-switch'; // /dist/js/bootstrap-switch.js',
import 'bootstrap-switch/dist/css/bootstrap3/bootstrap-switch.css';

import '../stylesheets/collectionsView.styl';

const CollectionsView = girder.views.body.CollectionsView;
const { wrap } = girder.utilities.PluginUtils;

var reFiltered = /^((?!(WholeTale)).)*$/;
var enableHiddenCollections = false;

wrap(CollectionsView, 'initialize', function (initialize, ...args) {
    initialize.apply(this, args);

    if (!enableHiddenCollections) {
        this.collection.filterFunc = function (collection) {
            return collection.name.match(reFiltered);
        };
    }
});

wrap(CollectionsView, 'render', function (render) {
    render.call(this);
    this.$('.g-collection-pagination').before(CollectionsViewTemplate());
    this.$('.g-plugin-switch')
        .bootstrapSwitch()
        .bootstrapSwitch('state', enableHiddenCollections)
        .off('switchChange.bootstrapSwitch')
        .on('switchChange.bootstrapSwitch', (event, state) => {
            if (state === true) {
                this.collection.filterFunc = null;
            } else {
                this.collection.filterFunc = function (collection) {
                    return collection.name.match(reFiltered);
                };
            }
            enableHiddenCollections = state;
            this.collection.fetch({}, true);
        });
    return this;
});
