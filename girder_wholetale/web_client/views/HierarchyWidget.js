import WholeTaleHierarchyWidget from '../templates/WholeTaleHierarchyWidget.pug';

const $ = girder.$;
const { wrap } = girder.utilities.PluginUtils;
const { getCurrentUser } = girder.auth;
const { getPublicSettings } = girder.rest;
const HierarchyWidget = girder.views.widgets.HierarchyWidget;

wrap(HierarchyWidget, 'render', function (render) {
    var widget = this;
    const folderHeader = widget.$('.g-folder-header-buttons');
    const publicSettings = getPublicSettings() || {};
    const isFolder = widget.parentModel.resourceName === 'folder';
    const isWholeTaleEnabled = publicSettings['wholetale.girder_ui_enabled'] === true;
    const isUserLoggedIn = getCurrentUser() !== null;
    const isFolderHeaderPresent = folderHeader.length > 0;

    if (isUserLoggedIn && isFolder && isFolderHeaderPresent && isWholeTaleEnabled) {
        render.call(widget);
        $(WholeTaleHierarchyWidget()).prependTo(widget.$('.g-folder-header-buttons'));
        document.getElementsByClassName('g-createTale-button')[0].style.display = 'inline';
    } else {
        render.call(widget);
    }
});

function _analyzeInWT(e) {
    let subdomain = window.location.host.split('.')[0];
    let dashboardUrl = window.location.origin.replace(subdomain, 'dashboard') + '/mine';
    const params = new URLSearchParams();
    params.set('name', 'My Tale');
    params.set('asTale', false);
    const dataSet = [
        {
            'itemId': this.parentModel.id,
            'mountPath': '/',
            '_modelType': 'folder'
        }
    ];
    params.set('dataSet', JSON.stringify(dataSet));
    window.location.assign(dashboardUrl + `?${params.toString()}`);
}

HierarchyWidget.prototype.events['click .g-createTale-button'] = _analyzeInWT;
