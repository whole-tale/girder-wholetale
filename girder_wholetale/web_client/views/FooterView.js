import LayoutFooterTemplate from '../templates/layoutFooter.pug';
// import '@girder/core/stylesheets/layout/footer.styl';

const FooterView = girder.views.layout.FooterView;
const { getApiRoot, restRequest } = girder.rest;
const { wrap } = girder.utilities.PluginUtils;

wrap(FooterView, 'initialize', function (initialize, ...args) {
    this.aboutHref = 'https://wholetale.org/';
    this.contactHref = 'https://groups.google.com/forum/#!forum/wholetale';
    this.bugHref = 'https://github.com/whole-tale/whole-tale/issues/new';
    initialize.apply(this, args);

    if (!this.wtSettings) {
        restRequest({
            url: 'wholetale/settings',
            method: 'GET'
        }).done((resp) => {
            this.wtSettings = resp;
            this.aboutHref = resp['wholetale.about_href'];
            this.contactHref = resp['wholetale.contact_href'];
            this.bugHref = resp['wholetale.bug_href'];
            this.render();
        });
    }
});

wrap(FooterView, 'render', function (render) {
    let apiRoot = getApiRoot();
    if (apiRoot.substring(0, 1) !== '/') {
        apiRoot = '/' + apiRoot;
    }
    this.$el.html(LayoutFooterTemplate({
        apiRoot: apiRoot,
        aboutHref: this.aboutHref,
        contactHref: this.contactHref,
        bugLink: this.bugHref
    }));
    return this;
});
