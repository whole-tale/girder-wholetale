import HeaderLogoTemplate from '../templates/headerLogo.pug';
import HeaderLinkTemplate from '../templates/headerLink.pug';
import HeaderUserViewMenuTemplate from '../templates/headerUserViewMenu.pug';
import '../stylesheets/header.styl';

const $ = girder.$;
const HeaderUserView = girder.views.layout.HeaderUserView;
const { getCurrentUser, getCurrentToken } = girder.auth;
const { restRequest, getApiRoot } = girder.rest;
const { wrap } = girder.utilities.PluginUtils;

/**
 * Customize the header view
 */
wrap(HeaderUserView, 'render', function (render) {
    render.call(this);

    // Update based on branding configuration
    if (!this.branded) {
        restRequest({
            method: 'GET',
            url: 'wholetale/settings'
        }).done((resp) => {
            let logoUrl = '';
            if (resp['wholetale.logo']) {
                logoUrl = `${getApiRoot()}/${resp['wholetale.logo']}`;
            }
            // parse the dashboard URL and append girderToken as a query parameter
            let dashboardUrl = URL.parse(resp['wholetale.dashboard_url']);
            dashboardUrl.searchParams.set('girderToken', getCurrentToken());

            let title = resp['wholetale.dashboard_link_title'];
            let bannerColor = resp['core.banner_color'];

            if (!$('.g-app-logo').length) {
                $('.g-app-title').prepend(HeaderLogoTemplate({ logoUrl: logoUrl }));
            }
            if (!$('.g-dashboard-link').length) {
                $('.g-quick-search-form').after(HeaderLinkTemplate({
                    dashboardUrl: dashboardUrl.href,
                    title: title }));
                document.getElementsByClassName('g-header-wrapper')[0].style.backgroundColor = bannerColor;
            }
            this.branded = true;
        });
    }

    // Add an entry to the user dropdown menu to navigate to user's ext keys
    var currentUser = getCurrentUser();
    if (currentUser) {
        this.$('#g-user-action-menu>ul').prepend(HeaderUserViewMenuTemplate({
            href: '#ext_keys'
        }));
    }
    return this;
});
