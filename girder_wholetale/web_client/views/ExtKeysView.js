import ExtKeyView from './ExtKeyDialog';
import ExtKeysViewTemplate from '../templates/extKeysView.pug';
import '../stylesheets/extKeysView.styl';

const $ = girder.$;
const _ = girder._;
const View = girder.views.View;
const { getCurrentToken, cookie } = girder.auth;
const { restRequest } = girder.rest;
const { splitRoute } = girder.misc;

const parseJwt = (token) => {
    try {
        return JSON.parse(atob(token.split('.')[1]));
    } catch (e) {
        return null;
    }
};

var ExtKeysView = View.extend({
    events: {
        'click .g-oauth-button': function (event) {
            var providerId = $(event.currentTarget).attr('g-provider');
            var provider = _.findWhere(this.providers, {name: providerId});
            if (provider.state === 'authorized') {
                restRequest({
                    url: 'account/' + provider.name + '/revoke'
                }).done((resp) => {
                    this.render();
                });
            } else {
                window.location = provider.url;
            }
        },

        'click .g-apikey-button': function (event) {
            var container = $('#g-dialog-container');
            var providerId = $(event.currentTarget).attr('g-provider');
            this.addApiKeyView = new ExtKeyView({
                el: container,
                parentView: this,
                provider: providerId
            });
            this.addApiKeyView.render();
        },

        'click .g-key-provider-delete-button': function (event) {
            var provider = $(event.currentTarget).attr('g-provider');
            var resourceServer = $(event.currentTarget).attr('g-resource');
            restRequest({
                url: 'account/' + provider + '/revoke',
                data: {
                    resource_server: resourceServer
                }
            }).done((resp) => {
                this.render();
            });
        }
    },

    initialize: function (settings) {
        this.redirect = settings.redirect || splitRoute(window.location.href).base;
        this.token = getCurrentToken() || cookie.find('girderToken');
        this.modeText = settings.modeText || 'authorize';
        this.providers = null;
        this.enablePasswordLogin = _.has(settings, 'enablePasswordLogin') ? settings.enablePasswordLogin : true;
        this.render();
    },

    render: function () {
        restRequest({
            url: 'account',
            data: {
                redirect: this.redirect
            }
        }).done((resp) => {
            this.providers = resp;
            if (this.providers === null) {
                return this;
            }

            var buttons = [];
            var revokeButtons = [];
            var keyProviders = [];
            _.each(this.providers, function (provider) {
                var btn = this._buttons[provider.name];
                if (btn) {
                    revokeButtons.push(btn);
                } else {
                    keyProviders.push(provider);
                }
            }, this);

            if (buttons.length || revokeButtons.length) {
                this.$el.html(ExtKeysViewTemplate({
                    modeText: this.modeText,
                    buttons: buttons,
                    revokeButtons: revokeButtons,
                    keyProviders: keyProviders,
                    enablePasswordLogin: this.enablePasswordLogin
                }));
            }

            return this;
        });
    },

    _buttons: {
        orcid: {
            icon: 'orcid',
            class: 'g-oauth-button-orcid'
        },
        globus: {
            icon: 'globus',
            class: 'g-oauth-button-globus'
        },
        box: {
            icon: 'box',
            class: 'g-oauth-button-box'
        }
    }
});

export default ExtKeysView;
