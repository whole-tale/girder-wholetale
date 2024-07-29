import ConfigViewTemplate from '../templates/configView.pug';
import '../stylesheets/configView.styl';

import 'bootstrap-switch'; // /dist/js/bootstrap-switch.js',
import 'bootstrap-switch/dist/css/bootstrap3/bootstrap-switch.css';

const _ = girder._;
const { restRequest, getApiRoot } = girder.rest;
const events = girder.events;
const View = girder.views.View;
const UploadWidget = girder.views.widgets.UploadWidget;
const FolderModel = girder.models.FolderModel;
const PluginConfigBreadcrumbWidget = girder.views.widgets.PluginConfigBreadcrumbWidget;

var ConfigView = View.extend({
    events: {
        'submit #g-wholetale-config-form': function (event) {
            event.preventDefault();
            this.$('#g-wholetale-error-message').empty();

            this._saveSettings([{
                key: 'wholetale.website_url',
                value: this.$('#wholetale_website_url').val()
            }, {
                key: 'wholetale.dashboard_link_title',
                value: this.$('#wholetale_dashboard_link_title').val()
            }, {
                key: 'wholetale.catalog_link_title',
                value: this.$('#wholetale_catalog_link_title').val()
            }, {
                key: 'wholetale.enable_data_catalog',
                value: this.$('#wholetale_enable_data_catalog').is(':checked')
            }, {
                key: 'wholetale.about_href',
                value: this.$('#wholetale_about_href').val()
            }, {
                key: 'wholetale.contact_href',
                value: this.$('#wholetale_contact_href').val()
            }, {
                key: 'wholetale.bug_href',
                value: this.$('#wholetale_bug_href').val()
            }, {
                key: 'wholetale.logo',
                value: this.logoFileId
            }, {
                key: 'wholetale.instance_cap',
                value: this.$('#wholetale_instance_cap').val()
            }, {
                key: 'wholetale.dataverse_url',
                value: this.$('#wholetale_dataverse_url').val()
            }, {
                key: 'wholetale.dataverse_extra_hosts',
                value: this.$('#wholetale_extra_hosts').val().trim()
            }, {
                key: 'wholetale.external_auth_providers',
                value: this.$('#wholetale_external_auth_providers').val().trim()
            }, {
                key: 'wholetale.external_apikey_groups',
                value: this.$('#wholetale_external_apikey_groups').val().trim()
            }, {
                key: 'wholetale.publisher_repositories',
                value: this.$('#wholetale_publisher_repositories').val().trim()
            }, {
                key: 'wholetale.homes_root',
                value: this.$('#wholetale-homes-root').val().trim()
            }, {
                key: 'wholetale.workspaces_root',
                value: this.$('#wholetale-workspaces-root').val().trim()
            }, {
                key: 'wholetale.versions_root',
                value: this.$('#wholetale-versions-root').val().trim()
            }, {
                key: 'wholetale.runs_root',
                value: this.$('#wholetale-runs-root').val().trim()
            }, {
                key: 'wholetale.dav_server',
                value: this.$('#wholetale-enable-dav-server').is(':checked')
            }]);
        },

        'click #g-wholetale-logo-reset': function (event) {
            this.logoFileId = null;
            this._updateLogoDisplay();
        }
    },
    initialize: function () {
        this.breadcrumb = new PluginConfigBreadcrumbWidget({
            pluginName: 'WholeTale',
            parentView: this
        });

        var keys = [
            'wholetale.website_url',
            'wholetale.dashboard_link_title',
            'wholetale.catalog_link_title',
            'wholetale.enable_data_catalog',
            'wholetale.about_href',
            'wholetale.contact_href',
            'wholetale.bug_href',
            'wholetale.logo',
            'wholetale.instance_cap',
            'wholetale.dataverse_url',
            'wholetale.dataverse_extra_hosts',
            'wholetale.external_auth_providers',
            'wholetale.external_apikey_groups',
            'wholetale.publisher_repositories',
            'wholetale.homes_root',
            'wholetale.workspaces_root',
            'wholetale.versions_root',
            'wholetale.runs_root',
            'wholetale.dav_server'
        ];

        restRequest({
            url: 'system/setting',
            type: 'GET',
            data: {
                list: JSON.stringify(keys),
                default: 'none'
            }
        }).done(_.bind(function (resp) {
            this.settings = resp;
            restRequest({
                url: 'system/setting',
                type: 'GET',
                data: {
                    list: JSON.stringify(keys),
                    default: 'default'
                }
            }).done(_.bind(function (resp) {
                this.defaults = resp;
                restRequest({
                    method: 'GET',
                    url: 'wholetale/assets'
                }).done(_.bind(function (resp) {
                    this.logoFileId = this.settings['wholetale.logo'];

                    this.logoUploader = new UploadWidget({
                        parent: new FolderModel({_id: resp['wholetale.logo']}),
                        parentType: 'folder',
                        title: 'Dashboard Logo',
                        modal: false,
                        multiFile: false,
                        parentView: this
                    });
                    this.listenTo(this.logoUploader, 'g:uploadFinished', (event) => {
                        this.logoFileId = event.files[0].id;
                        this._updateLogoDisplay();
                    });

                    this.render();
                }, this));
            }, this));
        }, this));
    },

    _updateLogoDisplay: function () {
        let logoUrl;
        if (this.logoFileId) {
            logoUrl = `${getApiRoot()}/file/${this.logoFileId}/download?contentDisposition=inline`;
            this.$('.g-wholetale-logo-preview img').attr('src', logoUrl);
        }
    },

    render: function () {
        this.$el.html(ConfigViewTemplate({
            settings: this.settings,
            defaults: this.defaults,
            JSON: window.JSON
        }));
        this.breadcrumb.setElement(this.$('.g-config-breadcrumb-container')).render();

        this.logoUploader
            .render()
            .$el.appendTo(this.$('.g-wholetale-logo-upload-container'));
        this._updateLogoDisplay();

        this.$('.g-setting-switch').bootstrapSwitch();

        return this;
    },

    _saveSettings: function (settings) {
        restRequest({
            method: 'PUT',
            url: 'system/setting',
            data: {
                list: JSON.stringify(settings)
            },
            error: null
        }).done(() => {
            events.trigger('g:alert', {
                icon: 'ok',
                text: 'Settings saved.',
                type: 'success',
                timeout: 3000
            });
        }).fail((resp) => {
            this.$('#g-wholetale-error-message').text(resp.responseJSON.message);
        });
    }
});

export default ConfigView;
