import ConfigView from './views/ConfigView';
import InstanceListWidget from './views/InstanceListWidget';
import LaunchTaleView from './views/body/LaunchTaleView';
import ExtKeysView from './views/ExtKeysView';

const events = girder.events;
const router = girder.router;
const { exposePluginConfig } = girder.utilities.PluginUtils;

exposePluginConfig('wholetale', 'plugins/wholetale/config');

router.route('plugins/wholetale/config', 'wholetaleConfig', function () {
    events.trigger('g:navigateTo', ConfigView);
});

router.route('instance/user/:id', 'instanceList', function (id) {
    events.trigger('g:navigateTo', InstanceListWidget, {
        filter: {userId: id}
    });
});

router.route('launch', 'launchTale', (params) => {
    events.trigger('g:navigateTo', LaunchTaleView, {
        url: params.url
    });
});

router.route('ext_keys', 'extKeys', () => {
    events.trigger('g:navigateTo', ExtKeysView);
});
