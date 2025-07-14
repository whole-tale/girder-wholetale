import BannerTemplate from '../templates/alertBanner.pug';
import '../stylesheets/alertBanner.styl';

const HeaderView = girder.views.layout.HeaderView;
const { wrap } = girder.utilities.PluginUtils;
const { restRequest } = girder.rest;

wrap(HeaderView, 'render', function (render) {
  render.call(this);

  if (!this.maintenanceBannerAdded) {
    restRequest({
      method: 'GET',
      url: 'system/banner',
    }).done((resp) => {
      if (resp && resp.text) {
        const customHeaderHtml = BannerTemplate({text: resp.text, level: resp.level || 'info'});
        this.$el.find('.g-header-wrapper').append(customHeaderHtml);
        const maintenanceBanner = document.getElementById('maintenance-banner');
        const closeBannerBtn = document.getElementById('close-banner-btn');
        // Check if the banner was already closed in this session
        if (sessionStorage.getItem('maintenanceBannerClosed') === 'true') {
          maintenanceBanner.style.display = 'none';
        }
        // Add event listener to the close button
        closeBannerBtn.addEventListener('click', function() {
          maintenanceBanner.style.display = 'none';
          // Store the closed state in sessionStorage
          sessionStorage.setItem('maintenanceBannerClosed', 'true');
        });
      }
    }).fail(() => {
      console.error('Failed to fetch system banner.');
    });
    this.maintenanceBannerAdded = true;
  }

  return this;
});
