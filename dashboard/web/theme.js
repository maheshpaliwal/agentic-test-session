'use strict';
/* Runs in <head> before first paint so the chosen theme never flashes. Order: ?theme= param, saved choice, OS preference, dark. */
(function () {
  var t = null;
  try {
    var m = /(?:^|[?&])theme=(light|dark)(?:&|$)/.exec(location.search + '&' + (location.hash.split('?')[1] || ''));
    if (m) { t = m[1]; localStorage.setItem('tsession.theme', t); }
    else t = localStorage.getItem('tsession.theme');
  } catch (e) { /* storage may be unavailable */ }
  if (t !== 'light' && t !== 'dark') {
    t = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  document.documentElement.setAttribute('data-theme', t);
})();
