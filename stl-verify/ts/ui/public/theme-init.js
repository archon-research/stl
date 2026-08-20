// Applies the stored theme before first paint so a dark-mode reload does not
// flash the light palette while React loads (ThemeProvider only applies the
// class in a post-paint effect). Mirrors ThemeProvider's storage contract
// exactly: keys `theme`/`archon-theme`, values light|dark|auto, applied as the
// `dark` class plus `data-theme`. Loaded as a classic blocking script from
// <head>; the CSP allows script-src 'self' only, so this cannot be inlined.
(function () {
  var mode;
  try {
    mode =
      window.localStorage.getItem('theme') ||
      window.localStorage.getItem('archon-theme');
  } catch (_error) {
    mode = null;
  }

  if (mode !== 'light' && mode !== 'dark') {
    mode = window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light';
  }

  document.documentElement.classList.toggle('dark', mode === 'dark');
  document.documentElement.dataset.theme = mode;
})();
