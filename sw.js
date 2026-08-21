/* Shift Tracker Pro — Service Worker v4
   1. App-shell caching: the app opens even with no internet at all.
   2. Web Push: reminders & messages arrive even when the app is closed. */

const SHELL_CACHE = 'st-shell-v9';
const SHELL_FILES = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png'
];

// ── install: cache the app shell ──
self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(SHELL_CACHE)
      .then((cache) => Promise.allSettled(SHELL_FILES.map((f) => cache.add(f))))
      .then(() => self.skipWaiting())
  );
});

// ── activate: clean old caches ──
self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== SHELL_CACHE).map((k) => caches.delete(k))))
      .then(() => clients.claim())
  );
});

// ── fetch strategy ──
self.addEventListener('fetch', (e) => {
  if (e.request.method !== 'GET') return;
  const url = new URL(e.request.url);
  if (url.origin !== location.origin) return;            // fonts etc: let the browser handle
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws')) return; // live data: network only

  // Page navigations: try the network first (fresh version), fall back to cache when offline
  if (e.request.mode === 'navigate') {
    e.respondWith(
      fetch(e.request)
        .then((res) => {
          const copy = res.clone();
          caches.open(SHELL_CACHE).then((c) => c.put('/index.html', copy));
          return res;
        })
        .catch(() => caches.match('/index.html').then((r) => r || caches.match('/')))
    );
    return;
  }

  // Static assets: cache-first, then network (and cache for next time)
  e.respondWith(
    caches.match(e.request).then((cached) => cached || fetch(e.request).then((res) => {
      const copy = res.clone();
      caches.open(SHELL_CACHE).then((c) => c.put(e.request, copy));
      return res;
    }))
  );
});

// ── web push: OS notifications even when the app is closed ──
self.addEventListener('push', (e) => {
  let data = {};
  try { data = e.data ? e.data.json() : {}; }
  catch (err) { data = { title: 'Shift Tracker', body: e.data ? e.data.text() : '' }; }

  e.waitUntil(self.registration.showNotification(data.title || 'Shift Tracker', {
    body: data.body || '',
    icon: '/icons/icon-192.png',
    badge: '/icons/icon-192.png',
    tag: data.tag || ('st-' + Date.now()),
    renotify: true,
    vibrate: [250, 150, 250, 150, 250],
    requireInteraction: !!data.urgent,
    data: { url: data.url || '/' }
  }));
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const targetUrl = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if ('focus' in c) return c.focus();
      }
      return clients.openWindow(targetUrl);
    })
  );
});
