/**
 * ═══════════════════════════════════════════════════════════════
 *  SHIFT TRACKER PRO — SERVER  (v1.1: avatars, delete account)
 * ═══════════════════════════════════════════════════════════════
 *  What it does:
 *   • Hosts the app itself (serves public/index.html)
 *   • Real accounts (register / login with Gmail + password)
 *   • Syncs each user's data (shifts, notes, reminders, settings)
 *     so it follows them on every device
 *   • Real friends: find by Gmail, send/accept/decline requests
 *   • Real-time chat + online presence over WebSockets
 *   • Pushes due reminders to connected devices over the socket
 *
 *  Run:        node server.js        (defaults to port 8000)
 *  Deploy:     see README.md (Render / Railway free tier)
 *  Storage:    data.json in this folder (auto-created)
 * ═══════════════════════════════════════════════════════════════
 */

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const webpush = require('web-push');

const PORT = process.env.PORT || 8000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');

// ────────────────────────── tiny JSON database ──────────────────────────
let db = {
  users: [],            // { id, name, email, passHash, salt, hourlyWage, avatar, createdAt, lastSeen }
  sessions: {},         // token -> { userId, createdAt }
  states: {},           // userId -> { shifts, notes, reminders, settings }
  friendRequests: [],   // { id, fromId, toId, status: 'pending'|'accepted'|'declined', createdAt }
  messages: [],         // { id, fromId, toId, text, imageUrl, timestamp, read }
  pushedReminders: {},  // userId -> [reminderId,...] already pushed by server
  pushSubs: {},         // userId -> { endpoint: subscription }
  vapid: null           // { publicKey, privateKey } generated once
};

// ── optional GitHub-backed persistence ────────────────────────────────
// Render's free plan has an ephemeral disk (data wipes on restart/sleep).
// With these env vars set, the server restores data.json from your GitHub
// repo on boot and pushes a backup there after changes — data never dies.
//   GH_TOKEN  fine-grained PAT, Contents: Read&Write on your repo
//   GH_REPO   e.g. "yourname/shift-tracker-pro"
//   GH_PATH   file path in the repo (default: backup/data.json)
//   GH_BRANCH branch (default: main)
const GH_TOKEN = process.env.GH_TOKEN || '';
const GH_REPO = process.env.GH_REPO || '';
const GH_PATH = process.env.GH_PATH || 'backup/data.json';
const GH_BRANCH = process.env.GH_BRANCH || 'main';
const ghEnabled = !!(GH_TOKEN && GH_REPO);
let ghSyncTimer = null, ghSha = null, ghSyncRunning = false;
const ghState = { lastOk: null, lastError: null };

async function ghApi(method, body) {
  const sep = method === 'GET' ? '?ref=' + GH_BRANCH : '';
  return fetch('https://api.github.com/repos/' + GH_REPO + '/contents/' + GH_PATH + sep, {
    method,
    headers: {
      'Authorization': 'Bearer ' + GH_TOKEN,
      'Accept': 'application/vnd.github+json',
      'Content-Type': 'application/json',
      'User-Agent': 'shift-tracker-pro'
    },
    body: body ? JSON.stringify(body) : undefined
  });
}

async function ghRestore() {
  if (!ghEnabled) return;
  try {
    const r = await ghApi('GET');
    if (!r.ok) { console.log('☁️ GitHub backup: none yet (status ' + r.status + ')'); return; }
    const j = await r.json();
    ghSha = j.sha || null;
    const raw = Buffer.from(String(j.content || '').replace(/\n/g, ''), 'base64').toString('utf8');
    const parsed = JSON.parse(raw);
    if (parsed && Array.isArray(parsed.users)) {
      db = Object.assign(db, parsed);
      console.log('☁️ Restored from GitHub:', db.users.length, 'users,', (db.messages || []).length, 'messages');
    }
  } catch (e) { console.log('☁️ GitHub restore failed (continuing):', e.message); }
}

function ghSchedulePush() {
  if (!ghEnabled) return;
  clearTimeout(ghSyncTimer);
  ghSyncTimer = setTimeout(ghPush, 60000); // at most ~1 push/min, after changes settle
}

async function ghPush() {
  if (!ghEnabled || ghSyncRunning) return;
  ghSyncRunning = true;
  try {
    const body = {
      message: 'data backup ' + new Date().toISOString(),
      content: Buffer.from(JSON.stringify(db), 'utf8').toString('base64'),
      branch: GH_BRANCH
    };
    if (ghSha) body.sha = ghSha;
    let r = await ghApi('PUT', body);
    if (r.status === 409 || r.status === 422) { // stale sha — refresh and retry once
      try {
        const g = await ghApi('GET');
        if (g.ok) { ghSha = (await g.json()).sha; body.sha = ghSha; r = await ghApi('PUT', body); }
      } catch (e) {}
    }
    if (r.ok) {
      try { const j = await r.json(); if (j.content && j.content.sha) ghSha = j.content.sha; } catch (e) {}
      ghState.lastOk = new Date().toISOString(); ghState.lastError = null;
      console.log('☁️ GitHub backup pushed');
    } else {
      ghState.lastError = 'HTTP ' + r.status;
      console.log('☁️ GitHub backup failed:', r.status);
    }
  } catch (e) { ghState.lastError = String(e.message || e); console.log('☁️ GitHub backup error:', e.message); }
  ghSyncRunning = false;
}

function loadDB() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    db = Object.assign(db, JSON.parse(raw));
    console.log('📦 Loaded database:', db.users.length, 'users,', db.messages.length, 'messages');
  } catch (e) {
    console.log('📦 No database yet — starting fresh');
  }
}

let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db, null, 1));
    fs.renameSync(tmp, DATA_FILE);
    ghSchedulePush(); // cloud backup (no-op unless GH_TOKEN/GH_REPO are set)
  }, 200); // debounced atomic write
}

loadDB();

// VAPID keys for Web Push (generated once, then persisted) — set up during
// bootstrap, AFTER any GitHub restore so backup keys win.
async function bootstrap() {
  await ghRestore();
  // No backup in the repo yet? Push one shortly after boot — this makes it
  // verifiable right away AND protects fresh data written before any restart.
  if (ghEnabled && !ghSha) setTimeout(ghPush, 20000);
  if (!db.vapid) {
    db.vapid = webpush.generateVAPIDKeys();
    saveDB();
    console.log('🔐 Generated new VAPID key pair for push notifications');
  }
  webpush.setVapidDetails('mailto:admin@shift-tracker.local', db.vapid.publicKey, db.vapid.privateKey);
}

// ────────────────────────── helpers ──────────────────────────
const id = () => 'u_' + Date.now().toString(36) + '_' + crypto.randomBytes(6).toString('hex');
const nowISO = () => new Date().toISOString();

function hashPassword(password, salt) {
  salt = salt || crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return { salt, hash };
}

function verifyPassword(password, salt, expectedHash) {
  const { hash } = hashPassword(password, salt);
  try { return crypto.timingSafeEqual(Buffer.from(hash), Buffer.from(expectedHash)); }
  catch (e) { return false; }
}

function publicUser(u) {
  return { id: u.id, name: u.name, email: u.email, avatar: u.avatar || null, online: onlineIds.has(u.id) };
}

function auth(req, res, next) {
  const token = req.headers['x-auth-token'];
  const session = token && db.sessions[token];
  if (!session) return res.status(401).json({ error: 'Not logged in' });
  const user = db.users.find(u => u.id === session.userId);
  if (!user) return res.status(401).json({ error: 'Account not found' });
  req.user = user;
  next();
}

// friendship helpers
function areFriends(a, b) {
  return db.friendRequests.some(r =>
    r.status === 'accepted' &&
    ((r.fromId === a && r.toId === b) || (r.fromId === b && r.toId === a)));
}
function findRequestBetween(a, b) {
  return db.friendRequests.find(r =>
    r.status === 'pending' &&
    ((r.fromId === a && r.toId === b) || (r.fromId === b && r.toId === a)));
}
function friendIdsOf(userId) {
  return db.friendRequests
    .filter(r => r.status === 'accepted' && (r.fromId === userId || r.toId === userId))
    .map(r => (r.fromId === userId ? r.toId : r.fromId));
}

// ── web push helper: OS notifications even when the app is closed ──
async function pushTo(userId, payload) {
  const subs = (db.pushSubs && db.pushSubs[userId]) || {};
  const keys = Object.keys(subs);
  if (!keys.length) return { sent: 0 };
  let sent = 0;
  await Promise.all(keys.map(async key => {
    try {
      await webpush.sendNotification(subs[key], JSON.stringify(payload), { TTL: 3600, urgency: payload.urgent ? 'high' : 'normal' });
      sent++;
    } catch (err) {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) {
        delete db.pushSubs[userId][key]; // subscription is dead — prune it
        saveDB();
      } else {
        console.log('push error:', err && err.message);
      }
    }
  }));
  if (sent) console.log('🔔 pushed "' + (payload.title || '') + '" to', userId, `(${sent} device(s))`);
  return { sent };
}

// ────────────────────────── express app ──────────────────────────
const app = express();

// CORS: the app face may live on another host (e.g. Netlify) while this is the brain.
// The API is token-protected, so answering cross-origin requests is safe here.
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '6mb' })); // large enough for chat photos (data-URL)

// serve the app itself
app.use(express.static(path.join(__dirname, 'public'), { index: 'index.html', maxAge: 0 }));

// ─────────── v1.3: the brain can ALSO wear the face ───────────
// If public/ isn't deployed (Render free), the app shell + PWA assets are
// embedded right here, and the face can also live as index.html in the
// repo root. One address = the whole app, no separate static host needed.
const EMBEDDED_ASSETS = {"manifest.json":"ewogICJuYW1lIjogIlNoaWZ0IFRyYWNrZXIgUHJvIiwKICAic2hvcnRfbmFtZSI6ICJTaGlmdCBUcmFja2VyIiwKICAiZGVzY3JpcHRpb24iOiAiVHJhY2sgc2hpZnRzLCB3YWdlcyBhbmQgY29ubmVjdCB3aXRoIGNvd29ya2VycyIsCiAgInN0YXJ0X3VybCI6ICIvIiwKICAiZGlzcGxheSI6ICJzdGFuZGFsb25lIiwKICAiYmFja2dyb3VuZF9jb2xvciI6ICIjMGYxNzJhIiwKICAidGhlbWVfY29sb3IiOiAiIzFlM2E1ZiIsCiAgImljb25zIjogWwogICAgewogICAgICAic3JjIjogIi9pY29ucy9pY29uLTE5Mi5wbmciLAogICAgICAic2l6ZXMiOiAiMTkyeDE5MiIsCiAgICAgICJ0eXBlIjogImltYWdlL3BuZyIsCiAgICAgICJwdXJwb3NlIjogImFueSIKICAgIH0sCiAgICB7CiAgICAgICJzcmMiOiAiL2ljb25zL2ljb24tNTEyLnBuZyIsCiAgICAgICJzaXplcyI6ICI1MTJ4NTEyIiwKICAgICAgInR5cGUiOiAiaW1hZ2UvcG5nIiwKICAgICAgInB1cnBvc2UiOiAiYW55IgogICAgfQogIF0KfQ==","sw.js":"LyogU2hpZnQgVHJhY2tlciBQcm8g4oCUIFNlcnZpY2UgV29ya2VyIHYzCiAgIDEuIEFwcC1zaGVsbCBjYWNoaW5nOiB0aGUgYXBwIG9wZW5zIGV2ZW4gd2l0aCBubyBpbnRlcm5ldCBhdCBhbGwuCiAgIDIuIFdlYiBQdXNoOiByZW1pbmRlcnMgJiBtZXNzYWdlcyBhcnJpdmUgZXZlbiB3aGVuIHRoZSBhcHAgaXMgY2xvc2VkLiAqLwoKY29uc3QgU0hFTExfQ0FDSEUgPSAnc3Qtc2hlbGwtdjQnOwpjb25zdCBTSEVMTF9GSUxFUyA9IFsKICAnLycsCiAgJy9pbmRleC5odG1sJywKICAnL21hbmlmZXN0Lmpzb24nLAogICcvaWNvbnMvaWNvbi0xOTIucG5nJywKICAnL2ljb25zL2ljb24tNTEyLnBuZycKXTsKCi8vIOKUgOKUgCBpbnN0YWxsOiBjYWNoZSB0aGUgYXBwIHNoZWxsIOKUgOKUgApzZWxmLmFkZEV2ZW50TGlzdGVuZXIoJ2luc3RhbGwnLCAoZSkgPT4gewogIGUud2FpdFVudGlsKAogICAgY2FjaGVzLm9wZW4oU0hFTExfQ0FDSEUpCiAgICAgIC50aGVuKChjYWNoZSkgPT4gUHJvbWlzZS5hbGxTZXR0bGVkKFNIRUxMX0ZJTEVTLm1hcCgoZikgPT4gY2FjaGUuYWRkKGYpKSkpCiAgICAgIC50aGVuKCgpID0+IHNlbGYuc2tpcFdhaXRpbmcoKSkKICApOwp9KTsKCi8vIOKUgOKUgCBhY3RpdmF0ZTogY2xlYW4gb2xkIGNhY2hlcyDilIDilIAKc2VsZi5hZGRFdmVudExpc3RlbmVyKCdhY3RpdmF0ZScsIChlKSA9PiB7CiAgZS53YWl0VW50aWwoCiAgICBjYWNoZXMua2V5cygpCiAgICAgIC50aGVuKChrZXlzKSA9PiBQcm9taXNlLmFsbChrZXlzLmZpbHRlcigoaykgPT4gayAhPT0gU0hFTExfQ0FDSEUpLm1hcCgoaykgPT4gY2FjaGVzLmRlbGV0ZShrKSkpKQogICAgICAudGhlbigoKSA9PiBjbGllbnRzLmNsYWltKCkpCiAgKTsKfSk7CgovLyDilIDilIAgZmV0Y2ggc3RyYXRlZ3kg4pSA4pSACnNlbGYuYWRkRXZlbnRMaXN0ZW5lcignZmV0Y2gnLCAoZSkgPT4gewogIGlmIChlLnJlcXVlc3QubWV0aG9kICE9PSAnR0VUJykgcmV0dXJuOwogIGNvbnN0IHVybCA9IG5ldyBVUkwoZS5yZXF1ZXN0LnVybCk7CiAgaWYgKHVybC5vcmlnaW4gIT09IGxvY2F0aW9uLm9yaWdpbikgcmV0dXJuOyAgICAgICAgICAgIC8vIGZvbnRzIGV0YzogbGV0IHRoZSBicm93c2VyIGhhbmRsZQogIGlmICh1cmwucGF0aG5hbWUuc3RhcnRzV2l0aCgnL2FwaS8nKSB8fCB1cmwucGF0aG5hbWUuc3RhcnRzV2l0aCgnL3dzJykpIHJldHVybjsgLy8gbGl2ZSBkYXRhOiBuZXR3b3JrIG9ubHkKCiAgLy8gUGFnZSBuYXZpZ2F0aW9uczogdHJ5IHRoZSBuZXR3b3JrIGZpcnN0IChmcmVzaCB2ZXJzaW9uKSwgZmFsbCBiYWNrIHRvIGNhY2hlIHdoZW4gb2ZmbGluZQogIGlmIChlLnJlcXVlc3QubW9kZSA9PT0gJ25hdmlnYXRlJykgewogICAgZS5yZXNwb25kV2l0aCgKICAgICAgZmV0Y2goZS5yZXF1ZXN0KQogICAgICAgIC50aGVuKChyZXMpID0+IHsKICAgICAgICAgIGNvbnN0IGNvcHkgPSByZXMuY2xvbmUoKTsKICAgICAgICAgIGNhY2hlcy5vcGVuKFNIRUxMX0NBQ0hFKS50aGVuKChjKSA9PiBjLnB1dCgnL2luZGV4Lmh0bWwnLCBjb3B5KSk7CiAgICAgICAgICByZXR1cm4gcmVzOwogICAgICAgIH0pCiAgICAgICAgLmNhdGNoKCgpID0+IGNhY2hlcy5tYXRjaCgnL2luZGV4Lmh0bWwnKS50aGVuKChyKSA9PiByIHx8IGNhY2hlcy5tYXRjaCgnLycpKSkKICAgICk7CiAgICByZXR1cm47CiAgfQoKICAvLyBTdGF0aWMgYXNzZXRzOiBjYWNoZS1maXJzdCwgdGhlbiBuZXR3b3JrIChhbmQgY2FjaGUgZm9yIG5leHQgdGltZSkKICBlLnJlc3BvbmRXaXRoKAogICAgY2FjaGVzLm1hdGNoKGUucmVxdWVzdCkudGhlbigoY2FjaGVkKSA9PiBjYWNoZWQgfHwgZmV0Y2goZS5yZXF1ZXN0KS50aGVuKChyZXMpID0+IHsKICAgICAgY29uc3QgY29weSA9IHJlcy5jbG9uZSgpOwogICAgICBjYWNoZXMub3BlbihTSEVMTF9DQUNIRSkudGhlbigoYykgPT4gYy5wdXQoZS5yZXF1ZXN0LCBjb3B5KSk7CiAgICAgIHJldHVybiByZXM7CiAgICB9KSkKICApOwp9KTsKCi8vIOKUgOKUgCB3ZWIgcHVzaDogT1Mgbm90aWZpY2F0aW9ucyBldmVuIHdoZW4gdGhlIGFwcCBpcyBjbG9zZWQg4pSA4pSACnNlbGYuYWRkRXZlbnRMaXN0ZW5lcigncHVzaCcsIChlKSA9PiB7CiAgbGV0IGRhdGEgPSB7fTsKICB0cnkgeyBkYXRhID0gZS5kYXRhID8gZS5kYXRhLmpzb24oKSA6IHt9OyB9CiAgY2F0Y2ggKGVycikgeyBkYXRhID0geyB0aXRsZTogJ1NoaWZ0IFRyYWNrZXInLCBib2R5OiBlLmRhdGEgPyBlLmRhdGEudGV4dCgpIDogJycgfTsgfQoKICBlLndhaXRVbnRpbChzZWxmLnJlZ2lzdHJhdGlvbi5zaG93Tm90aWZpY2F0aW9uKGRhdGEudGl0bGUgfHwgJ1NoaWZ0IFRyYWNrZXInLCB7CiAgICBib2R5OiBkYXRhLmJvZHkgfHwgJycsCiAgICBpY29uOiAnL2ljb25zL2ljb24tMTkyLnBuZycsCiAgICBiYWRnZTogJy9pY29ucy9pY29uLTE5Mi5wbmcnLAogICAgdGFnOiBkYXRhLnRhZyB8fCAoJ3N0LScgKyBEYXRlLm5vdygpKSwKICAgIHJlbm90aWZ5OiB0cnVlLAogICAgdmlicmF0ZTogWzI1MCwgMTUwLCAyNTAsIDE1MCwgMjUwXSwKICAgIHJlcXVpcmVJbnRlcmFjdGlvbjogISFkYXRhLnVyZ2VudCwKICAgIGRhdGE6IHsgdXJsOiBkYXRhLnVybCB8fCAnLycgfQogIH0pKTsKfSk7CgpzZWxmLmFkZEV2ZW50TGlzdGVuZXIoJ25vdGlmaWNhdGlvbmNsaWNrJywgKGUpID0+IHsKICBlLm5vdGlmaWNhdGlvbi5jbG9zZSgpOwogIGNvbnN0IHRhcmdldFVybCA9IChlLm5vdGlmaWNhdGlvbi5kYXRhICYmIGUubm90aWZpY2F0aW9uLmRhdGEudXJsKSB8fCAnLyc7CiAgZS53YWl0VW50aWwoCiAgICBjbGllbnRzLm1hdGNoQWxsKHsgdHlwZTogJ3dpbmRvdycsIGluY2x1ZGVVbmNvbnRyb2xsZWQ6IHRydWUgfSkudGhlbigobGlzdCkgPT4gewogICAgICBmb3IgKGNvbnN0IGMgb2YgbGlzdCkgewogICAgICAgIGlmICgnZm9jdXMnIGluIGMpIHJldHVybiBjLmZvY3VzKCk7CiAgICAgIH0KICAgICAgcmV0dXJuIGNsaWVudHMub3BlbldpbmRvdyh0YXJnZXRVcmwpOwogICAgfSkKICApOwp9KTsK","icons/icon-192.png":"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","icons/icon-512.png":"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","icons/apple-touch-icon-180.png":"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"};

app.get('/', (req, res) => {
  const candidates = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')];
  for (const p of candidates) {
    try { if (fs.existsSync(p)) return res.type('html').send(fs.readFileSync(p, 'utf8')); } catch (e) {}
  }
  res.status(404).type('text').send('Shift Tracker Pro: face not uploaded yet — add index.html to the repo root.');
});

app.get(['/manifest.json', '/sw.js', '/icons/icon-192.png', '/icons/icon-512.png', '/icons/apple-touch-icon-180.png'], (req, res) => {
  const rel = req.path.slice(1);
  const types = { '.json': 'application/manifest+json', '.js': 'text/javascript', '.png': 'image/png' };
  const type = types[path.extname(rel)] || 'application/octet-stream';
  try {
    const disk = path.join(__dirname, 'public', rel);
    if (fs.existsSync(disk)) { res.type(type); return res.send(fs.readFileSync(disk)); }
  } catch (e) {}
  if (!EMBEDDED_ASSETS[rel]) return res.status(404).end();
  res.type(type);
  res.send(Buffer.from(EMBEDDED_ASSETS[rel], 'base64'));
});

app.get('/api/health', (req, res) => res.json({
  ok: true,
  v: '1.4',
  users: db.users.length,
  time: nowISO(),
  backup: {
    enabled: ghEnabled,
    tokenSet: !!GH_TOKEN,
    repoSet: !!GH_REPO,
    repo: GH_REPO || null,
    lastOk: ghState.lastOk,
    lastError: ghState.lastError
  }
}));

// ── auth ──
app.post('/api/auth/register', (req, res) => {
  let { name, email, password } = req.body || {};
  name = String(name || '').trim();
  email = String(email || '').trim().toLowerCase();
  password = String(password || '');

  if (!name) return res.status(400).json({ error: 'Please enter your name' });
  if (!email.includes('@') || !email.includes('.')) return res.status(400).json({ error: 'Please enter a valid Gmail address' });
  if (password.length < 4) return res.status(400).json({ error: 'Password must be at least 4 characters' });
  if (db.users.some(u => u.email === email)) return res.status(409).json({ error: 'This email already has an account — sign in instead' });

  const { salt, hash } = hashPassword(password);
  const user = { id: id(), name, email, passHash: hash, salt, hourlyWage: null, avatar: null, createdAt: nowISO(), lastSeen: null };
  db.users.push(user);
  db.states[user.id] = { shifts: [], notes: [], reminders: [], settings: {} };

  const token = crypto.randomBytes(24).toString('hex');
  db.sessions[token] = { userId: user.id, createdAt: nowISO() };
  saveDB();
  console.log('👤 registered:', email);
  res.json({ token, user: { id: user.id, name: user.name, email: user.email, hourlyWage: user.hourlyWage, avatar: user.avatar } });
});

app.post('/api/auth/login', (req, res) => {
  const email = String((req.body || {}).email || '').trim().toLowerCase();
  const password = String((req.body || {}).password || '');
  const user = db.users.find(u => u.email === email);
  if (!user || !verifyPassword(password, user.salt, user.passHash)) {
    return res.status(401).json({ error: 'Wrong email or password' });
  }
  const token = crypto.randomBytes(24).toString('hex');
  db.sessions[token] = { userId: user.id, createdAt: nowISO() };
  saveDB();
  console.log('🔑 login:', email);
  res.json({ token, user: { id: user.id, name: user.name, email: user.email, hourlyWage: user.hourlyWage, avatar: user.avatar } });
});

app.post('/api/auth/logout', auth, (req, res) => {
  const token = req.headers['x-auth-token'];
  delete db.sessions[token];
  saveDB();
  res.json({ ok: true });
});

app.get('/api/me', auth, (req, res) => {
  res.json({ user: { id: req.user.id, name: req.user.name, email: req.user.email, hourlyWage: req.user.hourlyWage, avatar: req.user.avatar || null } });
});

app.put('/api/me', auth, (req, res) => {
  const { name, hourlyWage } = req.body || {};
  if (name !== undefined && String(name).trim()) req.user.name = String(name).trim();
  if (hourlyWage !== undefined) {
    const w = parseFloat(hourlyWage);
    if (!isNaN(w) && w > 0) req.user.hourlyWage = w;
  }
  saveDB();
  res.json({ ok: true, user: { id: req.user.id, name: req.user.name, email: req.user.email, hourlyWage: req.user.hourlyWage, avatar: req.user.avatar || null } });
});

// ── profile picture / avatar ──
app.put('/api/me/avatar', auth, (req, res) => {
  const { avatar } = req.body || {};
  if (avatar === null || avatar === undefined || avatar === '') {
    req.user.avatar = null;
  } else if (typeof avatar === 'string' && avatar.length <= 262144 &&
             (/^preset:[0-3]$/.test(avatar) || avatar.startsWith('data:image/'))) {
    req.user.avatar = avatar;
  } else {
    return res.status(400).json({ error: 'Invalid avatar' });
  }
  saveDB();
  res.json({ ok: true, avatar: req.user.avatar || null });
});

// ── delete account (and everything attached to it) ──
app.delete('/api/me', auth, (req, res) => {
  const uid = req.user.id;
  const email = req.user.email;
  const friends = friendIdsOf(uid);

  db.users = db.users.filter(u => u.id !== uid);
  db.friendRequests = db.friendRequests.filter(r => r.fromId !== uid && r.toId !== uid);
  db.messages = db.messages.filter(m => m.fromId !== uid && m.toId !== uid);
  delete db.states[uid];
  delete db.pushedReminders[uid];
  delete db.pushSubs[uid];
  Object.keys(db.sessions).forEach(t => { if (db.sessions[t].userId === uid) delete db.sessions[t]; });
  saveDB();

  // let their friends' apps refresh the list live
  friends.forEach(fid => sendTo(fid, { type: 'friend_removed', userId: uid }));
  console.log('🗑️ account deleted:', email);
  res.json({ ok: true });
});

// ── state sync (shifts / notes / reminders / settings) ──
app.get('/api/state', auth, (req, res) => {
  res.json({ state: db.states[req.user.id] || { shifts: [], notes: [], reminders: [], settings: {} } });
});

app.put('/api/state', auth, (req, res) => {
  const s = req.body && req.body.state;
  if (!s || typeof s !== 'object') return res.status(400).json({ error: 'Bad state' });
  const cur = db.states[req.user.id] || {};
  db.states[req.user.id] = {
    shifts: Array.isArray(s.shifts) ? s.shifts : (cur.shifts || []),
    notes: Array.isArray(s.notes) ? s.notes : (cur.notes || []),
    reminders: Array.isArray(s.reminders) ? s.reminders : (cur.reminders || []),
    settings: (s.settings && typeof s.settings === 'object') ? s.settings : (cur.settings || {})
  };
  saveDB();
  res.json({ ok: true });
});

// ── friends ──
app.get('/api/users', auth, (req, res) => {
  // Small private app: everyone can see all registered users and their
  // online status (so you can find coworkers and add them as friends).
  res.json({ users: db.users.filter(u => u.id !== req.user.id).map(publicUser) });
});

app.get('/api/users/lookup', auth, (req, res) => {
  const email = String(req.query.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ error: 'Missing email' });
  if (email === req.user.email) return res.json({ relationship: 'self' });
  const target = db.users.find(u => u.email === email);
  if (!target) return res.status(404).json({ error: 'No account with this Gmail yet', relationship: 'notfound' });

  let relationship = 'none';
  if (areFriends(req.user.id, target.id)) relationship = 'friends';
  else {
    const r = findRequestBetween(req.user.id, target.id);
    if (r) relationship = r.fromId === req.user.id ? 'pending_outgoing' : 'pending_incoming';
  }
  res.json({ user: publicUser(target), relationship, requestId: (findRequestBetween(req.user.id, target.id) || {}).id });
});

app.get('/api/friends', auth, (req, res) => {
  const friends = friendIdsOf(req.user.id)
    .map(fid => db.users.find(u => u.id === fid)).filter(Boolean).map(publicUser);
  const decorate = r => {
    const other = db.users.find(u => u.id === (r.fromId === req.user.id ? r.toId : r.fromId));
    return other ? { id: r.id, status: r.status, createdAt: r.createdAt, user: publicUser(other), direction: r.fromId === req.user.id ? 'outgoing' : 'incoming' } : null;
  };
  res.json({
    friends,
    incoming: db.friendRequests.filter(r => r.toId === req.user.id && r.status === 'pending').map(decorate).filter(Boolean),
    outgoing: db.friendRequests.filter(r => r.fromId === req.user.id && r.status === 'pending').map(decorate).filter(Boolean)
  });
});

app.post('/api/friends/request', auth, (req, res) => {
  let { toUserId, email } = req.body || {};
  let target = toUserId ? db.users.find(u => u.id === toUserId)
                        : db.users.find(u => u.email === String(email || '').trim().toLowerCase());
  if (!target) return res.status(404).json({ error: 'User not found' });
  if (target.id === req.user.id) return res.status(400).json({ error: "That's you!" });
  if (areFriends(req.user.id, target.id)) return res.status(409).json({ error: 'Already friends' });

  const between = findRequestBetween(req.user.id, target.id);
  if (between) {
    if (between.fromId === req.user.id) return res.status(409).json({ error: 'Request already sent' });
    // they already asked us → accepting = instant friendship
    between.status = 'accepted';
    saveDB();
    sendTo(target.id, { type: 'friend_accepted', user: publicUser(req.user) });
    return res.json({ ok: true, autoAccepted: true, friend: publicUser(target) });
  }

  const r = { id: id(), fromId: req.user.id, toId: target.id, status: 'pending', createdAt: nowISO() };
  db.friendRequests.push(r);
  saveDB();
  sendTo(target.id, { type: 'friend_request', request: { id: r.id, user: publicUser(req.user), createdAt: r.createdAt } });
  if (!onlineIds.has(target.id)) {
    pushTo(target.id, { title: '📨 Friend request', body: req.user.name + ' wants to be your friend', tag: 'freq-' + r.id });
  }
  console.log('🤝 request:', req.user.email, '→', target.email);
  res.status(201).json({ ok: true, request: r });
});

app.post('/api/friends/:requestId/accept', auth, (req, res) => {
  const r = db.friendRequests.find(x => x.id === req.params.requestId);
  if (!r || r.status !== 'pending') return res.status(404).json({ error: 'Request not found' });
  if (r.toId !== req.user.id) return res.status(403).json({ error: 'Not your request' });
  r.status = 'accepted';
  saveDB();
  const from = db.users.find(u => u.id === r.fromId);
  if (from) {
    sendTo(from.id, { type: 'friend_accepted', user: publicUser(req.user) });
    if (!onlineIds.has(from.id)) {
      pushTo(from.id, { title: '🎉 ' + req.user.name + ' accepted!', body: 'You are now friends — say hello!', tag: 'facc-' + r.id });
    }
  }
  console.log('🎉 friends:', req.user.email, '↔', from && from.email);
  res.json({ ok: true, friend: from ? publicUser(from) : null });
});

app.post('/api/friends/:requestId/decline', auth, (req, res) => {
  const r = db.friendRequests.find(x => x.id === req.params.requestId);
  if (!r || r.status !== 'pending') return res.status(404).json({ error: 'Request not found' });
  if (r.toId !== req.user.id) return res.status(403).json({ error: 'Not your request' });
  r.status = 'declined';
  saveDB();
  res.json({ ok: true });
});

app.delete('/api/friends/:userId', auth, (req, res) => {
  const before = db.friendRequests.length;
  db.friendRequests = db.friendRequests.filter(r =>
    !(r.status === 'accepted' &&
      ((r.fromId === req.user.id && r.toId === req.params.userId) ||
       (r.fromId === req.params.userId && r.toId === req.user.id))));
  saveDB();
  res.json({ ok: true, removed: before - db.friendRequests.length });
});

// ── messages ──
function conversationBetween(a, b) {
  return db.messages
    .filter(m => (m.fromId === a && m.toId === b) || (m.fromId === b && m.toId === a))
    .sort((x, y) => new Date(x.timestamp) - new Date(y.timestamp));
}

app.get('/api/conversations', auth, (req, res) => {
  const friends = friendIdsOf(req.user.id);
  const convs = friends.map(fid => {
    const u = db.users.find(x => x.id === fid);
    if (!u) return null;
    const thread = conversationBetween(req.user.id, fid);
    const last = thread[thread.length - 1] || null;
    const unread = thread.filter(m => m.toId === req.user.id && !m.read).length;
    return { friend: publicUser(u), lastMessage: last, unread };
  }).filter(Boolean);
  convs.sort((a, b) => new Date(b.lastMessage ? b.lastMessage.timestamp : 0) - new Date(a.lastMessage ? a.lastMessage.timestamp : 0));
  res.json({ conversations: convs, totalUnread: convs.reduce((s, c) => s + c.unread, 0) });
});

app.get('/api/messages/:friendId', auth, (req, res) => {
  const fid = req.params.friendId;
  if (!areFriends(req.user.id, fid)) return res.status(403).json({ error: 'Not friends' });
  const thread = conversationBetween(req.user.id, fid);
  let changed = false;
  thread.forEach(m => { if (m.toId === req.user.id && !m.read) { m.read = true; changed = true; } });
  if (changed) saveDB();
  res.json({ messages: thread });
});

app.post('/api/messages', auth, (req, res) => {
  const { toUserId, text, imageUrl } = req.body || {};
  const target = db.users.find(u => u.id === toUserId);
  if (!target) return res.status(404).json({ error: 'User not found' });
  if (!areFriends(req.user.id, target.id)) return res.status(403).json({ error: 'You can only message friends' });
  const clean = String(text || '').slice(0, 4000);
  const img = typeof imageUrl === 'string' && imageUrl.startsWith('data:image/') ? imageUrl.slice(0, 5_000_000) : null;
  if (!clean && !img) return res.status(400).json({ error: 'Empty message' });

  const msg = { id: id(), fromId: req.user.id, toId: target.id, text: clean || null, imageUrl: img, timestamp: nowISO(), read: false };
  db.messages.push(msg);
  saveDB();
  sendTo(target.id, { type: 'message', message: msg, from: publicUser(req.user) });
  if (!onlineIds.has(target.id)) {
    pushTo(target.id, {
      title: '💬 ' + req.user.name,
      body: clean ? clean.slice(0, 120) : '📷 Sent you a photo',
      tag: 'msg-' + req.user.id,
      urgent: true
    });
  }
  res.status(201).json({ message: msg });
});

// ── web push ──
app.get('/api/push/vapid-key', (req, res) => {
  res.json({ publicKey: db.vapid.publicKey });
});

app.post('/api/push/subscribe', auth, (req, res) => {
  const sub = (req.body || {}).subscription;
  if (!sub || !sub.endpoint) return res.status(400).json({ error: 'Bad subscription' });
  if (!db.pushSubs[req.user.id]) db.pushSubs[req.user.id] = {};
  db.pushSubs[req.user.id][sub.endpoint] = sub;
  saveDB();
  console.log('🔔 push subscribed:', req.user.email);
  res.json({ ok: true, devices: Object.keys(db.pushSubs[req.user.id]).length });
});

app.post('/api/push/unsubscribe', auth, (req, res) => {
  const endpoint = (req.body || {}).endpoint;
  if (db.pushSubs[req.user.id] && endpoint) {
    delete db.pushSubs[req.user.id][endpoint];
    saveDB();
  }
  res.json({ ok: true });
});

app.post('/api/push/test', auth, async (req, res) => {
  const r = await pushTo(req.user.id, {
    title: 'Shift Tracker 🎉',
    body: 'Notifications work! You will get reminders and messages even when the app is closed.',
    tag: 'st-test',
    urgent: true
  });
  res.json({ ok: true, sent: r.sent });
});

// ────────────────────────── http + websocket server ──────────────────────────
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const onlineIds = new Map(); // userId -> Set<ws>

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  const token = url.searchParams.get('token');
  const session = token && db.sessions[token];
  if (!session) { socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => { ws.userId = session.userId; wss.emit('connection', ws, req); });
});

function sendTo(userId, obj) {
  const set = onlineIds.get(userId);
  if (!set) return;
  const payload = JSON.stringify(obj);
  set.forEach(ws => { try { ws.send(payload); } catch (e) {} });
}

function broadcastPresence(userId, online) {
  // small personal app: presence goes to everyone connected
  const payload = JSON.stringify({ type: 'presence', userId, online });
  onlineIds.forEach(set => set.forEach(ws => { try { ws.send(payload); } catch (e) {} }));
}

wss.on('connection', (ws) => {
  const uid = ws.userId;
  if (!onlineIds.has(uid)) onlineIds.set(uid, new Set());
  onlineIds.get(uid).add(ws);
  ws.isAlive = true;

  const user = db.users.find(u => u.id === uid);
  if (user) { user.lastSeen = nowISO(); saveDB(); }

  ws.send(JSON.stringify({ type: 'hello', userId: uid, online: [...onlineIds.keys()] }));
  broadcastPresence(uid, true);
  console.log('🟢 online:', user ? user.email : uid, `(${onlineIds.size} connected)`);

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    try {
      const data = JSON.parse(raw);
      if (data.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
    } catch (e) {}
  });
  ws.on('close', () => {
    const set = onlineIds.get(uid);
    if (set) {
      set.delete(ws);
      if (set.size === 0) {
        onlineIds.delete(uid);
        if (user) { user.lastSeen = nowISO(); saveDB(); }
        broadcastPresence(uid, false);
        console.log('⚪ offline:', user ? user.email : uid);
      }
    }
  });
});

// heartbeat
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

// ── reminder scheduler: pushes due reminders to connected devices ──
setInterval(() => {
  const now = new Date();
  const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
  db.users.forEach(u => {
    const st = db.states[u.id];
    if (!st || !Array.isArray(st.reminders)) return;
    (st.reminders || []).forEach(r => {
      if (!r.date || !r.time || r.triggered) return;
      if (r.date > today) return;
      const due = new Date(`${r.date}T${r.time}`);
      if (isNaN(due) || due > now) return;
      if (now - due > 24 * 3600e3) return;                  // too old
      const pushed = db.pushedReminders[u.id] || (db.pushedReminders[u.id] = []);
      if (pushed.includes(r.id)) return;
      pushed.push(r.id);
      saveDB();
      const payload = { id: r.id, title: r.title, note: r.note, date: r.date, time: r.time, ringtone: r.ringtone };
      if (onlineIds.has(u.id)) {
        // app open somewhere: in-app ringing alarm
        sendTo(u.id, { type: 'reminder', reminder: payload });
        console.log('⏰ reminder over socket to', u.email, ':', r.title);
      } else {
        // app closed: real OS push notification 🔔
        pushTo(u.id, {
          title: '🔔 Reminder: ' + r.title,
          body: (r.note ? r.note + ' · ' : '') + 'Scheduled ' + r.time + ' — open Shift Tracker to dismiss',
          tag: 'rem-' + r.id,
          urgent: true
        });
      }
    });
  });
}, 20000);

// boot: pull the GitHub backup (if configured) before taking traffic
bootstrap().then(() => server.listen(PORT, () => {
  console.log('');
  console.log('  ╔═══════════════════════════════════════════╗');
  console.log('  ║   SHIFT TRACKER PRO server is running 🚀  ║');
  console.log('  ╚═══════════════════════════════════════════╝');
  console.log('   Local:   http://localhost:' + PORT);
  console.log('   Network: http://<your-computer-ip>:' + PORT + '  (phones on same Wi-Fi)');
  console.log('');
}));
