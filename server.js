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
      console.log('☁️ GitHub backup pushed');
    } else {
      console.log('☁️ GitHub backup failed:', r.status);
    }
  } catch (e) { console.log('☁️ GitHub backup error:', e.message); }
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

app.get('/api/health', (req, res) => res.json({ ok: true, users: db.users.length, time: nowISO() }));

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
