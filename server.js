'use strict';
// 그라데이션 데스매치 - 웹 서버
// 외부 패키지 없이 Node.js만으로 동작합니다.
//   - GET /        게임 페이지 (public/index.html)
//   - GET /healthz 서버 상태 확인
//   - /ws          실시간 대전용 WebSocket (모든 접속자가 하나의 방에 들어갑니다)
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3000;
const MAX_PLAYERS = Math.max(2, Number(process.env.MAX_PLAYERS) || 24);
const MAX_MSG = 16 * 1024;          // 메시지 하나의 최대 크기
const RATE = 45;                     // 초당 최대 메시지 수 (접속자 1명 기준)
const PING_MS = 20000;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------- static files (gzip once, keep in memory) ----------
const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.webmanifest': 'application/manifest+json' };
const files = new Map();
function loadFiles() {
  files.clear();
  // public/ folder if present; otherwise only index.html next to this file (never serve server.js itself)
  const names = fs.existsSync(PUB) ? fs.readdirSync(PUB).map((n) => path.join(PUB, n)) : [path.join(__dirname, 'index.html')];
  for (const full of names) {
    const name = path.basename(full);
    if (!fs.statSync(full).isFile()) continue;
    const raw = fs.readFileSync(full), ext = path.extname(name).toLowerCase();
    const zip = /text|javascript|json|svg|manifest/.test(TYPES[ext] || '') ? zlib.gzipSync(raw, { level: 9 }) : null;
    files.set('/' + name, { raw, zip, type: TYPES[ext] || 'application/octet-stream', etag: '"' + crypto.createHash('sha1').update(raw).digest('base64').slice(0, 16) + '"' });
  }
  files.set('/', files.get('/index.html'));
}
loadFiles();

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  res.setHeader('X-Content-Type-Options', 'nosniff');
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end(`ok ${clients.size}/${MAX_PLAYERS}`); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  const f = files.get(url);
  if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('찾을 수 없어요'); }
  const headers = { 'Content-Type': f.type, 'Cache-Control': 'no-cache', 'ETag': f.etag, 'Vary': 'Accept-Encoding' };
  if (req.headers['if-none-match'] === f.etag) { res.writeHead(304, headers); return res.end(); }
  const gz = f.zip && /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  if (gz) headers['Content-Encoding'] = 'gzip';
  const body = gz ? f.zip : f.raw; headers['Content-Length'] = body.length;
  res.writeHead(200, headers); res.end(req.method === 'HEAD' ? undefined : body);
});

// ---------- WebSocket (RFC 6455, text frames only) ----------
let counter = 0;
const clients = new Map(); // id -> client

function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) { h = Buffer.from([0x80 | op, len]); }
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(len, 6); }
  return Buffer.concat([h, payload]);
}
function sendRaw(c, buf) {
  if (c.dead) return;
  if (c.socket.writableLength > 1024 * 1024) return drop(c, 'slow'); // 너무 느린 접속은 정리
  c.socket.write(buf);
}
const sendJSON = (c, obj) => sendRaw(c, frame(1, Buffer.from(JSON.stringify(obj))));
function broadcast(from, obj) { const buf = frame(1, Buffer.from(JSON.stringify(obj))); for (const c of clients.values()) if (c !== from && c.ready) sendRaw(c, buf); }

function drop(c, why) {
  if (c.dead) return; c.dead = true;
  try { c.socket.end(frame(8, Buffer.from([0x03, 0xe8]))); } catch (e) { }
  setTimeout(() => { try { c.socket.destroy(); } catch (e) { } }, 1000);
  if (clients.get(c.id) === c) {
    clients.delete(c.id);
    if (c.ready) broadcast(c, { t: 'bye', id: c.id });
    log(`- ${c.id} (${why}) · 접속 ${clients.size}명`);
  }
}

function onMessage(c, text) {
  // 초당 메시지 수 제한
  const now = Date.now();
  c.tokens = Math.min(RATE, c.tokens + (now - c.last) * RATE / 1000); c.last = now;
  if (c.tokens < 1) { c.over++; if (c.over > RATE * 4) drop(c, 'flood'); return; }
  c.tokens -= 1; c.over = 0;
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'p') {
    if (!m.p || typeof m.p !== 'object' || Array.isArray(m.p)) return;
    c.p = m.p;
    broadcast(c, { t: 'p', id: c.id, p: c.p });
  }
}

function handleData(c, chunk) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, chunk]) : chunk;
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80; let len = b[1] & 0x7f, off = 2;
    if (!masked) return drop(c, 'unmasked');
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2) !== 0) return drop(c, 'too big'); len = b.readUInt32BE(6); off = 10; }
    if (len > MAX_MSG) return drop(c, 'too big');
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), data = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    c.alive = true;
    if (op === 8) return drop(c, 'closed');
    if (op === 9) { sendRaw(c, frame(10, data)); continue; }
    if (op === 10) continue;
    if (op === 1 || op === 2 || op === 0) {
      if (op !== 0) c.frag = [];
      c.frag.push(data); c.fragLen = (op !== 0 ? 0 : c.fragLen) + len;
      if (c.fragLen > MAX_MSG) return drop(c, 'too big');
      if (fin) { const msg = Buffer.concat(c.frag); c.frag = []; c.fragLen = 0; onMessage(c, msg.toString('utf8')); if (c.dead) return; }
      continue;
    }
    return drop(c, 'bad opcode');
  }
}

server.on('upgrade', (req, socket, head) => {
  const url = (req.url || '').split('?')[0], key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + String(++counter).padStart(9, '0'), socket, buf: Buffer.alloc(0), frag: [], fragLen: 0, p: null, ready: false, dead: false, alive: true, tokens: RATE, last: Date.now(), over: 0 };
  socket.on('data', (d) => { try { handleData(c, d); } catch (e) { drop(c, 'error'); } });
  socket.on('close', () => drop(c, 'left'));
  socket.on('error', () => drop(c, 'socket error'));
  if (clients.size >= MAX_PLAYERS) { sendJSON(c, { t: 'full', max: MAX_PLAYERS }); c.dead = true; setTimeout(() => socket.destroy(), 300); return; }
  clients.set(c.id, c);
  const peers = []; for (const o of clients.values()) if (o !== c && o.ready) peers.push({ id: o.id, p: o.p });
  sendJSON(c, { t: 'hello', id: c.id, peers, max: MAX_PLAYERS });
  c.ready = true;
  if (head && head.length) { try { handleData(c, head); } catch (e) { drop(c, 'error'); } }
  log(`+ ${c.id} · 접속 ${clients.size}명`);
});

setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { drop(c, 'timeout'); continue; }
    c.alive = false; sendRaw(c, frame(9, Buffer.alloc(0)));
  }
}, PING_MS).unref();

function log(s) { if (!process.env.QUIET) console.log(new Date().toISOString().slice(11, 19), s); }

server.listen(PORT, '0.0.0.0', () => console.log(`그라데이션 데스매치 서버 실행 중 → http://localhost:${PORT}  (최대 ${MAX_PLAYERS}명)`));
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { for (const c of clients.values()) drop(c, 'shutdown'); server.close(); setTimeout(() => process.exit(0), 300); });
