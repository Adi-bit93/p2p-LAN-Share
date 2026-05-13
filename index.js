'use strict';

const fs      = require('fs');
const path    = require('path');

const discovery              = require('./src/discovery');
const { Receiver }           = require('./src/receiver');
const { TransferQueue }      = require('./src/queue');
const { formatBytes }        = require('./src/sender');
const stats                  = require('./src/stats');
const config                 = require('./src/config');
const logger                 = require('./src/logger');
const bridge                 = require('./src/bridge');

// ─── Ensure downloads dir exists ──────────────────────────────────────────────
if (!fs.existsSync(config.DOWNLOADS_DIR)) {
  fs.mkdirSync(config.DOWNLOADS_DIR, { recursive: true });
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
const startTime = Date.now();
logger.info('main', '─────────────────────────────────────');
logger.info('main', '  P2P File Share — starting up');
logger.info('main', '─────────────────────────────────────');
logger.info('main', `hostname  : ${config.PEER_NAME}`);
logger.info('main', `local IP  : ${config.LOCAL_IP}`);
logger.info('main', `udp port  : ${config.UDP_PORT}  (discovery)`);
logger.info('main', `tcp port  : ${config.TCP_PORT}  (transfers)`);
logger.info('main', `http port : ${config.HTTP_PORT} (UI)`);
logger.info('main', `ws port   : ${config.WS_PORT}   (live updates)`);
logger.info('main', `encryption: AES-256-GCM enabled`);
logger.info('main', `downloads : ${config.DOWNLOADS_DIR}`);
logger.info('main', '─────────────────────────────────────');

// ─── Discovery ────────────────────────────────────────────────────────────────
discovery.start();

// Broadcast scan events to UI
discovery.on('scan:start',    ()    => bridge.broadcast('scan:start', {}));
discovery.on('scan:complete', (d)   => bridge.broadcast('scan:complete', d));

discovery.on('peer:new', (p) => {
  logger.success('main', `peer joined → "${p.name}" @ ${p.ip}`);
  bridge.broadcast('peer:new', p);
  bridge.broadcast('peers', discovery.getPeers());
});

discovery.on('peer:update', (p) => {
  bridge.broadcast('peer:update', p);
  bridge.broadcast('peers', discovery.getPeers());
});

discovery.on('peer:lost', (p) => {
  logger.warn('main', `peer left → "${p.name}" @ ${p.ip}`);
  bridge.broadcast('peer:lost', p);
  bridge.broadcast('peers', discovery.getPeers());
});

// ─── Receiver ────────────────────────────────────────────────────────────────
const receiver = new Receiver();
receiver.start();

receiver.on('transfer:start', ({ filename, filesize, from, encrypted }) => {
  logger.info('main', `incoming "${filename}" (${formatBytes(filesize)}) from ${from}${encrypted ? ' [🔒]' : ''}`);
  bridge.broadcast('transfer:start', { filename, filesize, from, encrypted: !!encrypted });
});

receiver.on('transfer:progress', ({ filename, percent }) => {
  process.stdout.write(`\r  ↓ "${filename}" ${percent}%   `);
  bridge.broadcast('transfer:progress', { filename, percent });
});

receiver.on('transfer:done', ({ filename, filepath, durationMs }) => {
  process.stdout.write('\n');
  const filesize = fs.statSync(filepath).size;
  logger.success('main', `saved "${filename}" in ${durationMs}ms → ${filepath}`);
  stats.recordReceived({ filename, peer: 'remote', bytes: filesize, durationMs });
  bridge.broadcast('transfer:done', { filename, filepath, durationMs });
  bridge.broadcast('stats', stats.snapshot());
});

receiver.on('transfer:error', ({ filename, reason }) => {
  process.stdout.write('\n');
  logger.error('main', `"${filename}" failed — ${reason}`);
  bridge.broadcast('transfer:error', { filename, reason });
});

// ─── Queue ────────────────────────────────────────────────────────────────────
const queue = new TransferQueue();

queue.on('job:queued', ({ id, filename, peer }) => {
  bridge.broadcast('job:queued', { id, filename, peer });
});

queue.on('job:start', ({ id, filename, peer, attempt }) => {
  if (attempt > 1) {
    logger.warn('main', `retrying "${filename}" → ${peer} (attempt ${attempt})`);
    bridge.broadcast('job:retrying', { id, filename, attempt, maxAttempts: config.MAX_RETRIES + 1 });
  }
});

queue.on('job:progress', ({ id, filename, percent, speedMBps }) => {
  process.stdout.write(`\r  ↑ "${filename}" ${percent}% @ ${speedMBps} MB/s   `);
  bridge.broadcast('job:progress', { id, filename, percent, speedMBps });
});

queue.on('job:done', ({ id, filename, peer, durationMs, attempts }) => {
  process.stdout.write('\n');
  logger.success('main', `"${filename}" delivered in ${durationMs}ms`);
  bridge.broadcast('job:done', { id, filename, durationMs, attempts });
  bridge.broadcast('stats', stats.snapshot());
});

queue.on('job:failed', ({ id, filename, peer, reason, attempts }) => {
  process.stdout.write('\n');
  logger.error('main', `"${filename}" permanently failed after ${attempts} attempt(s)`);
  stats.recordFailure({ filename, peer, attempts });
  bridge.broadcast('job:failed', { id, filename, reason, attempts });
  bridge.broadcast('stats', stats.snapshot());
});

// ─── Bridge (UI) ─────────────────────────────────────────────────────────────
bridge.start();

// Handle send requests from browser UI
bridge.onSend((peerIp, peerPort, filename) => {
  // Browser sends just the filename — it must be in downloads or a known path
  // For now, log and inform. Full drag-and-drop from browser requires multipart upload (Day 5 bonus)
  logger.info('bridge', `UI send request: "${filename}" → ${peerIp}`);
  logger.info('bridge', `use terminal: s <n> <full-path> for now`);
});

// ─── Ready ────────────────────────────────────────────────────────────────────
const elapsed = Date.now() - startTime;
logger.success('main', `ready in ${elapsed}ms`);
logger.info('main', `open browser → http://localhost:${config.HTTP_PORT}`);
logger.info('main', 'terminal: l=peers, s <n> <path>=send, st=stats, h=help, q=quit');

// Send initial state to any browser that connects after boot
setInterval(() => {
  if (discovery.getPeers().length > 0) {
    bridge.broadcast('peers', discovery.getPeers());
  }
  bridge.broadcast('stats', stats.snapshot());
}, 5000);

// ─── CLI ──────────────────────────────────────────────────────────────────────
process.stdin.setEncoding('utf8');
let buf = '';

process.stdin.on('data', async (key) => {
  buf += key;
  if (!buf.includes('\n')) return;
  const input = buf.trim(); buf = '';

  if (input === 'l') {
    const ps = discovery.getPeers();
    if (!ps.length) { logger.info('peers', 'no peers yet'); return; }
    logger.info('peers', `${ps.length} peer(s) online:`);
    ps.forEach((p, i) => logger.info('peers', `  ${i + 1}. "${p.name}" — ${p.ip}:${p.tcpPort}`));
    return;
  }

  if (input.startsWith('s ')) {
    const parts    = input.split(' ');
    const idx      = parseInt(parts[1], 10) - 1;
    const rawPath  = parts.slice(2).join(' ').trim();
    const filePath = rawPath.replace(/^[\u201C\u201D\u2018\u2019"']+|[\u201C\u201D\u2018\u2019"']+$/g, '');
    const ps       = discovery.getPeers();

    if (isNaN(idx) || idx < 0 || idx >= ps.length) {
      logger.warn('main', 'invalid peer number — use "l" to list peers'); return;
    }
    if (!fs.existsSync(filePath)) {
      logger.warn('main', 'file not found');
      logger.warn('main', `  path parsed: ${filePath}`);
      logger.warn('main', '  tip: no quotes needed, just type the path directly');
      return;
    }

    const peer     = ps[idx];
    const stat     = fs.statSync(filePath);
    const filename = path.basename(filePath);
    const jobId    = queue.enqueue(peer.ip, peer.tcpPort, filePath, filename, stat.size);
    logger.info('main', `queued job #${jobId} — "${filename}" (${formatBytes(stat.size)}) → "${peer.name}"`);
    return;
  }

  if (input === 'st') { stats.print(); return; }

  if (input === 'h') {
    console.log('\n  l                — list online peers');
    console.log('  s <n> <path>     — send file to peer n');
    console.log('  st               — show stats');
    console.log('  q                — quit\n');
    return;
  }

  if (input === 'q') {
    stats.print(); discovery.stop(); receiver.stop(); bridge.stop(); process.exit(0);
  }
});

// ─── Shutdown ─────────────────────────────────────────────────────────────────
process.on('SIGINT', () => {
  logger.info('main', 'shutting down...');
  stats.print();
  discovery.stop(); receiver.stop(); bridge.stop();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  logger.error('main', `uncaught: ${err.message}`);
  process.exit(1);
});