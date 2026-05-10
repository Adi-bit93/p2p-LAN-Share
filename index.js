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

// ─── Ensure downloads directory exists ───────────────────────────────────────
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
logger.info('main', `downloads : ${config.DOWNLOADS_DIR}`);
logger.info('main', `max retry : ${config.MAX_RETRIES} attempts`);
logger.info('main', '─────────────────────────────────────');

// ─── Start peer discovery ─────────────────────────────────────────────────────
discovery.start();

discovery.on('peer:new',  (p) => logger.success('main', `peer joined → "${p.name}" @ ${p.ip}:${p.tcpPort}`));
discovery.on('peer:lost', (p) => logger.warn   ('main', `peer left   → "${p.name}" @ ${p.ip}`));

// ─── Start TCP receiver ───────────────────────────────────────────────────────
const receiver = new Receiver();
receiver.start();

receiver.on('transfer:start', ({ filename, filesize, from }) => {
  logger.info('main', `incoming "${filename}" (${formatBytes(filesize)}) from ${from}`);
});

receiver.on('transfer:progress', ({ filename, percent }) => {
  process.stdout.write(`\r  ↓ "${filename}" ${percent}%   `);
});

receiver.on('transfer:done', ({ filename, filepath, durationMs }) => {
  process.stdout.write('\n');
  const filesize = fs.statSync(filepath).size;
  logger.success('main', `saved "${filename}" in ${durationMs}ms → ${filepath}`);
  stats.recordReceived({ filename, peer: 'unknown', bytes: filesize, durationMs });
});

receiver.on('transfer:error', ({ filename, reason }) => {
  process.stdout.write('\n');
  logger.error('main', `"${filename}" failed — ${reason}`);
});

// ─── Transfer queue ───────────────────────────────────────────────────────────
const queue = new TransferQueue();

queue.on('job:start', ({ id, filename, peer, attempt }) => {
  if (attempt > 1) {
    logger.warn('main', `retrying "${filename}" → ${peer} (attempt ${attempt})`);
  }
});

queue.on('job:progress', ({ filename, percent, speedMBps }) => {
  process.stdout.write(`\r  ↑ "${filename}" ${percent}% @ ${speedMBps} MB/s   `);
});

queue.on('job:done', ({ filename, peer, durationMs, attempts }) => {
  process.stdout.write('\n');
  logger.success('main', `"${filename}" delivered in ${durationMs}ms${attempts > 1 ? ` (${attempts} attempts)` : ''}`);
});

queue.on('job:failed', ({ filename, peer, reason, attempts }) => {
  process.stdout.write('\n');
  logger.error('main', `"${filename}" permanently failed after ${attempts} attempt(s) — ${reason}`);
});

queue.on('queue:empty', () => {
  const s = queue.status();
  if (s.stats.totalSent > 0) {
    logger.info('main', `queue empty — ${s.stats.totalSent} file(s) sent, ${s.stats.totalRetries} retries`);
  }
});

// ─── Ready ────────────────────────────────────────────────────────────────────
const elapsed = Date.now() - startTime;
logger.success('main', `ready in ${elapsed}ms`);
logger.info('main', 'type h for help');

// ─── CLI input handler ────────────────────────────────────────────────────────
process.stdin.setEncoding('utf8');
let inputBuffer = '';

process.stdin.on('data', async (key) => {
  inputBuffer += key;
  if (!inputBuffer.includes('\n')) return;
  const input = inputBuffer.trim();
  inputBuffer = '';

  // ── l — list peers ────────────────────────────────────────────────────────
  if (input === 'l') {
    const peers = discovery.getPeers();
    if (peers.length === 0) {
      logger.info('peers', 'no peers found yet — waiting for broadcasts...');
    } else {
      logger.info('peers', `${peers.length} peer(s) online:`);
      peers.forEach((p, i) =>
        logger.info('peers', `  ${i + 1}. "${p.name}" — ${p.ip}:${p.tcpPort}`)
      );
    }
    return;
  }

  // ── s <n> <path> — send file via queue ───────────────────────────────────
  if (input.startsWith('s ')) {
    const parts    = input.split(' ');
    const idx      = parseInt(parts[1], 10) - 1;
    const rawPath  = parts.slice(2).join(' ').trim();
    // Strip ALL quote variants — straight, curly/smart quotes from PowerShell/copy-paste
    const filePath = rawPath.replace(/^[\u201C\u201D\u2018\u2019"']+|[\u201C\u201D\u2018\u2019"']+$/g, '');
    const peers    = discovery.getPeers();

    if (isNaN(idx) || idx < 0 || idx >= peers.length) {
      logger.warn('main', 'invalid peer number — use "l" to list peers first');
      return;
    }
    if (!fs.existsSync(filePath)) {
      logger.warn('main', 'file not found');
      logger.warn('main', `  path parsed: ${filePath}`);
      logger.warn('main', '  tip: no quotes needed, just type the path directly');
      logger.warn('main', '  e.g: s 1 C:\\Users\\chava\\Downloads\\lecture.mp4');
      return;
    }

    const peer     = peers[idx];
    const stat     = fs.statSync(filePath);
    const filename = path.basename(filePath);
    const jobId    = queue.enqueue(peer.ip, peer.tcpPort, filePath, filename, stat.size);

    logger.info('main', `queued job #${jobId} — "${filename}" (${formatBytes(stat.size)}) → "${peer.name}"`);
    return;
  }

  // ── q:status — show queue status ─────────────────────────────────────────
  if (input === 'q') {
    discovery.stop();
    receiver.stop();
    process.exit(0);
  }

  // ── st — show stats ───────────────────────────────────────────────────────
  if (input === 'st') {
    stats.print();
    const qs = queue.status();
    logger.info('queue', `active: ${qs.active}, pending: ${qs.pending}`);
    return;
  }

  // ── h — help ──────────────────────────────────────────────────────────────
  if (input === 'h') {
    console.log('\n  Commands:');
    console.log('  l                — list online peers');
    console.log('  s <n> <path>     — send file to peer number n (queued, auto-retry)');
    console.log('  st               — show transfer statistics');
    console.log('  q                — quit');
    console.log('\n  Example:');
    console.log('  s 1 C:\\Users\\chava\\Downloads\\lecture.mp4\n');
    return;
  }
});

// ─── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGINT', () => {
  logger.info('main', 'shutting down...');
  stats.print();
  discovery.stop();
  receiver.stop();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  logger.error('main', `uncaught: ${err.message}`);
  process.exit(1);
});