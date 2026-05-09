'use strict';

const fs = require('fs');
const discovery = require('./src/discovery');
const { Receiver } = require('./src/receiver');
const { Sender, formatBytes } = require('./src/sender');
const config = require('./src/config');
const logger = require('./src/logger');

// ─── Ensure downloads directory exists ───────────────────────────────────────
if (!fs.existsSync(config.DOWNLOADS_DIR)) {
  fs.mkdirSync(config.DOWNLOADS_DIR, { recursive: true });
  logger.info('main', `created downloads dir: ${config.DOWNLOADS_DIR}`);
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
logger.info('main', '─────────────────────────────────────');

// ─── Start peer discovery ─────────────────────────────────────────────────────
discovery.start();

discovery.on('peer:new', (p) => logger.success('main', `peer joined → "${p.name}" @ ${p.ip}:${p.tcpPort}`));
discovery.on('peer:lost', (p) => logger.warn('main', `peer left   → "${p.name}" @ ${p.ip}`));

// ─── Start TCP receiver ───────────────────────────────────────────────────────
const receiver = new Receiver();
receiver.start();

receiver.on('transfer:start', ({ filename, filesize, from }) => {
  logger.info('main', `incoming "${filename}" (${formatBytes(filesize)}) from ${from}`);
});
receiver.on('transfer:done', ({ filename, durationMs, filepath }) => {
  logger.success('main', `saved "${filename}" in ${durationMs}ms → ${filepath}`);
});
receiver.on('transfer:error', ({ filename, reason }) => {
  logger.error('main', `"${filename}" failed — ${reason}`);
});

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

  // l — list peers
  if (input === 'l') {
    const peers = discovery.getPeers();
    if (peers.length === 0) {
      logger.info('peers', 'no peers found yet');
    } else {
      logger.info('peers', `${peers.length} peer(s) online:`);
      peers.forEach((p, i) =>
        logger.info('peers', `  ${i + 1}. "${p.name}" — ${p.ip}:${p.tcpPort}`)
      );
    }
    return;
  }

  // s <peerNumber> <filePath> — send file
  if (input.startsWith('s ')) {
    const parts = input.split(' ');
    const idx = parseInt(parts[1], 10) - 1;
    const filePath = parts.slice(2).join(' ').trim();
    const peers = discovery.getPeers();

    if (isNaN(idx) || idx < 0 || idx >= peers.length) {
      logger.warn('main', 'invalid peer number — use "l" to list peers');
      return;
    }
    if (!fs.existsSync(filePath)) {
      logger.warn('main', `file not found: "${filePath}"`);
      return;
    }

    const peer = peers[idx];
    const sender = new Sender();

    sender.on('progress', ({ filename, percent, speedMBps }) => {
      process.stdout.write(`\r  → "${filename}" ${percent}% @ ${speedMBps} MB/s   `);
    });
    sender.on('done', ({ filename, durationMs }) => {
      process.stdout.write('\n');
      logger.success('main', `"${filename}" delivered in ${durationMs}ms`);
    });
    sender.on('error', () => process.stdout.write('\n'));

    logger.info('main', `sending to "${peer.name}" @ ${peer.ip}...`);
    try { await sender.send(peer.ip, peer.tcpPort, filePath); } catch { /* logged */ }
    return;
  }

  // h — help
  if (input === 'h') {
    console.log('\n  l               — list online peers');
    console.log('  s <n> <path>    — send file to peer number n');
    console.log('  e.g: s 1 C:\\Users\\chava\\Videos\\lecture.mp4');
    console.log('  q               — quit\n');
    return;
  }

  // q — quit
  if (input === 'q') {
    discovery.stop();
    receiver.stop();
    process.exit(0);
  }
});

process.on('SIGINT', () => { discovery.stop(); receiver.stop(); process.exit(0); });
process.on('uncaughtException', (err) => { logger.error('main', err.message); process.exit(1); });