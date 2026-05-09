'use strict';

const fs        = require('fs');
const path      = require('path');
const discovery = require('./src/discovery.js');
const config    = require('./src/config.js');
const logger    = require('./src/logger.js');

//   Ensure downloads directory exists 
if (!fs.existsSync(config.DOWNLOADS_DIR)) {
  fs.mkdirSync(config.DOWNLOADS_DIR, { recursive: true });
  logger.info('main', `created downloads dir: ${config.DOWNLOADS_DIR}`);
}

//   Boot  
const startTime = Date.now();

logger.info('main', '─────────────────────────────────────');
logger.info('main', '  P2P File Share — starting up');
logger.info('main', '─────────────────────────────────────');
logger.info('main', `hostname : ${config.PEER_NAME}`);
logger.info('main', `local IP : ${config.LOCAL_IP}`);
logger.info('main', `udp port : ${config.UDP_PORT}`);
logger.info('main', `tcp port : ${config.TCP_PORT}`);
logger.info('main', '─────────────────────────────────────');

// Start peer discovery  
discovery.start();

discovery.on('peer:new', (peer) => {
  logger.success('main', `peer joined  → "${peer.name}" @ ${peer.ip}:${peer.tcpPort}`);
  // Day 4: emit to WebSocket clients here
});

discovery.on('peer:lost', (peer) => {
  logger.warn('main', `peer left    → "${peer.name}" @ ${peer.ip}`);
  // Day 4: emit to WebSocket clients here
});

discovery.on('peer:update', (peer) => {
  // silent — just a heartbeat refresh
});

const elapsed = Date.now() - startTime;
logger.success('main', `ready in ${elapsed}ms — waiting for peers...`);

// List peers on demand (press L + Enter) 
process.stdin.setEncoding('utf8');
process.stdin.on('data', (key) => {
  const input = key.trim().toLowerCase();

  if (input === 'l') {
    const peers = discovery.getPeers();
    if (peers.length === 0) {
      logger.info('peers', 'no peers found yet — waiting for broadcasts...');
    } else {
      logger.info('peers', `${peers.length} peer(s) online:`);
      peers.forEach((p, i) => {
        logger.info('peers', `  ${i + 1}. "${p.name}" — ${p.ip}:${p.tcpPort}`);
      });
    }
  }

  if (input === 'q') {
    logger.info('main', 'shutting down...');
    discovery.stop();
    process.exit(0);
  }

  if (input === 'h') {
    console.log('\n  Commands:');
    console.log('  l — list online peers');
    console.log('  q — quit\n');
  }
});

logger.info('main', 'commands: l=list peers, h=help, q=quit');

//  Graceful shutdown 
process.on('SIGINT', () => {
  logger.info('main', 'SIGINT received — shutting down...');
  discovery.stop();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  logger.error('main', `uncaught: ${err.message}`);
  process.exit(1);
});