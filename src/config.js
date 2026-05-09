'use strict';

const os = require('os');

//Ports 
const UDP_PORT      = 9999;   // peer discovery broadcast
const TCP_PORT      = 8888;   // file transfer
const WS_PORT       = 7777;   // websocket bridge (UI ↔ backend)
const HTTP_PORT     = 3000;   // serves the frontend HTML

// Discovery
const BROADCAST_INTERVAL_MS  = 5000;   // announce yourself every 5s
const PEER_EXPIRY_MS          = 15000;  // remove peer if silent for 15s
const BROADCAST_ADDR          = '255.255.255.255';

//Transfer
const CHUNK_SIZE     = 1024 * 1024;  // 1 MB read buffer
const DOWNLOADS_DIR  = require('path').join(__dirname, '..', 'downloads');
const MAX_RETRIES    = 3;

// Identity 
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      // skip loopback and non-IPv4
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
}

const LOCAL_IP    = getLocalIP();
const PEER_NAME   = os.hostname(); // use machine hostname as display name

module.exports = {
  UDP_PORT,
  TCP_PORT,
  WS_PORT,
  HTTP_PORT,
  BROADCAST_INTERVAL_MS,
  PEER_EXPIRY_MS,
  BROADCAST_ADDR,
  CHUNK_SIZE,
  DOWNLOADS_DIR,
  MAX_RETRIES,
  LOCAL_IP,
  PEER_NAME,
};