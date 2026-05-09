'use strict';

const dgram  = require('dgram');
const { EventEmitter } = require('events');

const {
  UDP_PORT,
  BROADCAST_ADDR,
  BROADCAST_INTERVAL_MS,
  PEER_EXPIRY_MS,
  LOCAL_IP,
  PEER_NAME,
  TCP_PORT,
} = require('./config');

// ─── Peer registry  
// Structure: { [ip]: { name, ip, tcpPort, lastSeen } }
const peers = new Map();

// ─── Discovery module  
class Discovery extends EventEmitter {
  constructor() {
    super();
    this._broadcaster = null;   // UDP socket for sending
    this._listener    = null;   // UDP socket for receiving
    this._broadcastTimer = null;
    this._expiryTimer    = null;
  }

  // ── Start both broadcaster and listener  
  start() {
    this._startListener();
    this._startBroadcaster();
    this._startExpiryChecker();
    console.log(`[discovery] started — IP: ${LOCAL_IP}, name: "${PEER_NAME}"`);
  }

  // ── Stop everything cleanly  
  stop() {
    clearInterval(this._broadcastTimer);
    clearInterval(this._expiryTimer);
    if (this._broadcaster) this._broadcaster.close();
    if (this._listener)    this._listener.close();
    console.log('[discovery] stopped');
  }

  // ── Return current live peers (excludes self)  
  getPeers() {
    return Array.from(peers.values());
  }

  // ── Internal: build the heartbeat message  
  _buildMessage() {
    return Buffer.from(JSON.stringify({
      name:    PEER_NAME,
      ip:      LOCAL_IP,
      tcpPort: TCP_PORT,
      action:  'HELLO',
    }));
  }

  // ── Internal: broadcaster socket  
  _startBroadcaster() {
    const sock = dgram.createSocket('udp4');

    sock.bind(() => {
      sock.setBroadcast(true);

      const send = () => {
        const msg = this._buildMessage();
        sock.send(msg, 0, msg.length, UDP_PORT, BROADCAST_ADDR, (err) => {
          if (err) console.error('[discovery] broadcast error:', err.message);
        });
      };

      send(); // send immediately on start
      this._broadcastTimer = setInterval(send, BROADCAST_INTERVAL_MS);
    });

    sock.on('error', (err) => {
      console.error('[discovery] broadcaster socket error:', err.message);
    });

    this._broadcaster = sock;
  }

  // ── Internal: listener socket  
  _startListener() {
    const sock = dgram.createSocket('udp4');

    sock.bind(UDP_PORT, () => {
      sock.setBroadcast(true);
      console.log(`[discovery] listening on UDP port ${UDP_PORT}`);
    });

    sock.on('message', (msg, rinfo) => {
      // ignore own broadcasts
      if (rinfo.address === LOCAL_IP) return;

      let payload;
      try {
        payload = JSON.parse(msg.toString());
      } catch {
        return; // ignore malformed packets
      }

      const { name, tcpPort, action } = payload;
      if (action !== 'HELLO') return;

      const ip        = rinfo.address;
      const isNew     = !peers.has(ip);
      const peer      = { name, ip, tcpPort, lastSeen: Date.now() };

      peers.set(ip, peer);

      if (isNew) {
        console.log(`[discovery] new peer: "${name}" @ ${ip}`);
        this.emit('peer:new', peer);
      } else {
        this.emit('peer:update', peer);
      }
    });

    sock.on('error', (err) => {
      console.error('[discovery] listener socket error:', err.message);
    });

    this._listener = sock;
  }

  // ── Internal: remove stale peers  
  _startExpiryChecker() {
    this._expiryTimer = setInterval(() => {
      const now     = Date.now();
      const expired = [];

      for (const [ip, peer] of peers) {
        if (now - peer.lastSeen > PEER_EXPIRY_MS) {
          peers.delete(ip);
          expired.push(peer);
          console.log(`[discovery] peer expired: "${peer.name}" @ ${ip}`);
          this.emit('peer:lost', peer);
        }
      }
    }, PEER_EXPIRY_MS);
  }
}

const instance = new Discovery();

// expose internal map only for tests — do not use in production code
instance._peers = peers;

module.exports = instance;