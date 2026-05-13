'use strict';

const dgram  = require('dgram');
const net    = require('net');
const os     = require('os');
const dns    = require('dns');
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

// ─── Device registry ──────────────────────────────────────────────────────────
// All devices on LAN — both p2p-capable and plain devices
// Structure: { [ip]: { name, ip, tcpPort, p2pCapable, lastSeen, status } }
const peers = new Map();

// ─── Subnet helpers ───────────────────────────────────────────────────────────
function getSubnetBase() {
  // From LOCAL_IP e.g. "192.168.1.45" → "192.168.1"
  return LOCAL_IP.split('.').slice(0, 3).join('.');
}

function getAllSubnetIPs() {
  const base = getSubnetBase();
  const ips  = [];
  for (let i = 1; i <= 254; i++) {
    const ip = `${base}.${i}`;
    if (ip !== LOCAL_IP) ips.push(ip);   // skip self
  }
  return ips;
}

// ─── Resolve hostname for an IP ───────────────────────────────────────────────
function resolveHostname(ip) {
  return new Promise((resolve) => {
    dns.reverse(ip, (err, hostnames) => {
      if (err || !hostnames || hostnames.length === 0) {
        resolve(ip);   // fallback to IP as name
      } else {
        // Strip .local suffix, use first hostname
        resolve(hostnames[0].replace(/\.local$/, '').split('.')[0]);
      }
    });
  });
}

// ─── TCP ping — check if a host is alive on any common port ──────────────────
function tcpPing(ip, port = 80, timeoutMs = 500) {
  return new Promise((resolve) => {
    const sock = new net.Socket();
    sock.setTimeout(timeoutMs);
    sock.on('connect', () => { sock.destroy(); resolve(true);  });
    sock.on('timeout', () => { sock.destroy(); resolve(false); });
    sock.on('error',   () => { sock.destroy(); resolve(false); });
    sock.connect(port, ip);
  });
}

// Probe multiple ports — device is alive if ANY port responds
async function isAlive(ip) {
  // Check our app port first (fastest for p2p peers)
  // Then common ports: 80 (HTTP), 443 (HTTPS), 22 (SSH), 445 (SMB/Windows), 62078 (iPhone)
  const ports = [TCP_PORT, 80, 443, 22, 445, 8080, 62078, 5000];
  const results = await Promise.all(ports.map(p => tcpPing(ip, p, 400)));
  return results.some(r => r === true);
}

// ─── Discovery class ──────────────────────────────────────────────────────────
class Discovery extends EventEmitter {
  constructor() {
    super();
    this._broadcaster    = null;
    this._listener       = null;
    this._broadcastTimer = null;
    this._expiryTimer    = null;
    this._scanning       = false;
    this._scanTimer      = null;
  }

  // ── Start everything ──────────────────────────────────────────────────────
  start() {
    this._startListener();
    this._startBroadcaster();
    this._startExpiryChecker();

    // Scan immediately, then every 60 seconds
    this._runLANScan();
    this._scanTimer = setInterval(() => this._runLANScan(), 60000);

    console.log(`[discovery] started — IP: ${LOCAL_IP}, subnet: ${getSubnetBase()}.0/24`);
  }

  stop() {
    clearInterval(this._broadcastTimer);
    clearInterval(this._expiryTimer);
    clearInterval(this._scanTimer);
    if (this._broadcaster) this._broadcaster.close();
    if (this._listener)    this._listener.close();
    console.log('[discovery] stopped');
  }

  // ── Return all known devices ──────────────────────────────────────────────
  getPeers() {
    return Array.from(peers.values());
  }

  // ── Return only p2p-capable devices (have our app running) ───────────────
  getP2PPeers() {
    return Array.from(peers.values()).filter(p => p.p2pCapable);
  }

  // ── LAN-wide scan using TCP probing ───────────────────────────────────────
  async _runLANScan() {
    if (this._scanning) return;   // don't overlap scans
    this._scanning = true;

    this.emit('scan:start', {});
    const allIPs = getAllSubnetIPs();
    console.log(`[discovery] scanning ${allIPs.length} hosts on ${getSubnetBase()}.0/24...`);

    const BATCH = 30;   // probe 30 hosts at a time (avoid overwhelming the network)
    let found = 0;

    for (let i = 0; i < allIPs.length; i += BATCH) {
      const batch   = allIPs.slice(i, i + BATCH);
      const results = await Promise.all(batch.map(async (ip) => {
        const alive = await isAlive(ip);
        return { ip, alive };
      }));

      for (const { ip, alive } of results) {
        if (!alive) continue;
        found++;

        const existing = peers.get(ip);

        if (!existing) {
          // New device found — resolve its hostname
          const name = await resolveHostname(ip);
          const device = {
            name,
            ip,
            tcpPort:     TCP_PORT,   // assumed — overwritten if they send HELLO
            p2pCapable:  false,       // unknown until they send UDP HELLO
            lastSeen:    Date.now(),
            status:      'online',
          };
          peers.set(ip, device);
          console.log(`[discovery] device found: "${name}" @ ${ip}`);
          this.emit('peer:new', device);
        } else {
          // Refresh lastSeen
          existing.lastSeen = Date.now();
          existing.status   = 'online';
        }
      }
    }

    console.log(`[discovery] scan complete — ${found} device(s) online on LAN`);
    this._scanning = false;
    this.emit('scan:complete', { total: found });
  }

  // ── UDP broadcaster — announces THIS app to the LAN ──────────────────────
  _startBroadcaster() {
    const sock = dgram.createSocket('udp4');

    sock.bind(() => {
      sock.setBroadcast(true);

      const send = () => {
        const msg = Buffer.from(JSON.stringify({
          name:    PEER_NAME,
          ip:      LOCAL_IP,
          tcpPort: TCP_PORT,
          action:  'HELLO',
        }));
        sock.send(msg, 0, msg.length, UDP_PORT, BROADCAST_ADDR, (err) => {
          if (err) console.error('[discovery] broadcast error:', err.message);
        });
      };

      send();
      this._broadcastTimer = setInterval(send, BROADCAST_INTERVAL_MS);
    });

    sock.on('error', err => console.error('[discovery] broadcaster error:', err.message));
    this._broadcaster = sock;
  }

  // ── UDP listener — marks devices that have our app as p2p-capable ─────────
  _startListener() {
    const sock = dgram.createSocket('udp4');

    sock.bind(UDP_PORT, () => {
      sock.setBroadcast(true);
      console.log(`[discovery] listening on UDP port ${UDP_PORT}`);
    });

    sock.on('message', (msg, rinfo) => {
      if (rinfo.address === LOCAL_IP) return;  // ignore self

      let payload;
      try { payload = JSON.parse(msg.toString()); }
      catch { return; }

      const { name, tcpPort, action } = payload;
      if (action !== 'HELLO') return;

      const ip      = rinfo.address;
      const wasKnown = peers.has(ip);
      const wasP2P   = peers.get(ip)?.p2pCapable;

      // Update or create the peer entry — mark as p2p-capable
      const peer = {
        name,
        ip,
        tcpPort,
        p2pCapable: true,     // ← key flag: has our app
        lastSeen:   Date.now(),
        status:     'online',
      };
      peers.set(ip, peer);

      if (!wasKnown) {
        console.log(`[discovery] p2p peer joined: "${name}" @ ${ip}`);
        this.emit('peer:new', peer);
      } else if (!wasP2P) {
        // Was a plain LAN device, now upgraded to p2p-capable
        console.log(`[discovery] device upgraded to p2p: "${name}" @ ${ip}`);
        this.emit('peer:update', peer);
      } else {
        this.emit('peer:update', peer);
      }
    });

    sock.on('error', err => console.error('[discovery] listener error:', err.message));
    this._listener = sock;
  }

  // ── Expiry — remove devices not seen for PEER_EXPIRY_MS 
  _startExpiryChecker() {
    this._expiryTimer = setInterval(() => {
      const now = Date.now();
      for (const [ip, peer] of peers) {
        if (now - peer.lastSeen > PEER_EXPIRY_MS) {
          peers.delete(ip);
          console.log(`[discovery] device offline: "${peer.name}" @ ${ip}`);
          this.emit('peer:lost', peer);
        }
      }
    }, PEER_EXPIRY_MS);
  }
}

const instance = new Discovery();
instance._peers = peers;   // exposed for tests only

module.exports = instance;