'use strict';

const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const WS     = require('ws');
const { WS_PORT, HTTP_PORT, LOCAL_IP, PEER_NAME } = require('./config');
const logger = require('./logger');

/**
 * Bridge — HTTP server (serves index.html) + WebSocket server (live events).
 *
 * Message types sent to browser:
 *   init             { name, ip }
 *   peers            [{ name, ip, tcpPort }]
 *   peer:new         { name, ip, tcpPort }
 *   peer:lost        { name, ip }
 *   job:queued       { id, filename, peer }
 *   job:progress     { id, filename, percent, speedMBps }
 *   job:done         { id, filename, durationMs, attempts }
 *   job:failed       { id, filename, reason, attempts }
 *   job:retrying     { id, filename, attempt, maxAttempts }
 *   transfer:start   { filename, filesize, from, encrypted }
 *   transfer:progress{ filename, percent }
 *   transfer:done    { filename, filepath, durationMs }
 *   transfer:error   { filename, reason }
 *   stats            statsSnapshot
 *
 * Message types received from browser:
 *   send  { peerIp, peerPort, filename }  → triggers queue.enqueue via callback
 */
class Bridge {
  constructor() {
    this._wss      = null;
    this._server   = null;
    this._clients  = new Set();
    this._onSend   = null;   // callback set by index.js: (peerIp, peerPort, filename) => void
  }

  // ── Wire up the send callback from main ───────────────────────────────────
  onSend(fn) { this._onSend = fn; }

  // ── Start both servers ────────────────────────────────────────────────────
  start() {
    this._startHTTP();
    this._startWS();
  }

  stop() {
    if (this._wss)    this._wss.close();
    if (this._server) this._server.close();
  }

  // ── Broadcast typed message to all connected browsers ─────────────────────
  broadcast(type, data) {
    const msg = JSON.stringify({ type, data });
    for (const client of this._clients) {
      if (client.readyState === WS.OPEN) client.send(msg);
    }
  }

  // ── Send message to one client ────────────────────────────────────────────
  _send(ws, type, data) {
    if (ws.readyState === WS.OPEN) ws.send(JSON.stringify({ type, data }));
  }

  // ── HTTP — serves only public/index.html ──────────────────────────────────
  _startHTTP() {
    const htmlPath = path.join(__dirname, '..', 'public', 'index.html');

    this._server = http.createServer((req, res) => {
      if (req.method !== 'GET' || (req.url !== '/' && req.url !== '/index.html')) {
        res.writeHead(404); res.end('Not found'); return;
      }
      fs.readFile(htmlPath, (err, data) => {
        if (err) { res.writeHead(500); res.end('Could not read index.html'); return; }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(data);
      });
    });

    this._server.listen(HTTP_PORT, () => {
      logger.success('bridge', `UI → http://localhost:${HTTP_PORT}`);
    });
  }

  // ── WebSocket server ──────────────────────────────────────────────────────
  _startWS() {
    this._wss = new WS.Server({ port: WS_PORT });

    this._wss.on('connection', (ws) => {
      this._clients.add(ws);
      logger.info('bridge', `browser connected (${this._clients.size} tab(s))`);

      // Immediately send identity so topbar shows hostname + IP
      this._send(ws, 'init', { name: PEER_NAME, ip: LOCAL_IP });

      // Handle send requests from browser
      ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.action === 'send' && this._onSend) {
            this._onSend(msg.peerIp, msg.peerPort, msg.filename);
          }
        } catch {}
      });

      ws.on('close', () => {
        this._clients.delete(ws);
        logger.info('bridge', `browser disconnected (${this._clients.size} tab(s))`);
      });

      ws.on('error', () => this._clients.delete(ws));
    });

    this._wss.on('error', (err) => logger.error('bridge', `WS error: ${err.message}`));
    logger.info('bridge', `WebSocket on port ${WS_PORT}`);
  }
}

module.exports = new Bridge();