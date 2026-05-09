'use strict';

const net    = require('net');
const fs     = require('fs');
const path   = require('path');
const { computeChecksum }           = require('./checksum');
const { TCP_PORT, CHUNK_SIZE }      = require('./config');
const logger                        = require('./logger');
const { EventEmitter }              = require('events');

/**
 * Sender — connects to a peer over TCP and streams a file.
 *
 * Events emitted:
 *   progress  ({ filename, sent, total, percent, speedMBps })
 *   done      ({ filename, checksum, durationMs })
 *   error     (err)
 */
class Sender extends EventEmitter {

  /**
   * Send a file to a peer.
   *
   * @param {string} peerIp     — target IP address
   * @param {number} peerPort   — target TCP port (default: TCP_PORT)
   * @param {string} filePath   — absolute path to file to send
   * @returns {Promise<void>}   — resolves when transfer is complete
   */
  send(peerIp, peerPort = TCP_PORT, filePath) {
    return new Promise((resolve, reject) => {

      // ── Pre-flight checks ────────────────────────────────────────────────
      if (!fs.existsSync(filePath)) {
        const err = new Error(`File not found: ${filePath}`);
        this.emit('error', err);
        return reject(err);
      }

      const stat     = fs.statSync(filePath);
      const filename = path.basename(filePath);
      const filesize = stat.size;

      logger.info('sender', `preparing "${filename}" (${formatBytes(filesize)})`);

      // ── Compute checksum before connecting ───────────────────────────────
      computeChecksum(filePath)
        .then((checksum) => {
          logger.info('sender', `checksum: ${checksum.slice(0, 16)}...`);
          this._connect(peerIp, peerPort, filePath, filename, filesize, checksum, resolve, reject);
        })
        .catch((err) => {
          logger.error('sender', `checksum failed: ${err.message}`);
          this.emit('error', err);
          reject(err);
        });
    });
  }

  // ── Internal: open TCP connection and stream the file ──────────────────────
  _connect(peerIp, peerPort, filePath, filename, filesize, checksum, resolve, reject) {
    const socket = net.createConnection(peerPort, peerIp);

    // Kill Nagle's algorithm — send chunks immediately, no buffering delay
    socket.setNoDelay(true);

    const startTime = Date.now();

    socket.on('connect', () => {
      logger.info('sender', `connected to ${peerIp}:${peerPort}`);

      // ── Build and send metadata header ──────────────────────────────────
      const metadata = JSON.stringify({ filename, filesize, checksum });
      const metaBuf  = Buffer.from(metadata, 'utf8');
      const lenBuf   = Buffer.alloc(4);
      lenBuf.writeUInt32BE(metaBuf.length, 0);

      // Write: [4 bytes: metadata length][N bytes: metadata JSON][file data]
      socket.write(lenBuf);
      socket.write(metaBuf);

      // ── Stream file in chunks ────────────────────────────────────────────
      const readStream = fs.createReadStream(filePath, {
        highWaterMark: CHUNK_SIZE,
      });

      let sent        = 0;
      let lastLogTime = Date.now();

      readStream.on('data', (chunk) => {
        socket.write(chunk);
        sent += chunk.length;

        const percent   = ((sent / filesize) * 100).toFixed(1);
        const elapsed   = (Date.now() - startTime) / 1000;
        const speedMBps = (sent / (1024 * 1024) / elapsed).toFixed(2);

        this.emit('progress', { filename, sent, total: filesize, percent, speedMBps });

        // Log to console every 2 seconds so it's not spammy
        if (Date.now() - lastLogTime > 2000) {
          logger.info('sender', `sending "${filename}" — ${percent}% @ ${speedMBps} MB/s`);
          lastLogTime = Date.now();
        }
      });

      readStream.on('end', () => {
        // File fully streamed — wait for receiver's ACK before resolving
        logger.info('sender', `stream complete — waiting for ACK...`);
      });

      readStream.on('error', (err) => {
        logger.error('sender', `read error: ${err.message}`);
        socket.destroy();
        this.emit('error', err);
        reject(err);
      });
    });

    // ── Receiver sends back a one-line JSON ACK after verifying checksum ──
    let ackBuffer = '';
    socket.on('data', (chunk) => {
      ackBuffer += chunk.toString();
      if (ackBuffer.includes('\n')) {
        try {
          const ack         = JSON.parse(ackBuffer.trim());
          const durationMs  = Date.now() - startTime;

          if (ack.status === 'ok') {
            logger.success('sender', `"${filename}" confirmed by receiver in ${durationMs}ms`);
            this.emit('done', { filename, checksum, durationMs });
            resolve();
          } else {
            const err = new Error(`Receiver rejected file: ${ack.reason}`);
            logger.error('sender', err.message);
            this.emit('error', err);
            reject(err);
          }
        } catch {
          // incomplete JSON — wait for more data
        }
        socket.destroy();
      }
    });

    socket.on('error', (err) => {
      logger.error('sender', `socket error: ${err.message}`);
      this.emit('error', err);
      reject(err);
    });

    socket.on('close', () => {
      logger.info('sender', 'connection closed');
    });
  }
}

// ── Utility ──────────────────────────────────────────────────────────────────
function formatBytes(bytes) {
  if (bytes < 1024)             return `${bytes} B`;
  if (bytes < 1024 * 1024)      return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)        return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

module.exports = { Sender, formatBytes };