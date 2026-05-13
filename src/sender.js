'use strict';

const net    = require('net');
const fs     = require('fs');
const path   = require('path');
const os     = require('os');

const { computeChecksum }              = require('./checksum');
const { encryptFile }                  = require('./crypto');
const { TCP_PORT, CHUNK_SIZE,
        ENCRYPTION_ENABLED }           = require('./config');
const logger                           = require('./logger');
const { EventEmitter }                 = require('events');

/**
 * Sender — encrypts (optionally) then streams a file to a peer over TCP.
 *
 * Events:
 *   progress  ({ filename, sent, total, percent, speedMBps })
 *   done      ({ filename, checksum, durationMs })
 *   error     (err)
 */
class Sender extends EventEmitter {

  send(peerIp, peerPort = TCP_PORT, filePath) {
    return new Promise((resolve, reject) => {

      if (!fs.existsSync(filePath)) {
        const err = new Error(`File not found: ${filePath}`);
        this.emit('error', err);
        return reject(err);
      }

      const stat     = fs.statSync(filePath);
      const filename = path.basename(filePath);
      const filesize = stat.size;

      logger.info('sender', `preparing "${filename}" (${formatBytes(filesize)})`);

      this._prepare(peerIp, peerPort, filePath, filename, filesize, resolve, reject);
    });
  }

  // ── Step 1: optionally encrypt → compute checksum → connect ───────────────
  async _prepare(peerIp, peerPort, filePath, filename, filesize, resolve, reject) {
    try {
      let sendPath  = filePath;   // path of file actually sent (may be encrypted copy)
      let encMeta   = null;       // encryption metadata, or null if disabled

      if (ENCRYPTION_ENABLED) {
        // Write encrypted copy to OS temp dir
        const tmpPath = path.join(os.tmpdir(), `p2p-enc-${Date.now()}-${filename}`);
        logger.info('sender', `encrypting "${filename}"...`);
        encMeta  = await encryptFile(filePath, tmpPath);
        sendPath = tmpPath;
        logger.info('sender', `encrypted  "${filename}" → temp file`);
      }

      // Checksum of what we actually send (encrypted bytes)
      const checksum = await computeChecksum(sendPath);
      const sendStat = fs.statSync(sendPath);
      logger.info('sender', `checksum: ${checksum.slice(0, 16)}...`);

      this._connect(
        peerIp, peerPort,
        sendPath, filename, sendStat.size,
        checksum, encMeta,
        filePath,   // original path — for cleanup reference only
        resolve, reject
      );
    } catch (err) {
      logger.error('sender', `prepare error: ${err.message}`);
      this.emit('error', err);
      reject(err);
    }
  }

  // ── Step 2: open TCP connection and stream encrypted file ──────────────────
  _connect(peerIp, peerPort, sendPath, filename, filesize,
           checksum, encMeta, origPath, resolve, reject) {

    const socket    = net.createConnection(peerPort, peerIp);
    const startTime = Date.now();
    const isTemp    = sendPath !== origPath;

    socket.setNoDelay(true);
    socket.setTimeout(10000);  // 10s connection timeout

    socket.on('timeout', () => {
      logger.warn('sender', 'connection timed out after 10s');
      socket.destroy(new Error('Connection timed out'));
    });

    socket.on('connect', () => {
      logger.info('sender', `connected to ${peerIp}:${peerPort}`);

      // ── Build metadata header ────────────────────────────────────────────
      const meta = {
        filename,
        filesize,
        checksum,
        encrypted: !!encMeta,
        ...(encMeta && { enc: encMeta }),   // include salt/iv/authTag if encrypted
      };

      const metaBuf = Buffer.from(JSON.stringify(meta), 'utf8');
      const lenBuf  = Buffer.alloc(4);
      lenBuf.writeUInt32BE(metaBuf.length, 0);

      socket.write(lenBuf);
      socket.write(metaBuf);

      // ── Stream file ──────────────────────────────────────────────────────
      const readStream = fs.createReadStream(sendPath, { highWaterMark: CHUNK_SIZE });
      let   sent       = 0;
      let   lastLog    = Date.now();

      readStream.on('data', (chunk) => {
        socket.write(chunk);
        sent += chunk.length;

        const percent   = ((sent / filesize) * 100).toFixed(1);
        const elapsed   = (Date.now() - startTime) / 1000 || 0.001;
        const speedMBps = (sent / (1024 * 1024) / elapsed).toFixed(2);

        this.emit('progress', { filename, sent, total: filesize, percent, speedMBps });

        if (Date.now() - lastLog > 2000) {
          logger.info('sender', `"${filename}" — ${percent}% @ ${speedMBps} MB/s`);
          lastLog = Date.now();
        }
      });

      readStream.on('end', () => {
        logger.info('sender', `stream complete — waiting for ACK...`);
      });

      readStream.on('error', (err) => {
        logger.error('sender', `read error: ${err.message}`);
        socket.destroy();
        this.emit('error', err);
        reject(err);
      });
    });

    // ── Wait for ACK from receiver ────────────────────────────────────────
    let ackBuf = '';
    socket.on('data', (chunk) => {
      ackBuf += chunk.toString();
      if (!ackBuf.includes('\n')) return;

      try {
        const ack        = JSON.parse(ackBuf.trim());
        const durationMs = Date.now() - startTime;

        if (ack.status === 'ok') {
          logger.success('sender', `"${filename}" confirmed by receiver in ${durationMs}ms`);
          this.emit('done', { filename, checksum, durationMs });
          resolve();
        } else {
          const err = new Error(`Receiver rejected: ${ack.reason}`);
          logger.error('sender', err.message);
          this.emit('error', err);
          reject(err);
        }
      } catch {
        // partial JSON — wait for more
      }

      // Cleanup temp encrypted file
      if (isTemp && fs.existsSync(sendPath)) {
        try { fs.unlinkSync(sendPath); } catch {}
      }

      socket.destroy();
    });

    socket.on('error', (err) => {
      logger.error('sender', `socket error: ${err.message}`);
      if (isTemp && fs.existsSync(sendPath)) {
        try { fs.unlinkSync(sendPath); } catch {}
      }
      this.emit('error', err);
      reject(err);
    });

    socket.on('close', () => logger.info('sender', 'connection closed'));
  }
}

function formatBytes(bytes) {
  if (bytes < 1024)        return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)   return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

module.exports = { Sender, formatBytes };