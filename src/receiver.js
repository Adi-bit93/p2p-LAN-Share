'use strict';

const net    = require('net');
const fs     = require('fs');
const path   = require('path');
const os     = require('os');
const { EventEmitter }            = require('events');
const { verifyChecksum }          = require('./checksum');
const { decryptFile }             = require('./crypto');
const { TCP_PORT, DOWNLOADS_DIR } = require('./config');
const logger                      = require('./logger');

/**
 * Receiver — accepts TCP file transfers, verifies checksum, decrypts if needed.
 *
 * Events:
 *   transfer:start    ({ filename, filesize, from, encrypted })
 *   transfer:progress ({ filename, received, total, percent })
 *   transfer:done     ({ filename, filepath, durationMs })
 *   transfer:error    ({ filename, reason })
 */
class Receiver extends EventEmitter {

  constructor() {
    super();
    this._server = null;
  }

  start(port = TCP_PORT) {
    if (!fs.existsSync(DOWNLOADS_DIR)) {
      fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
    }

    this._server = net.createServer((socket) => {
      socket.setNoDelay(true);
      this._handleConnection(socket);
    });

    this._server.listen(port, () => {
      logger.info('receiver', `TCP server listening on port ${port}`);
    });

    this._server.on('error', (err) => {
      logger.error('receiver', `server error: ${err.message}`);
    });
  }

  stop() {
    if (this._server) {
      this._server.close();
      logger.info('receiver', 'TCP server stopped');
    }
  }

  // ── Handle one incoming TCP connection ────────────────────────────────────
  _handleConnection(socket) {
    const from = socket.remoteAddress;
    logger.info('receiver', `incoming connection from ${from}`);

    const state = {
      phase:      'meta-length',
      metaBuf:    Buffer.alloc(0),
      metaLength: 0,
      metadata:   null,
      fileStream: null,
      filepath:   null,   // path where raw (possibly encrypted) bytes land
      received:   0,
      startTime:  Date.now(),
    };

    socket.on('data', (chunk) => {
      try {
        this._processChunk(socket, state, chunk, from);
      } catch (err) {
        logger.error('receiver', `processing error: ${err.message}`);
        this._ack(socket, false, err.message);
        socket.destroy();
      }
    });

    socket.on('error', (err) => {
      logger.error('receiver', `socket error from ${from}: ${err.message}`);
      this._cleanupPartial(state);
    });

    socket.on('close', () => {
      logger.info('receiver', `connection closed from ${from}`);
    });
  }

  // ── State machine: meta-length → meta-body → file ─────────────────────────
  _processChunk(socket, state, chunk, from) {
    let offset = 0;

    // Phase 1: read 4-byte metadata length
    while (state.phase === 'meta-length' && offset < chunk.length) {
      state.metaBuf = Buffer.concat([state.metaBuf, chunk.slice(offset, offset + 1)]);
      offset++;
      if (state.metaBuf.length === 4) {
        state.metaLength = state.metaBuf.readUInt32BE(0);
        state.metaBuf    = Buffer.alloc(0);
        state.phase      = 'meta-body';
      }
    }

    // Phase 2: read metadata JSON
    if (state.phase === 'meta-body' && offset < chunk.length) {
      const needed  = state.metaLength - state.metaBuf.length;
      const slice   = chunk.slice(offset, offset + needed);
      state.metaBuf = Buffer.concat([state.metaBuf, slice]);
      offset       += slice.length;

      if (state.metaBuf.length === state.metaLength) {
        state.metadata = JSON.parse(state.metaBuf.toString('utf8'));
        const { filename, filesize, encrypted } = state.metadata;

        logger.info('receiver', `receiving "${filename}" (${formatBytes(filesize)})${encrypted ? ' [encrypted]' : ''}`);

        // Write raw bytes to temp file — will decrypt after if needed
        const rawName   = `p2p-raw-${Date.now()}-${sanitize(filename)}`;
        state.filepath  = path.join(os.tmpdir(), rawName);
        state.fileStream = fs.createWriteStream(state.filepath);
        state.phase     = 'file';

        this.emit('transfer:start', { filename, filesize, from, encrypted: !!encrypted });
      }
    }

    // Zero-byte file: skip streaming entirely
    if (state.phase === 'file' && state.metadata.filesize === 0) {
      state.fileStream.end(() => this._finalise(socket, state, from));
      return;
    }

    // Phase 3: write file bytes to disk
    if (state.phase === 'file' && offset < chunk.length) {
      const { filename, filesize } = state.metadata;
      const remaining = filesize - state.received;
      const toWrite   = chunk.slice(offset, offset + remaining);

      state.fileStream.write(toWrite);
      state.received += toWrite.length;

      const percent = ((state.received / filesize) * 100).toFixed(1);
      this.emit('transfer:progress', { filename, received: state.received, total: filesize, percent });

      // log every ~10%
      if (Math.floor(state.received / filesize * 10) >
          Math.floor((state.received - toWrite.length) / filesize * 10)) {
        logger.info('receiver', `"${filename}" — ${percent}%`);
      }

      if (state.received >= filesize) {
        state.fileStream.end(() => this._finalise(socket, state, from));
      }
    }
  }

  // ── Verify checksum → decrypt → move to downloads ─────────────────────────
  async _finalise(socket, state, from) {
    const { filename, checksum: expected, encrypted, enc } = state.metadata;
    const durationMs = Date.now() - state.startTime;

    try {
      // Step 1: verify checksum of raw (encrypted) bytes
      logger.info('receiver', `verifying checksum for "${filename}"...`);
      const result = await verifyChecksum(state.filepath, expected);

      if (!result.valid) {
        logger.error('receiver', `checksum MISMATCH for "${filename}"`);
        this._cleanupPartial(state);
        this._ack(socket, false, 'checksum mismatch — file corrupted in transit');
        this.emit('transfer:error', { filename, reason: 'checksum mismatch' });
        return;
      }

      logger.success('receiver', `checksum OK for "${filename}"`);

      // Step 2: decrypt if needed
      const finalPath = uniquePath(DOWNLOADS_DIR, sanitize(filename));

      if (encrypted && enc) {
        logger.info('receiver', `decrypting "${filename}"...`);
        await decryptFile(state.filepath, finalPath, enc);
        fs.unlinkSync(state.filepath);   // remove encrypted temp file
        logger.success('receiver', `decrypted "${filename}" → ${finalPath}`);
      } else {
        // Just move from temp to downloads
        fs.renameSync(state.filepath, finalPath);
      }

      logger.success('receiver', `"${filename}" saved in ${durationMs}ms`);
      this._ack(socket, true);
      this.emit('transfer:done', { filename, filepath: finalPath, durationMs });

    } catch (err) {
      logger.error('receiver', `finalise error: ${err.message}`);
      this._cleanupPartial(state);

      // AES-GCM auth tag mismatch = tampered file
      const reason = err.message.includes('auth') || err.message.includes('GCM')
        ? 'decryption failed — data may be tampered'
        : err.message;

      this._ack(socket, false, reason);
      this.emit('transfer:error', { filename, reason });
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────
  _ack(socket, ok, reason = '') {
    const msg = ok
      ? JSON.stringify({ status: 'ok' }) + '\n'
      : JSON.stringify({ status: 'error', reason }) + '\n';
    socket.write(msg);
  }

  _cleanupPartial(state) {
    if (state.filepath && fs.existsSync(state.filepath)) {
      try { fs.unlinkSync(state.filepath); } catch {}
      logger.warn('receiver', `deleted partial/temp file`);
    }
  }
}

function formatBytes(b) {
  if (b < 1024)        return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1024 ** 3)   return `${(b / (1024 * 1024)).toFixed(1)} MB`;
  return `${(b / 1024 ** 3).toFixed(2)} GB`;
}

function sanitize(filename) {
  return path.basename(filename).replace(/[^a-zA-Z0-9._\- ]/g, '_');
}

function uniquePath(dir, filename) {
  const ext  = path.extname(filename);
  const base = path.basename(filename, ext);
  let   candidate = path.join(dir, filename);
  let   counter   = 1;
  while (fs.existsSync(candidate)) {
    candidate = path.join(dir, `${base}(${counter})${ext}`);
    counter++;
  }
  return candidate;
}

module.exports = { Receiver };