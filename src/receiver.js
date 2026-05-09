'use strict';

const net    = require('net');
const fs     = require('fs');
const path   = require('path');
const { EventEmitter }              = require('events');
const { verifyChecksum }            = require('./checksum');
const { TCP_PORT, DOWNLOADS_DIR }   = require('./config');
const logger                        = require('./logger');

/**
 * Receiver — TCP server that accepts incoming file transfers.
 *
 * Events emitted:
 *   transfer:start    ({ filename, filesize, from })
 *   transfer:progress ({ filename, received, total, percent })
 *   transfer:done     ({ filename, filepath, checksum, durationMs })
 *   transfer:error    ({ filename, reason })
 */
class Receiver extends EventEmitter {

  constructor() {
    super();
    this._server = null;
  }

  // ── Start the TCP server ───────────────────────────────────────────────────
  start(port = TCP_PORT) {
    // Ensure downloads directory exists
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

  // ── Stop the TCP server ────────────────────────────────────────────────────
  stop() {
    if (this._server) {
      this._server.close();
      logger.info('receiver', 'TCP server stopped');
    }
  }

  // ── Handle one incoming connection ─────────────────────────────────────────
  _handleConnection(socket) {
    const from = socket.remoteAddress;
    logger.info('receiver', `incoming connection from ${from}`);

    // State machine for this connection
    const state = {
      phase:        'meta-length',  // meta-length → meta-body → file
      metaLength:   0,
      metaReceived: 0,
      metaBuf:      Buffer.alloc(0),
      metadata:     null,
      fileStream:   null,
      received:     0,
      startTime:    Date.now(),
      filepath:     null,
    };

    socket.on('data', (chunk) => {
      try {
        this._processChunk(socket, state, chunk, from);
      } catch (err) {
        logger.error('receiver', `processing error: ${err.message}`);
        this._sendAck(socket, false, err.message);
        socket.destroy();
      }
    });

    socket.on('error', (err) => {
      logger.error('receiver', `socket error from ${from}: ${err.message}`);
      // Clean up partial file
      if (state.filepath && fs.existsSync(state.filepath)) {
        fs.unlinkSync(state.filepath);
        logger.warn('receiver', `deleted partial file: ${state.filepath}`);
      }
    });

    socket.on('close', () => {
      logger.info('receiver', `connection closed from ${from}`);
    });
  }

  // ── Process incoming data chunk (state machine) ────────────────────────────
  _processChunk(socket, state, chunk, from) {
    let offset = 0;

    // ── Phase 1: read 4-byte metadata length ──────────────────────────────
    while (state.phase === 'meta-length' && offset < chunk.length) {
      state.metaBuf = Buffer.concat([state.metaBuf, chunk.slice(offset, offset + 1)]);
      offset++;
      if (state.metaBuf.length === 4) {
        state.metaLength   = state.metaBuf.readUInt32BE(0);
        state.metaBuf      = Buffer.alloc(0);  // reset for meta body
        state.phase        = 'meta-body';
        logger.info('receiver', `expecting ${state.metaLength} bytes of metadata`);
      }
    }

    // ── Phase 2: read metadata JSON body ──────────────────────────────────
    if (state.phase === 'meta-body' && offset < chunk.length) {
      const needed  = state.metaLength - state.metaBuf.length;
      const slice   = chunk.slice(offset, offset + needed);
      state.metaBuf = Buffer.concat([state.metaBuf, slice]);
      offset       += slice.length;

      if (state.metaBuf.length === state.metaLength) {
        // Parse metadata
        state.metadata = JSON.parse(state.metaBuf.toString('utf8'));
        const { filename, filesize, checksum } = state.metadata;

        logger.info('receiver', `receiving "${filename}" (${formatBytes(filesize)})`);
        logger.info('receiver', `expected checksum: ${checksum.slice(0, 16)}...`);

        // Open write stream
        state.filepath   = path.join(DOWNLOADS_DIR, sanitizeFilename(filename));
        state.fileStream = fs.createWriteStream(state.filepath);
        state.phase      = 'file';

        this.emit('transfer:start', { filename, filesize, from });
      }
    }

    // ── Phase 3: stream file data to disk ─────────────────────────────────
    if (state.phase === 'file' && offset < chunk.length) {
      const fileChunk     = chunk.slice(offset);
      const { filename, filesize } = state.metadata;

      // Don't write more than expected (guard against extra bytes)
      const remaining     = filesize - state.received;
      const toWrite       = fileChunk.slice(0, remaining);

      state.fileStream.write(toWrite);
      state.received += toWrite.length;

      const percent = ((state.received / filesize) * 100).toFixed(1);
      this.emit('transfer:progress', {
        filename,
        received: state.received,
        total:    filesize,
        percent,
      });

      // Print progress every 10%
      if (Math.floor(state.received / filesize * 10) >
          Math.floor((state.received - toWrite.length) / filesize * 10)) {
        logger.info('receiver', `"${filename}" — ${percent}%`);
      }

      // ── File fully received ────────────────────────────────────────────
      if (state.received >= filesize) {
        state.fileStream.end(() => {
          this._finalizeTransfer(socket, state, from);
        });
      }
    }
  }

  // ── Verify checksum and send ACK ───────────────────────────────────────────
  async _finalizeTransfer(socket, state, from) {
    const { filename, checksum: expected } = state.metadata;
    const durationMs = Date.now() - state.startTime;

    logger.info('receiver', `verifying checksum for "${filename}"...`);

    try {
      const result = await verifyChecksum(state.filepath, expected);

      if (result.valid) {
        logger.success('receiver', `"${filename}" verified OK in ${durationMs}ms`);
        this._sendAck(socket, true);
        this.emit('transfer:done', {
          filename,
          filepath:  state.filepath,
          checksum:  result.actual,
          durationMs,
        });
      } else {
        logger.error('receiver', `checksum MISMATCH for "${filename}"`);
        logger.error('receiver', `  expected: ${result.expected}`);
        logger.error('receiver', `  actual:   ${result.actual}`);

        // Delete corrupted file
        fs.unlinkSync(state.filepath);
        logger.warn('receiver', `deleted corrupted file: ${state.filepath}`);

        this._sendAck(socket, false, 'checksum mismatch — file corrupted');
        this.emit('transfer:error', { filename, reason: 'checksum mismatch' });
      }
    } catch (err) {
      logger.error('receiver', `verification error: ${err.message}`);
      this._sendAck(socket, false, err.message);
    }
  }

  // ── Send JSON ACK back to sender ───────────────────────────────────────────
  _sendAck(socket, success, reason = '') {
    const ack = success
      ? JSON.stringify({ status: 'ok' }) + '\n'
      : JSON.stringify({ status: 'error', reason }) + '\n';

    socket.write(ack);
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function formatBytes(bytes) {
  if (bytes < 1024)        return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3)   return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

// Strip path traversal and dangerous characters from filename
function sanitizeFilename(filename) {
  return path.basename(filename).replace(/[^a-zA-Z0-9._\- ]/g, '_');
}

module.exports = { Receiver };