'use strict';

const { EventEmitter } = require('events');
const { Sender }       = require('./sender');
const { MAX_RETRIES }  = require('./config');
const logger           = require('./logger');

/**
 * TransferQueue — manages outgoing file transfers.
 *
 * - Runs up to MAX_CONCURRENT transfers simultaneously
 * - Auto-retries up to MAX_RETRIES times on checksum failure or network error
 * - Emits events for UI to consume
 *
 * Events emitted:
 *   job:queued    ({ id, filename, peer })
 *   job:start     ({ id, filename, peer, attempt })
 *   job:progress  ({ id, filename, percent, speedMBps })
 *   job:done      ({ id, filename, peer, durationMs, attempts })
 *   job:failed    ({ id, filename, peer, reason, attempts })
 *   queue:empty   ()
 */

const MAX_CONCURRENT = 3;  // max simultaneous outgoing transfers

class TransferQueue extends EventEmitter {
  constructor() {
    super();
    this._queue   = [];     // pending jobs
    this._active  = new Map(); // jobId → job (currently running)
    this._counter = 0;     // job ID counter
    this._stats   = {
      totalSent:    0,
      totalFailed:  0,
      totalBytes:   0,
      totalRetries: 0,
    };
  }

  // ── Add a file transfer job to the queue ──────────────────────────────────
  enqueue(peerIp, peerPort, filePath, filename, filesize) {
    const id  = ++this._counter;
    const job = {
      id,
      peerIp,
      peerPort,
      filePath,
      filename,
      filesize,
      attempt:   0,
      queuedAt:  Date.now(),
    };

    this._queue.push(job);
    logger.info('queue', `job #${id} queued — "${filename}" → ${peerIp}`);
    this.emit('job:queued', { id, filename, peer: peerIp });

    // Try to start immediately if a slot is free
    this._tick();
    return id;
  }

  // ── Current status snapshot ───────────────────────────────────────────────
  status() {
    return {
      pending:  this._queue.length,
      active:   this._active.size,
      stats:    { ...this._stats },
    };
  }

  // ── Internal: start as many jobs as concurrent slots allow ───────────────
  _tick() {
    while (this._active.size < MAX_CONCURRENT && this._queue.length > 0) {
      const job = this._queue.shift();
      this._run(job);
    }

    if (this._active.size === 0 && this._queue.length === 0) {
      this.emit('queue:empty');
    }
  }

  // ── Internal: run one job with retry logic ────────────────────────────────
  async _run(job) {
    job.attempt++;
    this._active.set(job.id, job);

    logger.info('queue',
      `job #${job.id} starting (attempt ${job.attempt}/${MAX_RETRIES + 1}) — "${job.filename}"`
    );
    this.emit('job:start', { id: job.id, filename: job.filename, peer: job.peerIp, attempt: job.attempt });

    const sender = new Sender();

    // Attach error listener immediately to prevent unhandled error crash
    // (actual error is caught by the try/catch below)
    sender.on("error", () => {});

    // Forward progress events with job id attached
    sender.on('progress', ({ filename, percent, speedMBps }) => {
      this.emit('job:progress', { id: job.id, filename, percent, speedMBps });
    });

    try {
      const t0 = Date.now();
      await sender.send(job.peerIp, job.peerPort, job.filePath);
      const durationMs = Date.now() - t0;

      // ── Success ──────────────────────────────────────────────────────────
      this._stats.totalSent++;
      this._stats.totalBytes  += job.filesize || 0;
      this._stats.totalRetries += (job.attempt - 1);

      logger.success('queue', `job #${job.id} done in ${durationMs}ms after ${job.attempt} attempt(s)`);
      this.emit('job:done', {
        id:        job.id,
        filename:  job.filename,
        peer:      job.peerIp,
        durationMs,
        attempts:  job.attempt,
      });

    } catch (err) {
      // ── Failure — retry or give up ────────────────────────────────────────
      if (job.attempt <= MAX_RETRIES) {
        const delay = job.attempt * 2000; // 2s, 4s, 6s backoff
        logger.warn('queue',
          `job #${job.id} failed (attempt ${job.attempt}) — retrying in ${delay}ms... reason: ${err.message}`
        );
        this._stats.totalRetries++;
        this._active.delete(job.id);

        // Put back at front of queue after delay
        setTimeout(() => {
          this._queue.unshift(job);
          this._tick();
        }, delay);
        return; // don't fall through to final cleanup
      }

      // All retries exhausted
      this._stats.totalFailed++;
      logger.error('queue',
        `job #${job.id} permanently failed after ${job.attempt} attempt(s) — "${job.filename}"`
      );
      this.emit('job:failed', {
        id:       job.id,
        filename: job.filename,
        peer:     job.peerIp,
        reason:   err.message,
        attempts: job.attempt,
      });
    }

    // Job finished (success or permanent failure) — free the slot
    this._active.delete(job.id);
    this._tick();
  }
}

module.exports = { TransferQueue };