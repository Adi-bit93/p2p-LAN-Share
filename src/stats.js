'use strict';

const logger = require('./logger');

/**
 * Stats — lightweight in-memory transfer statistics tracker.
 * Tracks speed, success rate, total data moved, and per-peer history.
 */
class Stats {
  constructor() {
    this._reset();
  }

  _reset() {
    this.startedAt      = Date.now();
    this.sent           = 0;       // files successfully sent
    this.received       = 0;       // files successfully received
    this.failed         = 0;       // permanent failures
    this.retries        = 0;       // total retry attempts
    this.bytesSent      = 0;       // total bytes sent
    this.bytesReceived  = 0;       // total bytes received
    this.transfers      = [];      // history: [{type, filename, peer, bytes, durationMs, speedMBps}]
    this.peakSpeedMBps  = 0;       // highest speed recorded
  }

  // ── Record a completed outgoing transfer ──────────────────────────────────
  recordSent({ filename, peer, bytes, durationMs }) {
    const speedMBps = durationMs > 0
      ? parseFloat((bytes / (1024 * 1024) / (durationMs / 1000)).toFixed(2))
      : 0;

    this.sent++;
    this.bytesSent += bytes;
    if (speedMBps > this.peakSpeedMBps) this.peakSpeedMBps = speedMBps;

    this.transfers.push({ type: 'sent', filename, peer, bytes, durationMs, speedMBps });
    logger.info('stats', `sent: ${filename} to ${peer} @ ${speedMBps} MB/s`);
  }

  // ── Record a completed incoming transfer ──────────────────────────────────
  recordReceived({ filename, peer, bytes, durationMs }) {
    const speedMBps = durationMs > 0
      ? parseFloat((bytes / (1024 * 1024) / (durationMs / 1000)).toFixed(2))
      : 0;

    this.received++;
    this.bytesReceived += bytes;
    if (speedMBps > this.peakSpeedMBps) this.peakSpeedMBps = speedMBps;

    this.transfers.push({ type: 'received', filename, peer, bytes, durationMs, speedMBps });
    logger.info('stats', `received: ${filename} from ${peer} @ ${speedMBps} MB/s`);
  }

  // ── Record a failed transfer ───────────────────────────────────────────────
  recordFailure({ filename, peer, attempts }) {
    this.failed++;
    this.retries += attempts - 1;
    this.transfers.push({ type: 'failed', filename, peer, bytes: 0, durationMs: 0, speedMBps: 0 });
  }

  // ── Print a summary to the console ────────────────────────────────────────
  print() {
    const uptimeSec = Math.floor((Date.now() - this.startedAt) / 1000);
    const totalGB   = ((this.bytesSent + this.bytesReceived) / (1024 ** 3)).toFixed(3);

    console.log('\n  ┌─────────────────────────────────┐');
    console.log('  │        transfer statistics       │');
    console.log('  ├─────────────────────────────────┤');
    console.log(`  │  uptime        : ${String(uptimeSec + 's').padEnd(14)} │`);
    console.log(`  │  files sent    : ${String(this.sent).padEnd(14)} │`);
    console.log(`  │  files received: ${String(this.received).padEnd(14)} │`);
    console.log(`  │  failed        : ${String(this.failed).padEnd(14)} │`);
    console.log(`  │  retries       : ${String(this.retries).padEnd(14)} │`);
    console.log(`  │  total data    : ${String(totalGB + ' GB').padEnd(14)} │`);
    console.log(`  │  peak speed    : ${String(this.peakSpeedMBps + ' MB/s').padEnd(14)} │`);
    console.log('  └─────────────────────────────────┘\n');

    if (this.transfers.length > 0) {
      console.log('  Recent transfers:');
      this.transfers.slice(-5).forEach((t) => {
        const icon  = t.type === 'sent' ? '↑' : t.type === 'received' ? '↓' : '✗';
        const speed = t.speedMBps ? ` @ ${t.speedMBps} MB/s` : '';
        console.log(`    ${icon} ${t.filename} (${t.peer})${speed}`);
      });
      console.log('');
    }
  }

  // ── Return a plain object snapshot (for WebSocket bridge in Day 4) ────────
  snapshot() {
    return {
      uptime:        Date.now() - this.startedAt,
      sent:          this.sent,
      received:      this.received,
      failed:        this.failed,
      retries:       this.retries,
      bytesSent:     this.bytesSent,
      bytesReceived: this.bytesReceived,
      peakSpeedMBps: this.peakSpeedMBps,
      recentTransfers: this.transfers.slice(-10),
    };
  }
}

// Export singleton — one stats instance for the whole app
module.exports = new Stats();