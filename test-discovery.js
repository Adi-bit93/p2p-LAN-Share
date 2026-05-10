'use strict';

/**
 * Day 3 test — concurrency, retry logic, queue, stats
 *
 * Run with:  node test-concurrency.js
 *
 * What this tests:
 *   ✓ TransferQueue starts and reports correct initial status
 *   ✓ Single job enqueues and completes via loopback
 *   ✓ 3 concurrent jobs all complete (parallel transfers)
 *   ✓ All 3 concurrent files pass checksum verification
 *   ✓ Failed job retries automatically (up to MAX_RETRIES)
 *   ✓ job:done event fires with correct metadata
 *   ✓ job:failed fires after all retries exhausted
 *   ✓ Stats records sent/received/failed correctly
 *   ✓ Stats snapshot returns correct shape
 *   ✓ App startup still under 500ms with queue + stats loaded
 */

const fs   = require('fs');
const path = require('path');
const os   = require('os');
const net  = require('net');

const { TransferQueue } = require('./src/queue');
const { Receiver }      = require('./src/receiver');
const { computeChecksum } = require('./src/checksum');
const stats             = require('./src/stats');
const config            = require('./src/config');

const GREEN = '\x1b[32m';
const RED   = '\x1b[31m';
const CYAN  = '\x1b[36m';
const BOLD  = '\x1b[1m';
const RESET = '\x1b[0m';

let passed = 0;
let failed = 0;

function assert(label, condition) {
  if (condition) {
    console.log(`  ${GREEN}✓${RESET} ${label}`);
    passed++;
  } else {
    console.log(`  ${RED}✗${RESET} ${label}`);
    failed++;
  }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── Temp directory setup ──────────────────────────────────────────────────────
const TMP = path.join(os.tmpdir(), 'p2p-day3-' + Date.now());
fs.mkdirSync(TMP, { recursive: true });

// Override downloads dir so test files don't go into real downloads folder
Object.defineProperty(config, 'DOWNLOADS_DIR', { get: () => TMP, configurable: true });

function makeFile(name, sizeBytes, fill = 0xab) {
  const p = path.join(TMP, name);
  fs.writeFileSync(p, Buffer.alloc(sizeBytes, fill));
  return p;
}

// ── Test port (avoid conflict with real app) ──────────────────────────────────
const TEST_PORT = 19888;

async function runTests() {
  console.log(`\n${BOLD}Day 3 — concurrency, retry, queue, stats tests${RESET}\n`);

  // ── Start a shared receiver for all transfer tests ────────────────────────
  const receiver = new Receiver();
  receiver.start(TEST_PORT);
  await sleep(200);

  // ── Test 1: startup speed with all modules loaded ─────────────────────────
  console.log(`${CYAN}•${RESET} test 1: startup speed (all modules)`);
  const t0 = Date.now();
  const _q = new TransferQueue();
  const _s = require('./src/stats');
  const elapsed = Date.now() - t0;
  assert(`all modules load in under 200ms (actual: ${elapsed}ms)`, elapsed < 200);

  // ── Test 2: queue initial state ───────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 2: queue initial state`);
  const queue = new TransferQueue();
  const initStatus = queue.status();
  assert('pending starts at 0',       initStatus.pending === 0);
  assert('active starts at 0',        initStatus.active  === 0);
  assert('stats.totalSent starts 0',  initStatus.stats.totalSent   === 0);
  assert('stats.totalFailed starts 0',initStatus.stats.totalFailed === 0);

  // ── Test 3: single job completes ─────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 3: single job — enqueue and complete`);

  const file1     = makeFile('job1.bin', 512 * 1024, 0x11);  // 512 KB
  const hash1     = await computeChecksum(file1);

  let job1Done    = false;
  let job1Id      = null;
  let job1DurationMs = 0;

  receiver.once('transfer:done', () => {});  // drain

  await new Promise((resolve) => {
    queue.once('job:done', (info) => {
      job1Done       = true;
      job1DurationMs = info.durationMs;
      resolve();
    });
    job1Id = queue.enqueue('127.0.0.1', TEST_PORT, file1, 'job1.bin', 512 * 1024);
  });

  assert('job returns a numeric ID',          typeof job1Id === 'number');
  assert('job:done event fires',              job1Done);
  assert('duration is a positive number',     job1DurationMs > 0);

  // Verify received file integrity
  const recvPath1 = path.join(TMP, 'job1.bin');
  if (fs.existsSync(recvPath1)) {
    const recvHash1 = await computeChecksum(recvPath1);
    assert('received file checksum matches',  recvHash1 === hash1);
  } else {
    failed++;
    console.log(`  ${RED}✗${RESET} received file checksum matches (file missing)`);
  }

  // ── Test 4: 3 concurrent jobs ─────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 4: 3 concurrent jobs`);

  // Use different fill bytes so each file has a unique checksum
  const file2 = makeFile('job2.bin', 1 * 1024 * 1024, 0x22);  // 1 MB
  const file3 = makeFile('job3.bin', 1 * 1024 * 1024, 0x33);  // 1 MB
  const file4 = makeFile('job4.bin', 1 * 1024 * 1024, 0x44);  // 1 MB

  const [hash2, hash3, hash4] = await Promise.all([
    computeChecksum(file2),
    computeChecksum(file3),
    computeChecksum(file4),
  ]);

  const queue2   = new TransferQueue();
  const doneSeen = new Set();
  let allDoneResolve;

  const allDone = new Promise(r => { allDoneResolve = r; });

  queue2.on('job:done', (info) => {
    doneSeen.add(info.id);
    if (doneSeen.size === 3) allDoneResolve();
  });

  // Drain receiver events
  let recvCount = 0;
  receiver.on('transfer:done', () => { recvCount++; });

  const tConcurrent = Date.now();

  // Enqueue all 3 at once — they should run in parallel
  const id2 = queue2.enqueue('127.0.0.1', TEST_PORT, file2, 'job2.bin', 1024 * 1024);
  const id3 = queue2.enqueue('127.0.0.1', TEST_PORT, file3, 'job3.bin', 1024 * 1024);
  const id4 = queue2.enqueue('127.0.0.1', TEST_PORT, file4, 'job4.bin', 1024 * 1024);

  await Promise.race([allDone, sleep(15000)]);
  const concurrentMs = Date.now() - tConcurrent;

  assert('all 3 jobs completed',             doneSeen.size === 3);
  assert('job IDs are all unique',            new Set([id2, id3, id4]).size === 3);
  assert('completed within 15s',             concurrentMs < 15000);

  console.log(`      (3 × 1 MB concurrent took ${concurrentMs}ms)`);

  // Verify all 3 received files
  await sleep(500); // let writes flush
  for (const [name, expectedHash] of [['job2.bin', hash2], ['job3.bin', hash3], ['job4.bin', hash4]]) {
    const p = path.join(TMP, name);
    if (fs.existsSync(p)) {
      const h = await computeChecksum(p);
      assert(`${name} checksum verified`, h === expectedHash);
    } else {
      failed++;
      console.log(`  ${RED}✗${RESET} ${name} checksum verified (file missing)`);
    }
  }

  // ── Test 5: retry logic ───────────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 5: retry logic`);

  const queue3    = new TransferQueue();
  let   attempts  = 0;
  let   failedEvt = null;

  // Use a port nobody is listening on — guaranteed to fail every attempt
  const DEAD_PORT = 19999;

  await new Promise((resolve) => {
    queue3.once('job:failed', (info) => {
      failedEvt = info;
      resolve();
    });

    // Listen to job:start to count attempts
    queue3.on('job:start', () => { attempts++; });

    queue3.enqueue('127.0.0.1', DEAD_PORT, file1, 'retry-test.bin', 512 * 1024);
  });

  assert('job:failed event fires',                  failedEvt !== null);
  assert('failed event has filename',               failedEvt?.filename === 'retry-test.bin');
  assert('failed event has reason',                 typeof failedEvt?.reason === 'string');
  assert(`retried ${config.MAX_RETRIES} time(s)`,  attempts === config.MAX_RETRIES + 1);
  assert('attempts count in event matches',         failedEvt?.attempts === config.MAX_RETRIES + 1);

  // ── Test 6: stats module ──────────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 6: stats module`);

  stats.recordSent({     filename: 'a.zip', peer: '10.0.0.1', bytes: 5 * 1024 * 1024, durationMs: 1000 });
  stats.recordSent({     filename: 'b.zip', peer: '10.0.0.2', bytes: 2 * 1024 * 1024, durationMs: 500  });
  stats.recordReceived({ filename: 'c.zip', peer: '10.0.0.3', bytes: 8 * 1024 * 1024, durationMs: 2000 });
  stats.recordFailure({  filename: 'd.zip', peer: '10.0.0.4', attempts: 4 });

  assert('sent count is 2',              stats.sent     === 2);
  assert('received count is 1',          stats.received === 1);
  assert('failed count is 1',            stats.failed   === 1);
  assert('retries count is 3',           stats.retries  === 3);  // 4 attempts - 1
  assert('bytesSent is 7 MB',            stats.bytesSent === 7 * 1024 * 1024);
  assert('bytesReceived is 8 MB',        stats.bytesReceived === 8 * 1024 * 1024);
  assert('peakSpeedMBps is set',         stats.peakSpeedMBps > 0);

  const snap = stats.snapshot();
  assert('snapshot() returns object',    typeof snap === 'object');
  assert('snapshot has sent field',      typeof snap.sent          === 'number');
  assert('snapshot has received field',  typeof snap.received      === 'number');
  assert('snapshot has bytesSent field', typeof snap.bytesSent     === 'number');
  assert('snapshot has recentTransfers', Array.isArray(snap.recentTransfers));

  // ── Test 7: queue:empty event ─────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 7: queue:empty event`);

  const queue4   = new TransferQueue();
  let emptyFired = false;

  await new Promise((resolve) => {
    queue4.once('queue:empty', () => { emptyFired = true; resolve(); });
    queue4.once('job:done',    () => {});  // drain
    queue4.enqueue('127.0.0.1', TEST_PORT, file1, 'empty-test.bin', 512 * 1024);
  });

  await sleep(200);
  assert('queue:empty fires after all jobs finish', emptyFired);

  // ── Cleanup ───────────────────────────────────────────────────────────────
  receiver.stop();
  try { fs.rmSync(TMP, { recursive: true, force: true }); } catch {}

  // ── Summary ───────────────────────────────────────────────────────────────
  console.log(`\n${'─'.repeat(44)}`);
  if (failed === 0) {
    console.log(`  ${GREEN}✓ All ${passed} tests passed${RESET}`);
    console.log(`  ${GREEN}Day 3 complete — ready to move to Day 4${RESET}`);
  } else {
    console.log(`  ${GREEN}✓ ${passed} passed   ${RED}✗ ${failed} failed${RESET}`);
    console.log(`  ${RED}Fix failing tests before moving to Day 4${RESET}`);
  }
  console.log(`${'─'.repeat(44)}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error(`\n${RED}Test runner crashed:${RESET}`, err.message);
  console.error(err.stack);
  process.exit(1);
});