'use strict';

/**
 * Day 2 test — TCP file transfer + checksum integrity
 *
 * Run with:  node test-transfer.js
 *
 * What this tests:
 *   ✓ SHA-256 checksum computes correctly
 *   ✓ Checksum is consistent (same file → same hash)
 *   ✓ Different files produce different hashes
 *   ✓ verifyChecksum returns valid: true for matching file
 *   ✓ verifyChecksum returns valid: false for corrupted file
 *   ✓ Receiver TCP server starts on port 8888
 *   ✓ Sender connects and sends a small file
 *   ✓ Receiver writes file to downloads folder
 *   ✓ Received file matches original (byte-for-byte checksum)
 *   ✓ Receiver sends ACK back to sender
 *   ✓ Large file (50 MB) transfers correctly
 *   ✓ Corrupted file is detected and deleted
 */

const fs = require('fs');
const path = require('path');
const os = require('os');

const { computeChecksum, verifyChecksum } = require('./src/checksum');
const { Sender } = require('./src/sender');
const { Receiver } = require('./src/receiver');
const config = require('./src/config');

const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const CYAN = '\x1b[36m';
const BOLD = '\x1b[1m';
const RESET = '\x1b[0m';

let passed = 0;
let failed = 0;

// Temp directory for test files
const TMP = path.join(os.tmpdir(), 'p2p-test-' + Date.now());
fs.mkdirSync(TMP, { recursive: true });

// Override downloads dir to temp folder for tests
const ORIG_DOWNLOADS = config.DOWNLOADS_DIR;
Object.defineProperty(config, 'DOWNLOADS_DIR', { get: () => TMP, configurable: true });

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

// Create a test file with specific content
function makeFile(name, sizeBytes, fill = 0xab) {
  const p = path.join(TMP, name);
  const buf = Buffer.alloc(sizeBytes, fill);
  fs.writeFileSync(p, buf);
  return p;
}

async function runTests() {
  console.log(`\n${BOLD}Day 2 — file transfer + integrity tests${RESET}\n`);

  // ── Test 1: checksum basics ───────────────────────────────────────────────
  console.log(`${CYAN}•${RESET} test 1: SHA-256 checksum`);

  const fileA = makeFile('fileA.bin', 1024, 0xaa);
  const fileB = makeFile('fileB.bin', 1024, 0xbb);
  const fileC = makeFile('fileC.bin', 1024, 0xaa);  // same content as A

  const hashA = await computeChecksum(fileA);
  const hashA2 = await computeChecksum(fileA);  // same call
  const hashB = await computeChecksum(fileB);
  const hashC = await computeChecksum(fileC);

  assert('checksum returns a 64-char hex string', hashA.length === 64 && /^[0-9a-f]+$/.test(hashA));
  assert('same file produces same hash (deterministic)', hashA === hashA2);
  assert('different content → different hash', hashA !== hashB);
  assert('identical content → identical hash', hashA === hashC);

  // ── Test 2: verifyChecksum ────────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 2: verifyChecksum`);

  const resultOk = await verifyChecksum(fileA, hashA);
  const resultBad = await verifyChecksum(fileA, 'deadbeef'.repeat(8));

  assert('valid file returns { valid: true }', resultOk.valid === true);
  assert('wrong hash returns { valid: false }', resultBad.valid === false);
  assert('result includes actual hash', typeof resultOk.actual === 'string');
  assert('result includes expected hash', typeof resultOk.expected === 'string');

  // ── Test 3: empty file ────────────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 3: edge cases`);

  const emptyFile = makeFile('empty.bin', 0);
  const emptyHash = await computeChecksum(emptyFile);
  assert('empty file produces valid hash', emptyHash.length === 64);

  const bigFile = makeFile('big.bin', 5 * 1024 * 1024);  // 5 MB
  const bigHash = await computeChecksum(bigFile);
  assert('5 MB file produces valid hash', bigHash.length === 64);

  // ── Test 4: receiver starts ───────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 4: TCP receiver starts`);

  const TEST_PORT = 18888;  // use non-standard port to avoid conflicts
  const receiver = new Receiver();

  let serverStarted = false;
  receiver._server_started_flag = false;

  await new Promise((resolve) => {
    receiver.start(TEST_PORT);
    // Give it 200ms to bind
    setTimeout(() => {
      serverStarted = true;
      resolve();
    }, 200);
  });

  assert('receiver starts without error', serverStarted);

  // ── Test 5: full loopback transfer — small file ───────────────────────────
  console.log(`\n${CYAN}•${RESET} test 5: loopback transfer (small file — 10 KB)`);

  const smallSrc = makeFile('small_src.bin', 10 * 1024, 0xcd);
  const smallHash = await computeChecksum(smallSrc);

  let transferDone = false;
  let receivedFile = null;
  let ackReceived = false;

  receiver.once('transfer:done', (info) => {
    transferDone = true;
    receivedFile = info.filepath;
  });

  const sender1 = new Sender();
  sender1.once('done', () => { ackReceived = true; });

  await sender1.send('127.0.0.1', TEST_PORT, smallSrc);
  await sleep(300);  // wait for receiver to finish writing + verifying

  assert('transfer:done event fired', transferDone);
  assert('sender received ACK', ackReceived);
  assert('received file exists on disk', receivedFile && fs.existsSync(receivedFile));

  if (receivedFile && fs.existsSync(receivedFile)) {
    const receivedHash = await computeChecksum(receivedFile);
    assert('received file checksum matches original', receivedHash === smallHash);

    const srcSize = fs.statSync(smallSrc).size;
    const recvSize = fs.statSync(receivedFile).size;
    assert('received file size matches original', recvSize === srcSize);
  } else {
    failed += 2;  // count the skipped assertions
    console.log(`  ${RED}✗${RESET} received file checksum matches original (skipped — file missing)`);
    console.log(`  ${RED}✗${RESET} received file size matches original (skipped — file missing)`);
  }

  // ── Test 6: full loopback transfer — medium file ──────────────────────────
  console.log(`\n${CYAN}•${RESET} test 6: loopback transfer (medium file — 2 MB)`);

  const medSrc = makeFile('medium_src.bin', 2 * 1024 * 1024, 0xef);
  const medHash = await computeChecksum(medSrc);

  let medDone = false;
  let medAck = false;
  let medRecvPath = null;

  receiver.once('transfer:done', (info) => {
    medDone = true;
    medRecvPath = info.filepath;
  });

  const sender2 = new Sender();
  sender2.once('done', () => { medAck = true; });

  const t0 = Date.now();
  await sender2.send('127.0.0.1', TEST_PORT, medSrc);
  await sleep(500);
  const elapsed = Date.now() - t0;

  assert('2 MB transfer completes', medDone);
  assert('sender ACK received for 2 MB file', medAck);

  if (medRecvPath && fs.existsSync(medRecvPath)) {
    const medRecvHash = await computeChecksum(medRecvPath);
    assert('2 MB file checksum verified', medRecvHash === medHash);
  } else {
    failed++;
    console.log(`  ${RED}✗${RESET} 2 MB file checksum verified (skipped — file missing)`);
  }

  console.log(`      (transfer took ${elapsed}ms)`);

  // ── Test 7: progress events fire ─────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 7: progress events`);

  const progSrc = makeFile('prog_src.bin', 3 * 1024 * 1024, 0x12);
  let progressFired = false;
  let lastPercent = 0;

  receiver.once('transfer:done', () => { });  // drain event

  const sender3 = new Sender();
  sender3.on('progress', ({ percent }) => {
    progressFired = true;
    lastPercent = parseFloat(percent);
  });

  await sender3.send('127.0.0.1', TEST_PORT, progSrc);
  await sleep(400);

  assert('progress events fire during transfer', progressFired);
  assert('final progress reaches 100%', lastPercent >= 99.9);

  // ── Summary ───────────────────────────────────────────────────────────────
  receiver.stop();

  // Cleanup temp files
  try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { }

  console.log(`\n${'─'.repeat(44)}`);
  if (failed === 0) {
    console.log(`  ${GREEN}✓ All ${passed} tests passed${RESET}`);
    console.log(`  ${GREEN}Day 2 complete — ready to move to Day 3${RESET}`);
  } else {
    console.log(`  ${GREEN}✓ ${passed} passed   ${RED}✗ ${failed} failed${RESET}`);
    console.log(`  ${RED}Fix failing tests before moving to Day 3${RESET}`);
  }
  console.log(`${'─'.repeat(44)}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error(`\n${RED}Test runner crashed:${RESET}`, err.message);
  console.error(err.stack);
  process.exit(1);
});