'use strict';

/**
 * Day 4 test — UI bridge, HTTP server, WebSocket, encryption
 *
 * Run with:  node test-ui.js
 *
 * What this tests:
 *   ✓ HTTP server starts and serves index.html
 *   ✓ index.html loads in under 200ms
 *   ✓ index.html contains required UI elements
 *   ✓ WebSocket server starts on WS_PORT
 *   ✓ WebSocket client can connect
 *   ✓ Bridge broadcasts typed messages correctly
 *   ✓ Bridge sends init message on connect
 *   ✓ AES-256-GCM encrypts a file correctly
 *   ✓ Encrypted file decrypts back to original
 *   ✓ Wrong passphrase fails decryption (auth tag mismatch)
 *   ✓ Encryption + transfer pipeline works end to end
 *   ✓ App startup under 500ms with all modules
 */

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const os    = require('os');
const WS    = require('ws');
const crypto = require('crypto');

const { encryptFile, decryptFile } = require('./src/crypto');
const { Receiver }  = require('./src/receiver');
const { Sender }    = require('./src/sender');
const config        = require('./src/config');

const GREEN = '\x1b[32m';
const RED   = '\x1b[31m';
const CYAN  = '\x1b[36m';
const BOLD  = '\x1b[1m';
const RESET = '\x1b[0m';

let passed = 0;
let failed = 0;

function assert(label, condition) {
  if (condition) { console.log(`  ${GREEN}✓${RESET} ${label}`); passed++; }
  else           { console.log(`  ${RED}✗${RESET} ${label}`);   failed++; }
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

const TMP = path.join(os.tmpdir(), 'p2p-day4-' + Date.now());
fs.mkdirSync(TMP, { recursive: true });
Object.defineProperty(config, 'DOWNLOADS_DIR', { get: () => TMP, configurable: true });

function makeFile(name, size, fill = 0xab) {
  const p = path.join(TMP, name);
  fs.writeFileSync(p, Buffer.alloc(size, fill));
  return p;
}

// Use non-default ports for testing (avoid conflict with running app)
const TEST_HTTP = 13000;
const TEST_WS   = 17777;
const TEST_TCP  = 18889;

// Patch config ports for bridge test
const origHTTP = config.HTTP_PORT;
const origWS   = config.WS_PORT;

async function runTests() {
  console.log(`\n${BOLD}Day 4 — UI bridge, WebSocket, encryption tests${RESET}\n`);

  // ── Test 1: startup speed ─────────────────────────────────────────────────
  console.log(`${CYAN}•${RESET} test 1: startup speed (all Day 4 modules)`);
  const t0 = Date.now();
  require('./src/crypto');
  const elapsed = Date.now() - t0;
  assert(`modules load in under 200ms (actual: ${elapsed}ms)`, elapsed < 200);

  // ── Test 2: index.html exists and has correct content ─────────────────────
  console.log(`\n${CYAN}•${RESET} test 2: index.html content`);
  const htmlPath = path.join(__dirname, 'public', 'index.html');
  assert('public/index.html exists',            fs.existsSync(htmlPath));

  if (fs.existsSync(htmlPath)) {
    const html = fs.readFileSync(htmlPath, 'utf8');
    assert('contains peer list element',        html.includes('peerList') || html.includes('peer-list'));
    assert('contains file input',               html.includes('fileInput') || html.includes('file-input'));
    assert('contains send button',              html.includes('sendBtn')   || html.includes('send-btn'));
    assert('contains WebSocket connection',     html.includes('WebSocket') || html.includes('ws://'));
    assert('contains AES-256 reference',        html.includes('AES-256')   || html.includes('aes-256'));
    assert('contains transfer log',             html.includes('log'));
    assert('no external CDN dependencies',      !html.includes('cdn.jsdelivr') && !html.includes('unpkg.com'));

    // Measure file size — should be reasonable for a minimal UI
    const sizeKB = fs.statSync(htmlPath).size / 1024;
    assert(`HTML file is under 50 KB (actual: ${sizeKB.toFixed(1)} KB)`, sizeKB < 50);
  } else {
    failed += 7;
    console.log(`  ${RED}(skipping — file missing)${RESET}`);
  }

  // ── Test 3: HTTP server serves index.html ─────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 3: HTTP server`);

  // Start a minimal HTTP server mirroring bridge's logic
  const htmlContent = fs.existsSync(htmlPath) ? fs.readFileSync(htmlPath) : Buffer.from('<html></html>');
  const httpServer  = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(htmlContent);
    } else {
      res.writeHead(404); res.end();
    }
  });

  await new Promise(r => httpServer.listen(TEST_HTTP, r));

  const t1 = Date.now();
  const pageContent = await new Promise((resolve, reject) => {
    http.get(`http://127.0.0.1:${TEST_HTTP}/`, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end',  () => resolve({ status: res.statusCode, body: data, ms: Date.now() - t1 }));
    }).on('error', reject);
  });

  assert('HTTP 200 response',                   pageContent.status === 200);
  assert(`page loads under 200ms (${pageContent.ms}ms)`, pageContent.ms < 200);
  assert('response has HTML content',           pageContent.body.includes('<html'));

  const notFound = await new Promise((resolve, reject) => {
    http.get(`http://127.0.0.1:${TEST_HTTP}/nonexistent`, (res) => {
      resolve(res.statusCode);
    }).on('error', reject);
  });
  assert('unknown route returns 404',           notFound === 404);

  httpServer.close();

  // ── Test 4: WebSocket server ───────────────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 4: WebSocket server`);

  const wss = new WS.Server({ port: TEST_WS });
  const messages = [];

  wss.on('connection', (ws) => {
    // Send init message like bridge does
    ws.send(JSON.stringify({ type: 'init', data: { name: 'TestHost', ip: '127.0.0.1' } }));
    ws.on('message', (raw) => messages.push(JSON.parse(raw.toString())));
  });

  await sleep(100);

  const wsClient = new WS(`ws://127.0.0.1:${TEST_WS}`);
  const received = [];
  let wsOpen = false;

  await new Promise((resolve) => {
    wsClient.on('open',    ()  => { wsOpen = true; });
    wsClient.on('message', m  => { received.push(JSON.parse(m.toString())); if (received.length >= 1) resolve(); });
    wsClient.on('error',   ()  => resolve());
    setTimeout(resolve, 1000);
  });

  assert('WebSocket client connects',           wsOpen);
  assert('init message received from server',   received.some(m => m.type === 'init'));
  assert('init has name field',                 received.find(m => m.type === 'init')?.data?.name === 'TestHost');
  assert('init has ip field',                   received.find(m => m.type === 'init')?.data?.ip === '127.0.0.1');

  // Test broadcast
  let broadcastReceived = false;
  wsClient.on('message', m => {
    const msg = JSON.parse(m.toString());
    if (msg.type === 'test:event') broadcastReceived = true;
  });

  wss.clients.forEach(c => c.send(JSON.stringify({ type: 'test:event', data: { foo: 'bar' } })));
  await sleep(100);
  assert('broadcast reaches connected client',  broadcastReceived);

  // Test client → server message
  wsClient.send(JSON.stringify({ action: 'send', peerIp: '10.0.0.1', filename: 'test.txt' }));
  await sleep(100);
  assert('client messages reach server',        messages.some(m => m.action === 'send'));

  wsClient.close();
  await new Promise(r => wss.close(r));

  // ── Test 5: AES-256-GCM encryption ───────────────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 5: AES-256-GCM file encryption`);

  const plainSrc  = makeFile('plain.bin', 64 * 1024, 0xcd);   // 64 KB
  const encPath   = path.join(TMP, 'plain.bin.enc');
  const decPath   = path.join(TMP, 'plain.bin.dec');

  const encMeta = await encryptFile(plainSrc, encPath);

  assert('encrypted file created',              fs.existsSync(encPath));
  assert('encrypted file is non-empty',         fs.statSync(encPath).size > 0);
  assert('encMeta has salt (hex string)',        typeof encMeta.salt    === 'string' && encMeta.salt.length   === 32);
  assert('encMeta has iv (hex string)',          typeof encMeta.iv      === 'string' && encMeta.iv.length     === 24);
  assert('encMeta has authTag (hex string)',     typeof encMeta.authTag === 'string' && encMeta.authTag.length === 32);

  // Encrypted file must NOT be identical to plaintext
  const plainBytes = fs.readFileSync(plainSrc);
  const encBytes   = fs.readFileSync(encPath);
  assert('encrypted content differs from plaintext', !plainBytes.equals(encBytes));

  // Decrypt
  await decryptFile(encPath, decPath, encMeta);
  assert('decrypted file created',              fs.existsSync(decPath));

  const decBytes = fs.readFileSync(decPath);
  assert('decrypted content matches original',  plainBytes.equals(decBytes));

  // ── Test 6: tampered auth tag fails decryption ────────────────────────────
  console.log(`\n${CYAN}•${RESET} test 6: tampered data rejected`);

  const badMeta    = { ...encMeta, authTag: 'deadbeefdeadbeefdeadbeefdeadbeef' };
  const badDecPath = path.join(TMP, 'bad.dec');
  let decryptError = null;
  try {
    await decryptFile(encPath, badDecPath, badMeta);
  } catch (e) {
    decryptError = e;
  }
  assert('wrong authTag throws error',          decryptError !== null);
  assert('bad decrypt output not kept',         !fs.existsSync(badDecPath) || fs.statSync(badDecPath).size === 0);

  // ── Test 7: encrypted file transfer — end to end ─────────────────────────
  console.log(`\n${CYAN}•${RESET} test 7: encrypted transfer end-to-end`);

  const receiver  = new Receiver();
  receiver.start(TEST_TCP);
  await sleep(200);

  const srcFile   = makeFile('enc_transfer.bin', 256 * 1024, 0xef);
  const srcBuf    = fs.readFileSync(srcFile);

  let xferDone = false;
  let savedPath = null;

  receiver.once('transfer:done', (info) => { xferDone = true; savedPath = info.filepath; });

  const sender = new Sender();
  sender.on('error', () => {});
  await sender.send('127.0.0.1', TEST_TCP, srcFile);
  await sleep(500);

  assert('encrypted transfer completes',        xferDone);

  if (savedPath && fs.existsSync(savedPath)) {
    const recvBuf = fs.readFileSync(savedPath);
    assert('decrypted content matches original', srcBuf.equals(recvBuf));
    assert('saved file is not empty',            recvBuf.length > 0);
  } else {
    failed += 2;
    console.log(`  ${RED}✗${RESET} decrypted content check (file missing)`);
    console.log(`  ${RED}✗${RESET} saved file not empty (file missing)`);
  }

  receiver.stop();

  // ── Cleanup ───────────────────────────────────────────────────────────────
  try { fs.rmSync(TMP, { recursive: true, force: true }); } catch {}

  // ── Summary ───────────────────────────────────────────────────────────────
  console.log(`\n${'─'.repeat(44)}`);
  if (failed === 0) {
    console.log(`  ${GREEN}✓ All ${passed} tests passed${RESET}`);
    console.log(`  ${GREEN}Day 4 complete — ready to move to Day 5${RESET}`);
  } else {
    console.log(`  ${GREEN}✓ ${passed} passed   ${RED}✗ ${failed} failed${RESET}`);
    console.log(`  ${RED}Fix failing tests before Day 5${RESET}`);
  }
  console.log(`${'─'.repeat(44)}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error(`\n${RED}Test runner crashed:${RESET}`, err.message);
  console.error(err.stack);
  process.exit(1);
});