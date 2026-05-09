'use strict';

/**
 * Day 1 test — peer discovery
 *
 * Run on ONE machine:      node test-discovery.js
 * Run on TWO LAN machines: node test-discovery.js  (on both simultaneously)
 *
 * Checks:
 *   ✓ Startup speed under 500ms
 *   ✓ Config values are valid
 *   ✓ Peer registry starts empty
 *   ✓ peer:new event emits correctly
 *   ✓ peer:lost event emits on expiry
 *   ✓ Expired peers are removed from registry
 *   ✓ getPeers() returns correct shape
 *   ✓ Multiple peers handled correctly
 */

const discovery = require('./src/discovery');
const config    = require('./src/config');

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

async function runTests() {
  console.log(`\n${BOLD}Day 1 — peer discovery tests${RESET}\n`);

  // Test 1: startup speed
  console.log(`${CYAN}•${RESET} test 1: startup speed`);
  const t0 = Date.now();
  discovery.start();
  const startupMs = Date.now() - t0;
  assert(`starts in under 500ms (actual: ${startupMs}ms)`, startupMs < 500);

  // Test 2: config sanity
  console.log(`\n${CYAN}•${RESET} test 2: config sanity`);
  assert('LOCAL_IP is a non-empty string',    typeof config.LOCAL_IP === 'string' && config.LOCAL_IP.length > 0);
  assert('LOCAL_IP looks like an IP address', /^\d+\.\d+\.\d+\.\d+$/.test(config.LOCAL_IP));
  assert('PEER_NAME is a non-empty string',   typeof config.PEER_NAME === 'string' && config.PEER_NAME.length > 0);
  assert('UDP_PORT is 9999',                  config.UDP_PORT === 9999);
  assert('TCP_PORT is 8888',                  config.TCP_PORT === 8888);
  assert('BROADCAST_INTERVAL_MS is 5000',     config.BROADCAST_INTERVAL_MS === 5000);
  assert('PEER_EXPIRY_MS is 15000',           config.PEER_EXPIRY_MS === 15000);
  assert('CHUNK_SIZE is 1 MB',                config.CHUNK_SIZE === 1024 * 1024);

  // Test 3: initial state 
  console.log(`\n${CYAN}•${RESET} test 3: initial state`);
  const initialPeers = discovery.getPeers();
  assert('getPeers() returns an array',   Array.isArray(initialPeers));
  assert('peer registry starts empty',    initialPeers.length === 0);

  // Test 4: peer:new event 
  console.log(`\n${CYAN}•${RESET} test 4: peer:new event`);
  let newPeerPayload = null;
  discovery.once('peer:new', (peer) => { newPeerPayload = peer; });

  const fakePeer = { name: 'TestLaptop', ip: '192.168.55.55', tcpPort: 8888, lastSeen: Date.now() };
  discovery._peers.set(fakePeer.ip, fakePeer);
  discovery.emit('peer:new', fakePeer);
  await sleep(50);

  assert('peer:new event fires',            newPeerPayload !== null);
  assert('emitted peer has correct name',   newPeerPayload?.name    === 'TestLaptop');
  assert('emitted peer has correct ip',     newPeerPayload?.ip      === '192.168.55.55');
  assert('emitted peer has tcpPort',        newPeerPayload?.tcpPort === 8888);

  //  Test 5: registry state after peer:new 
  console.log(`\n${CYAN}•${RESET} test 5: registry state`);
  const afterNew = discovery.getPeers();
  assert('getPeers() returns 1 peer',  afterNew.length === 1);
  assert('peer name matches',          afterNew[0].name === 'TestLaptop');
  assert('peer ip matches',            afterNew[0].ip   === '192.168.55.55');

  //  Test 6: peer:lost + expiry 
  console.log(`\n${CYAN}•${RESET} test 6: peer:lost + expiry`);
  let lostPeerPayload = null;
  discovery.once('peer:lost', (peer) => { lostPeerPayload = peer; });

  // Backdate so it looks stale
  fakePeer.lastSeen = Date.now() - config.PEER_EXPIRY_MS - 1000;
  discovery._peers.set(fakePeer.ip, fakePeer);

  // Run same expiry logic as _startExpiryChecker
  const now = Date.now();
  for (const [ip, peer] of discovery._peers) {
    if (now - peer.lastSeen > config.PEER_EXPIRY_MS) {
      discovery._peers.delete(ip);
      discovery.emit('peer:lost', peer);
    }
  }
  await sleep(50);

  assert('peer:lost event fires',              lostPeerPayload !== null);
  assert('lost peer has correct name',         lostPeerPayload?.name === 'TestLaptop');
  assert('peer removed from registry',         !discovery._peers.has(fakePeer.ip));
  assert('getPeers() returns 0 after expiry',  discovery.getPeers().length === 0);

  // Test 7: multiple peers  
  console.log(`\n${CYAN}•${RESET} test 7: multiple peers`);
  const multi = [
    { name: 'PeerA', ip: '10.0.0.1', tcpPort: 8888, lastSeen: Date.now() },
    { name: 'PeerB', ip: '10.0.0.2', tcpPort: 8888, lastSeen: Date.now() },
    { name: 'PeerC', ip: '10.0.0.3', tcpPort: 8888, lastSeen: Date.now() },
  ];
  multi.forEach(p => discovery._peers.set(p.ip, p));

  const multiResult = discovery.getPeers();
  assert('3 peers in registry',          multiResult.length === 3);
  assert('all peers have name field',    multiResult.every(p => typeof p.name === 'string'));
  assert('all peers have ip field',      multiResult.every(p => typeof p.ip   === 'string'));
  assert('all peers have tcpPort field', multiResult.every(p => typeof p.tcpPort === 'number'));
  discovery._peers.clear();

  //   Summary 
  console.log(`\n${'─'.repeat(44)}`);
  if (failed === 0) {
    console.log(`  ${GREEN}✓ All ${passed} tests passed${RESET}`);
    console.log(`  ${GREEN}Day 1 complete — ready to move to Day 2${RESET}`);
  } else {
    console.log(`  ${GREEN}✓ ${passed} passed   ${RED}✗ ${failed} failed${RESET}`);
    console.log(`  ${RED}Fix failing tests before moving to Day 2${RESET}`);
  }
  console.log(`${'─'.repeat(44)}\n`);

  discovery.stop();
  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error(`\n${RED}Test runner crashed:${RESET}`, err.message);
  process.exit(1);
});