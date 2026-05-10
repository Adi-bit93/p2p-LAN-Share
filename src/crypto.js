'use strict';

/**
 * crypto.js — AES-256-GCM file encryption/decryption
 *
 * How it works:
 *   1. A 32-byte file key is derived from the shared passphrase + a random salt
 *      using PBKDF2 (100,000 iterations of SHA-256)
 *   2. The file is encrypted chunk-by-chunk using AES-256-GCM
 *   3. The salt + IV + auth tag are sent in the transfer metadata
 *   4. Receiver derives the same key from the same passphrase + salt, decrypts
 *
 * AES-256-GCM gives us:
 *   - Confidentiality  (nobody on LAN can read the file)
 *   - Integrity        (auth tag detects tampering, on top of SHA-256 checksum)
 *   - Speed            (~500 MB/s on modern hardware via OpenSSL)
 */

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

const {
  ENCRYPTION_PASSPHRASE,
  ENCRYPTION_ALGO,
  CHUNK_SIZE,
} = require('./config');

const KEY_LEN      = 32;   // 256-bit key
const IV_LEN       = 12;   // 96-bit IV (GCM standard)
const SALT_LEN     = 16;   // 128-bit salt
const TAG_LEN      = 16;   // 128-bit auth tag
const KDF_ITERS    = 100000;
const KDF_DIGEST   = 'sha256';

// ── Derive a 256-bit key from passphrase + salt ───────────────────────────────
function deriveKey(passphrase, salt) {
  return crypto.pbkdf2Sync(passphrase, salt, KDF_ITERS, KEY_LEN, KDF_DIGEST);
}

/**
 * Encrypt a file and write the result to outPath.
 * Returns encryption metadata to embed in the transfer header.
 *
 * @param {string} inPath   — path to plaintext file
 * @param {string} outPath  — path to write encrypted output
 * @returns {{ salt, iv, authTag }} — hex strings to send in metadata
 */
async function encryptFile(inPath, outPath) {
  const salt = crypto.randomBytes(SALT_LEN);
  const iv   = crypto.randomBytes(IV_LEN);
  const key  = deriveKey(ENCRYPTION_PASSPHRASE, salt);

  const cipher    = crypto.createCipheriv(ENCRYPTION_ALGO, key, iv);
  const inStream  = fs.createReadStream(inPath,  { highWaterMark: CHUNK_SIZE });
  const outStream = fs.createWriteStream(outPath);

  await new Promise((resolve, reject) => {
    inStream.on('error',  reject);
    outStream.on('error', reject);
    outStream.on('finish', resolve);
    inStream.pipe(cipher).pipe(outStream);
  });

  // GCM auth tag is available only after the cipher is finalised (after pipe finishes)
  const authTag = cipher.getAuthTag();

  return {
    salt:    salt.toString('hex'),
    iv:      iv.toString('hex'),
    authTag: authTag.toString('hex'),
  };
}

/**
 * Decrypt a received encrypted file and write plaintext to outPath.
 * Throws if the auth tag doesn't match (tampered/corrupted data).
 *
 * @param {string} inPath      — path to encrypted file
 * @param {string} outPath     — path to write decrypted output
 * @param {object} encMeta     — { salt, iv, authTag } hex strings from metadata
 */
async function decryptFile(inPath, outPath, encMeta) {
  const salt    = Buffer.from(encMeta.salt,    'hex');
  const iv      = Buffer.from(encMeta.iv,      'hex');
  const authTag = Buffer.from(encMeta.authTag, 'hex');
  const key     = deriveKey(ENCRYPTION_PASSPHRASE, salt);

  const decipher  = crypto.createDecipheriv(ENCRYPTION_ALGO, key, iv);
  decipher.setAuthTag(authTag);

  const inStream  = fs.createReadStream(inPath,  { highWaterMark: CHUNK_SIZE });
  const outStream = fs.createWriteStream(outPath);

  try {
    await new Promise((resolve, reject) => {
      inStream.on('error',   reject);
      outStream.on('error',  reject);
      outStream.on('finish', resolve);
      decipher.on('error',   reject);
      inStream.pipe(decipher).pipe(outStream);
    });
  } catch (err) {
    try { outStream.destroy(); } catch {}
    try { if (fs.existsSync(outPath)) fs.unlinkSync(outPath); } catch {}
    throw err;
  }
}

module.exports = { encryptFile, decryptFile };