'use strict';

const crypto = require('crypto');
const fs = require('fs');
const { CHUNK_SIZE } = require('./config');

/**
 * Compute SHA-256 checksum of a file.
 * Reads in chunks — never loads entire file into RAM.
 *
 * @param {string} filePath — absolute or relative path to file
 * @returns {Promise<string>} — 64-char hex string e.g. "a3f5c2d1..."
 */
function computeChecksum(filePath) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(filePath, { highWaterMark: CHUNK_SIZE });

        stream.on('data', (chunk) => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
        stream.on('error', (err) => reject(err));
    });
}

/**
 * Verify a file matches an expected checksum.
 *
 * @param {string} filePath
 * @param {string} expectedChecksum
 * @returns {Promise<{ valid: boolean, actual: string, expected: string }>}
 */
async function verifyChecksum(filePath, expectedChecksum) {
    const actual = await computeChecksum(filePath);
    return {
        valid: actual === expectedChecksum,
        actual,
        expected: expectedChecksum,
    };
}

module.exports = { computeChecksum, verifyChecksum };