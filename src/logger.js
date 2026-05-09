'use strict';

const LEVELS = { info: '•', warn: '!', error: '✗', success: '✓', debug: '…' };

function log(level, module, message) {
  const time  = new Date().toTimeString().slice(0, 8);
  const icon  = LEVELS[level] || '•';
  console.log(`${time} ${icon} [${module}] ${message}`);
}

module.exports = {
  info:    (mod, msg) => log('info',    mod, msg),
  warn:    (mod, msg) => log('warn',    mod, msg),
  error:   (mod, msg) => log('error',   mod, msg),
  success: (mod, msg) => log('success', mod, msg),
  debug:   (mod, msg) => log('debug',   mod, msg),
};