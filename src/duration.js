'use strict';

const UNIT_MS = {
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
};

/**
 * Parse a duration into milliseconds. Accepts a plain number (already ms),
 * or a string like "500ms", "30s", "10m", "2h", "2d", or a combination such
 * as "1h30m". This is only used for the friendly `delay` option on
 * Client#push (e.g. `{ delay: '2d' }` to mean "run 2 days from now").
 */
function parseDuration(input) {
  if (typeof input === 'number') return input;
  if (typeof input !== 'string') {
    throw new TypeError(`Duration must be a number or string, got ${typeof input}`);
  }
  const re = /(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)/g;
  let total = 0;
  let matched = false;
  let m;
  while ((m = re.exec(input)) !== null) {
    matched = true;
    total += parseFloat(m[1]) * UNIT_MS[m[2]];
  }
  if (!matched) {
    throw new TypeError(`Could not parse duration "${input}". Try "30s", "10m", "2h", "2d", etc.`);
  }
  return total;
}

module.exports = { parseDuration };
