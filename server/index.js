// server/index.js
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use('/', express.static(path.join(__dirname, '..', 'web')));

const PORT = process.env.PORT || 3000;

const BINANCE_KLINES_URL = 'https://api.binance.com/api/v3/klines';
const BINANCE_PRICE_URL  = 'https://api.binance.com/api/v3/ticker/price';
const BINANCE_EXCHANGE_INFO = 'https://api.binance.com/api/v3/exchangeInfo';

// ----------------- utils -----------------
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function asyncPool(poolLimit, array, iteratorFn) {
  const ret = [];
  const executing = [];
  for (const item of array) {
    const p = Promise.resolve().then(() => iteratorFn(item));
    ret.push(p);

    if (poolLimit <= array.length) {
      const e = p.then(() => executing.splice(executing.indexOf(e), 1));
      executing.push(e);
      if (executing.length >= poolLimit) {
        await Promise.race(executing);
      }
    }
  }
  return Promise.all(ret);
}

function median(arr){
  if (!arr || !arr.length) return 0;
  const s = [...arr].sort((a,b) => a - b);
  const mid = Math.floor(s.length/2);
  return (s.length % 2) ? s[mid] : (s[mid-1] + s[mid]) / 2;
}

// ----------------- symbols endpoint -----------------
app.get('/api/symbols', async (req, res) => {
  try {
    const r = await axios.get(BINANCE_EXCHANGE_INFO, { timeout: 8000 });
    const symbols = (r.data && r.data.symbols) ? r.data.symbols
      .filter(s => s.symbol && s.symbol.endsWith('USDT') && s.status === 'TRADING')
      .map(s => s.symbol) : [];
    res.json({ ok: true, symbols });
  } catch (err) {
    console.error('symbols error', err && err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ----------------- fetch past range candles (5m, multi-chunk) -----------------
/**
 * Fetch 'months' months of 5m candles by chunking endTimes backwards and fetching in parallel.
 * returns array of { time, close, volume } ordered ascending by time.
 */
async function fetchPastRangeCandles(symbol, interval='5m', months=3, concurrency=4){
  const now = Date.now();
  const monthsMs = months * 30 * 24 * 60 * 60 * 1000;
  const startTime = now - monthsMs;

  const intervalMs = 5 * 60 * 1000;
  const perRequest = 1000; // Binance max per request
  const chunkMs = perRequest * intervalMs;

  // build endTimes array from now backwards
  const endTimes = [];
  let end = now;
  while (end > startTime) {
    endTimes.push(end);
    end -= chunkMs;
  }

  // parallel fetch with pool
  const results = await asyncPool(concurrency, endTimes, async (endT) => {
    const startT = Math.max(startTime, endT - chunkMs + intervalMs);
    const url = `${BINANCE_KLINES_URL}?symbol=${symbol}&interval=${interval}&startTime=${startT}&endTime=${endT}&limit=${perRequest}`;
    try {
      const r = await axios.get(url, { timeout: 20000 });
      if (!Array.isArray(r.data) || r.data.length === 0) return [];
      return r.data.map(c => ({
        time: c[0],
        close: Number(c[4]),
        volume: Number(c[5])
      }));
    } catch (err) {
      console.warn('chunk fetch failed for endT', endT, err && err.message);
      // small backoff then return empty
      await sleep(200);
      return [];
    }
  });

  // flatten + sort ascending
  const flat = results.flat().sort((a,b) => a.time - b.time);

  // estimate needed candles (safety)
  const needed = Math.ceil((months * 30 * 24 * 60) / 5); // months * days * hours * 12 (5m)
  return flat.slice(-Math.min(flat.length, needed));
}

// ----------------- spike extraction -----------------
/**
 * Extract strong volume spikes and return top levels based on filters:
 * - rangeType: 'percent' or 'points'
 * - rangeValue: numeric value of range
 * - gap: minimum separation between zones (points)
 * - maxLevels: number per side (we will return up to maxLevels)
 */
function extractZonesFiltered(candles, currentPrice, opts){
  const { rangeType='percent', rangeValue=3, gap=35, maxLevels=10, minVolumeFactor=3 } = opts;

  if (!candles || candles.length < 2) return { above: [], below: [] };

  const volumes = candles.map(c => c.volume).filter(v => v > 0);
  const med = median(volumes) || 0.0001;
  const threshold = Math.max(1, med * minVolumeFactor);

  // collect spikes where volume >= threshold
  const spikes = [];
  for (let i=1;i<candles.length;i++){
    const prev = candles[i-1];
    const cur  = candles[i];
    if (!cur || typeof cur.volume !== 'number') continue;
    if (cur.volume >= threshold) {
      spikes.push({
        price: Number(cur.close),
        volume: Number(cur.volume),
        deltaVol: Number(cur.volume - (prev.volume || 0)),
        time: cur.time,
        distance: Math.abs(Number(cur.close) - currentPrice)
      });
    }
  }

  if (!spikes.length) return { above: [], below: [] };

  // apply range filter: keep only spikes within chosen % or points
  let maxDistanceAllowed;
  if (rangeType === 'percent') {
    const pct = Number(rangeValue) || 3;
    maxDistanceAllowed = Math.abs(currentPrice) * (Math.max(0.001, pct) / 100);
  } else {
    maxDistanceAllowed = Number(rangeValue) || 300;
  }

  const inRange = spikes.filter(s => s.distance <= maxDistanceAllowed);

  // sort by deltaVol desc (strength)
  inRange.sort((a,b) => Math.abs(b.deltaVol) - Math.abs(a.deltaVol));

  // remove near-near by gap and build cleaned list
  const cleaned = [];
  for (const s of inRange) {
    if (!cleaned.some(c => Math.abs(c.price - s.price) < gap)) {
      cleaned.push(s);
      // keep some extra cleaned candidates (we'll pick per side later)
      if (cleaned.length >= maxLevels * 6) break;
    }
  }

  // split and pick up to maxLevels closest-first per side
  const above = cleaned.filter(z => z.price > currentPrice)
    .sort((a,b) => a.distance - b.distance)
    .slice(0, maxLevels);

  const below = cleaned.filter(z => z.price < currentPrice)
    .sort((a,b) => a.distance - b.distance)
    .slice(0, maxLevels);

  return { above, below };
}

// normalize zone for output
function normalizeZone(z){
  return {
    price: z.price,
    volume: Number(z.volume),
    deltaVol: Number(z.deltaVol),
    time: new Date(z.time).toISOString(),
    distance: Number(z.distance)
  };
}

// ----------------- main API: /api/zones -----------------
/**
 * Query params:
 *  - symbol (default BTCUSDT)
 *  - rangeType: 'percent' | 'points' (default 'percent')
 *  - rangeValue: numeric (default 3 => 3% if percent)
 *  - gap: min separation points (default 35)
 *  - maxLevels: number per side (default 10)
 *  - months: how many months to fetch (default 3)
 */
app.get('/api/zones', async (req, res) => {
  const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
  const rangeType = (req.query.rangeType || 'percent');
  const rangeValue = Number(req.query.rangeValue || 3);
  const gap = Number(req.query.gap || req.query.minSeparation || 35);
  const maxLevels = Math.max(1, Math.min(50, Number(req.query.maxLevels || 10)));
  const months = Math.max(1, Math.min(6, Number(req.query.months || 3)));

  try {
    // get current price
    const priceResp = await axios.get(`${BINANCE_PRICE_URL}?symbol=${symbol}`, { timeout: 8000 });
    const currentPrice = Number(priceResp.data.price);

    // fetch candles (may take ~1-3s for 3 months, concurrency helps)
    const candles = await fetchPastRangeCandles(symbol, '5m', months, 4);

    const zones = extractZonesFiltered(candles, currentPrice, {
      rangeType,
      rangeValue,
      gap,
      maxLevels,
      minVolumeFactor: 3
    });

    res.json({
      ok: true,
      symbol,
      currentPrice,
      above: (zones.above || []).slice(0, maxLevels).map(normalizeZone),
      below: (zones.below || []).slice(0, maxLevels).map(normalizeZone)
    });
  } catch (err) {
    console.error('api/zones error', err && err.message);
    res.status(500).json({ ok:false, error: err && err.message });
  }
});

// root fallback - static middleware serves index.html
app.get('/', (req,res) => {
  res.sendFile(path.join(__dirname, '..', 'web', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Delta Spike S/R (B1) server listening on ${PORT}`);
});
