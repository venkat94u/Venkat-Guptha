// server/index.js (CommonJS)
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());

// serve frontend
app.use('/', express.static(path.join(__dirname, '..', 'web')));

const PORT = process.env.PORT || 3000;

// helpers
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

// Binance APIs
const BINANCE_KLINES_URL = 'https://api.binance.com/api/v3/klines';
const BINANCE_PRICE_URL = 'https://api.binance.com/api/v3/ticker/price';

// fetch klines for a specific time range (endTime inclusive)
async function fetchKlinesRange(symbol, interval, startTime, endTime, limit=1000){
  const url = `${BINANCE_KLINES_URL}?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  return r.data;
}

// small concurrency pool for safe parallel fetches
async function asyncPool(poolLimit, array, iteratorFn) {
  const ret = [];
  const executing = [];
  for (const item of array) {
    const p = Promise.resolve().then(() => iteratorFn(item));
    ret.push(p);

    if (poolLimit <= array.length) {
      const e = p.then(()=> executing.splice(executing.indexOf(e), 1));
      executing.push(e);
      if (executing.length >= poolLimit) {
        await Promise.race(executing);
      }
    }
  }
  return Promise.all(ret);
}

/**
 * Fetch ~N months worth of 5m candles by constructing chunk endTimes and fetching multiple ranges in parallel.
 * intervalMs = 5m
 */
async function fetchPastRangeCandles(symbol, interval='5m', months=3, concurrency=4){
  const now = Date.now();
  const monthsMs = (months * 30 * 24 * 60 * 60 * 1000); // approx
  const startTime = now - monthsMs;
  // Binance limit 1000 candles per request. For 5m candles, 1000 * 5min = 5000 minutes = 83.3 hours.
  // We will create endTime chunks stepping backwards by chunkMs.
  const intervalMs = 5 * 60 * 1000;
  const candlesNeeded = Math.ceil(monthsMs / intervalMs);
  const perRequest = 1000;
  const chunkMs = perRequest * intervalMs;

  // Build endTime array (from now -> backwards) for each chunk
  const endTimes = [];
  let end = now;
  while (end > startTime) {
    endTimes.push(end);
    end = end - chunkMs;
  }

  // We'll fetch chunks in parallel with limited concurrency
  const results = await asyncPool(concurrency, endTimes, async (endT)=>{
    // compute start for this chunk
    const startT = Math.max(startTime, endT - chunkMs + intervalMs);
    // call fetchKlinesRange
    try {
      const data = await fetchKlinesRange(symbol, interval, startT, endT, perRequest);
      // map to structured objects
      return data.map(c => ({
        time: c[0],
        open: Number(c[1]),
        high: Number(c[2]),
        low: Number(c[3]),
        close: Number(c[4]),
        volume: Number(c[5])
      }));
    } catch (err) {
      // On failure, return empty array (caller can decide)
      console.warn('chunk fetch failed', err && err.message);
      return [];
    }
  });

  // results is array of arrays in descending time (most recent chunk first)
  // flatten and sort by time asc
  const flat = results.flat().sort((a,b)=> a.time - b.time);

  // safety: return up to candlesNeeded
  return flat.slice(-Math.min(flat.length, candlesNeeded));
}


// compute delta spikes using volume change (fast, robust)
function extractDeltaSpikeZones(candles, currentPrice, opts = {}){
  const minVolumeFactor = opts.minVolumeFactor || 3; // volume >= median * factor
  const minDistance = opts.minDistance || 50;       // at least ~50 price points away from same cluster
  const maxDistanceFromPrice = opts.maxDistanceFromPrice || 300; // only consider spikes within this many points from current price
  const maxLevels = opts.maxLevels || 30;

  if (!candles || candles.length < 2) return { above: [], below: [] };

  // gather volumes
  const volumes = candles.map(c=>c.volume).filter(v=>v>0);
  const medianVol = volumes.length ? median(volumes) : 0;
  const threshold = Math.max(1, medianVol * minVolumeFactor);

  const spikes = [];
  for (let i=1;i<candles.length;i++){
    const prev = candles[i-1];
    const cur = candles[i];
    const vol = cur.volume || 0;
    // price used for zone = rounded close price
    const price = Number(cur.close);
    if (vol >= threshold) {
      const deltaVol = vol - (prev.volume || 0);
      spikes.push({
        price,
        volume: vol,
        deltaVol,
        time: cur.time,
        distance: Math.abs(price - currentPrice)
      });
    }
  }

  if (!spikes.length) return { above: [], below: [] };

  // filter by distance from currentPrice
  const near = spikes.filter(s => s.distance <= maxDistanceFromPrice);

  // sort by deltaVol desc (strongest spikes first)
  const sorted = near.sort((a,b) => Math.abs(b.deltaVol) - Math.abs(a.deltaVol));

  // remove spikes that are too close to each other (< minDistance)
  const cleaned = [];
  for (const s of sorted) {
    if (!cleaned.some(c => Math.abs(c.price - s.price) < minDistance)) {
      cleaned.push(s);
      if (cleaned.length >= maxLevels * 2) break;
    }
  }

  // split into above / below and return closest-first
  const above = cleaned.filter(z=>z.price > currentPrice).sort((a,b)=>a.distance - b.distance).slice(0, maxLevels);
  const below = cleaned.filter(z=>z.price < currentPrice).sort((a,b)=>a.distance - b.distance).slice(0, maxLevels);

  return { above, below };
}

function median(arr){
  if (!arr.length) return 0;
  const s = [...arr].sort((a,b)=>a-b);
  const mid = Math.floor(s.length/2);
  return s.length % 2 ? s[mid] : (s[mid-1] + s[mid]) / 2;
}

// API: /api/zones?symbol=BTCUSDT&periodMonths=3&interval=5m
app.get('/api/zones', async (req, res) => {
  const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
  const months = Math.max(1, Number(req.query.periodMonths || 3));
  const interval = req.query.interval || '5m';
  // tuning params
  const minVolumeFactor = Number(req.query.minVolumeFactor || 3);
  const maxDistanceFromPrice = Number(req.query.maxDistance || 300);
  const minSeparation = Number(req.query.minSeparation || 50);
  const maxLevels = Number(req.query.maxLevels || 30);

  try {
    // current price
    const priceResp = await axios.get(`${BINANCE_PRICE_URL}?symbol=${symbol}`, { timeout: 10000 });
    const currentPrice = Number(priceResp.data.price);

    // fetch candles (this may take a second or two)
    const candles = await fetchPastRangeCandles(symbol, interval, months, 4);

    // compute zones
    const zones = extractDeltaSpikeZones(candles, currentPrice, {
      minVolumeFactor,
      minDistance: minSeparation,
      maxDistanceFromPrice,
      maxLevels
    });

    res.json({
      ok: true,
      symbol,
      currentPrice,
      above: zones.above.map(normalizeZone),
      below: zones.below.map(normalizeZone)
    });
  } catch (err) {
    console.error('api/zones error', err && err.message);
    res.status(500).json({ ok:false, error: err && err.message });
  }
});

function normalizeZone(z){
  return {
    price: z.price,
    volume: Number(z.volume),
    deltaVol: Number(z.deltaVol || 0),
    time: new Date(z.time).toISOString(),
    distance: Number(z.distance)
  };
}

app.get('/', (req,res)=> {
  // the static middleware will serve web/index.html automatically when hitting root.
  res.sendFile(path.join(__dirname, '..', 'web', 'index.html'));
});

app.listen(PORT, ()=> {
  console.log(`Delta Spike S/R server listening on ${PORT}`);
});
