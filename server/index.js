/**
 * server/index.js
 *
 * Aggregator (Option B) — multi-exchange historical backfill + aggregated buckets.
 *
 * Supported exchanges (connectors included):
 *  - binance (futures aggTrades)
 *  - binance-spot (recent trades)
 *  - okx
 *  - bybit
 *  - kucoin
 *  - bitget
 *  - gate
 *  - huobi
 *
 * Endpoints:
 *  - GET  /api/health
 *  - GET  /api/symbols
 *  - GET  /api/current-price?symbol=BTCUSDT
 *  - GET  /api/top-clusters?symbol=...&exchanges=binance,okx&periodMs=...&bucket=1&limit=100&sort=volume_desc
 *  - POST /api/backfill    { symbol, exchanges:[...], startTs?, endTs?, years? }
 *  - GET  /api/backfill/status/:id
 *  - GET  /api/backfill/jobs
 *
 * Notes:
 *  - Optionally protect POST /api/backfill with BACKFILL_KEY in env var (header x-api-key)
 *  - The backfill engine is best-effort — some exchanges expose limited history.
 */

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const Database = require('better-sqlite3');
const { default: pRetry } = require('p-retry');

const app = express();
app.use(cors());
app.use(bodyParser.json());

// --- DB setup ---
const DB_FILE = path.join(__dirname, 'data.sqlite');
const db = new Database(DB_FILE);

// create tables if needed
db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    exchange TEXT,
    symbol TEXT,
    price REAL,
    qty REAL,
    side TEXT,
    ts INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades(symbol, ts);

  CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    last_seen_ts INTEGER
  );

  CREATE TABLE IF NOT EXISTS backfill_jobs (
    id TEXT PRIMARY KEY,
    symbol TEXT,
    exchange TEXT,
    start_ts INTEGER,
    end_ts INTEGER,
    current_ts INTEGER,
    status TEXT,
    message TEXT,
    updated_at INTEGER
  );
`);

// --- utilities ---
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
function nowMs(){ return Date.now(); }
function makeJobId(){ return `job-${Math.random().toString(36).slice(2,9)}-${Date.now()}`; }
function roundToBucket(price, bucketSize){ return Math.round(price / bucketSize) * bucketSize; }

// insert trade and update symbols table
function insertTrade(trade) {
  try {
    const stmt = db.prepare('INSERT OR IGNORE INTO trades (id, exchange, symbol, price, qty, side, ts) VALUES (?, ?, ?, ?, ?, ?, ?)');
    stmt.run(trade.id, trade.exchange, trade.symbol, trade.price, trade.qty, trade.side, trade.ts);
    // update symbol last seen
    const sst = db.prepare('INSERT INTO symbols (symbol, last_seen_ts) VALUES (?, ?) ON CONFLICT(symbol) DO UPDATE SET last_seen_ts = excluded.last_seen_ts WHERE excluded.last_seen_ts > symbols.last_seen_ts');
    sst.run(trade.symbol, trade.ts || Date.now());
  } catch (e) {
    console.error('insertTrade error', e && e.message);
  }
}

// --- Exchange connectors (safe parsing) -- return arrays or empty []
async function fetchBinanceAggTrades(symbol, startTime, endTime, limit = 1000) {
  const url = `https://fapi.binance.com/fapi/v1/aggTrades?symbol=${symbol}&startTime=${startTime}&endTime=${endTime}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  return Array.isArray(r.data) ? r.data : [];
}

// binance spot recent trades (no start/end)
async function fetchBinanceSpotTrades(symbol, limit = 1000) {
  const url = `https://api.binance.com/api/v3/trades?symbol=${symbol}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  return Array.isArray(r.data) ? r.data : [];
}

// OKX recent trades (data.data)
async function fetchOKXTrades(instId, limit = 100) {
  const url = `https://www.okx.com/api/v5/market/trades?instId=${instId}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  return (r.data && r.data.data) ? r.data.data : [];
}

// Bybit recent trading records
async function fetchBybitTrades(symbol, limit = 200) {
  const url = `https://api.bybit.com/public/linear/recent-trading-records?symbol=${symbol}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  return (r.data && r.data.result) ? r.data.result : [];
}

// KuCoin histories
async function fetchKucoinTrades(symbol) {
  const url = `https://api.kucoin.com/api/v1/market/histories?symbol=${symbol}`;
  const r = await axios.get(url, { timeout: 20000 });
  // r.data.data is array of {time, price, size}
  return (r.data && r.data.data) ? r.data.data : [];
}

// Bitget recent fills (public endpoints differ between spot / futures; try safe)
async function fetchBitgetTrades(symbol) {
  // Bitget API variations — try generic endpoints; map defensively
  try {
    // futures/contract may use symbol like BTCUSDT
    const url = `https://api.bitget.com/api/spot/v1/market/trades?symbol=${symbol}`;
    const r = await axios.get(url, { timeout: 20000 });
    if (r.data && r.data.data) return r.data.data;
    if (r.data && Array.isArray(r.data)) return r.data;
  } catch (e) {
    // fallback: try mix v1 fills (may require auth rarely)
    try {
      const url2 = `https://api.bitget.com/api/mix/v1/market/fills?symbol=${symbol}`;
      const r2 = await axios.get(url2, { timeout: 20000 });
      if (r2.data && r2.data.data) return r2.data.data;
    } catch (e2) {}
  }
  return [];
}

// Gate.io recent trades
async function fetchGateTrades(symbol) {
  // gate uses currency_pair like BTC_USDT or BTC-USDT sometimes; try both
  const tryVariants = [symbol.replace('USDT','_USDT'), symbol.replace('USDT','-USDT'), symbol];
  for (const cp of tryVariants) {
    try {
      const url = `https://api.gateio.ws/api/v4/spot/trades?currency_pair=${cp}`;
      const r = await axios.get(url, { timeout: 20000 });
      if (r.data && Array.isArray(r.data)) return r.data;
    } catch (e) {
      // ignore and try next
    }
  }
  return [];
}

// Huobi recent trades
async function fetchHuobiTrades(symbol) {
  // huobi symbol often lowercase like btcusdt
  const sym = symbol.toLowerCase();
  try {
    const url = `https://api.huobi.pro/market/history/trade?symbol=${sym}`;
    const r = await axios.get(url, { timeout: 20000 });
    // r.data.data is array { id, ts, data: [ { price, amount, direction } ] }
    if (r.data && r.data.data) {
      return r.data.data.flatMap(entry => (entry.data || []).map(d => ({ ...d, ts: entry.id || entry.ts || Date.now() })));
    }
  } catch (e) {
    // some instances use hadax or hbdm; ignore
  }
  return [];
}

// Normalize and insert arrays returned by fetchers inside backfill window handling

// --- Backfill job engine (resumable) ---
async function createBackfillJob({ symbol, exchange, startTs, endTs }) {
  const jobId = makeJobId();
  const now = Date.now();
  const insert = db.prepare('INSERT INTO backfill_jobs (id, symbol, exchange, start_ts, end_ts, current_ts, status, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
  insert.run(jobId, symbol, exchange, startTs, endTs, startTs, 'pending', now);
  // run async
  runBackfill(jobId).catch(err => {
    console.error('runBackfill error for', jobId, err && err.message);
  });
  return jobId;
}

async function updateJob(jobId, props) {
  const now = Date.now();
  const row = db.prepare('SELECT * FROM backfill_jobs WHERE id = ?').get(jobId);
  if (!row) return;
  const current_ts = (props.current_ts !== undefined ? props.current_ts : row.current_ts);
  const status = props.status || row.status;
  const message = props.message || row.message;
  db.prepare('UPDATE backfill_jobs SET current_ts = ?, status = ?, message = ?, updated_at = ? WHERE id = ?')
    .run(current_ts, status, message, now, jobId);
}

async function runBackfill(jobId) {
  const jobRow = db.prepare('SELECT * FROM backfill_jobs WHERE id = ?').get(jobId);
  if (!jobRow) throw new Error('job not found ' + jobId);
  const { symbol, exchange } = jobRow;
  let current = Number(jobRow.current_ts) || Number(jobRow.start_ts);
  const end = Number(jobRow.end_ts);
  await updateJob(jobId, { status: 'running' });

  const windowMs = 60 * 60 * 1000; // 1 hour windows
  try {
    while (current <= end) {
      const wEnd = Math.min(end, current + windowMs - 1);
      try {
        // fetch based on exchange
        if (exchange === 'binance') {
          const data = await pRetry(() => fetchBinanceAggTrades(symbol, current, wEnd, 1000), { retries: 3 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              const trade = {
                id: `${symbol}-binance-${t.a || t.aggregateId || t.tradeId || Math.random()}`,
                exchange: 'binance',
                symbol,
                price: parseFloat(t.p),
                qty: parseFloat(t.q || t.qty || 0),
                side: (t.m === true) ? 'sell' : 'buy',
                ts: t.T || Date.now()
              };
              insertTrade(trade);
            }
          }
        } else if (exchange === 'binance-spot') {
          // Binance spot doesn't support start/end; best-effort for this window (fetch latest)
          const data = await pRetry(() => fetchBinanceSpotTrades(symbol, 1000), { retries: 2 });
          for (const t of data) {
            // t: { id, price, qty, quoteQty, time, isBuyerMaker }
            insertTrade({
              id: `${symbol}-binance-spot-${t.id || Math.random()}`,
              exchange: 'binance-spot',
              symbol,
              price: parseFloat(t.price),
              qty: parseFloat(t.qty || t.qty),
              side: t.isBuyerMaker ? 'sell' : 'buy',
              ts: t.time || Date.now()
            });
          }
        } else if (exchange === 'okx') {
          const instId = symbol.replace(/USDT$/i, '-USDT-SWAP');
          const data = await pRetry(() => fetchOKXTrades(instId, 100), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              // OKX trade shape: { p, sz, side, ts }
              insertTrade({
                id: `${symbol}-okx-${t.ts || Math.random()}`,
                exchange: 'okx',
                symbol,
                price: parseFloat(t.p),
                qty: parseFloat(t.sz || t.qty || 0),
                side: (t.side === 'sell' || t.side === 'S') ? 'sell' : 'buy',
                ts: Number(t.ts) || Date.now()
              });
            }
          }
        } else if (exchange === 'bybit') {
          const data = await pRetry(() => fetchBybitTrades(symbol, 200), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              insertTrade({
                id: `${symbol}-bybit-${t.trade_time_ms || Math.random()}`,
                exchange: 'bybit',
                symbol,
                price: parseFloat(t.price),
                qty: parseFloat(t.qty || t.size || 0),
                side: (t.side === 'Sell') ? 'sell' : 'buy',
                ts: t.trade_time_ms || Date.now()
              });
            }
          }
        } else if (exchange === 'kucoin') {
          const data = await pRetry(() => fetchKucoinTrades(symbol), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              // t: { time, price, size }
              insertTrade({
                id: `${symbol}-kucoin-${t.time || Math.random()}`,
                exchange: 'kucoin',
                symbol,
                price: parseFloat(t.price),
                qty: parseFloat(t.size || t.qty || 0),
                side: (t.side === 'sell' || t.side === 's') ? 'sell' : 'buy',
                ts: t.time || Date.now()
              });
            }
          }
        } else if (exchange === 'bitget') {
          const data = await pRetry(() => fetchBitgetTrades(symbol), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              // best-effort mapping: { price, size, side, ts } or different shapes
              insertTrade({
                id: `${symbol}-bitget-${t.tradeId || t.t || Math.random()}`,
                exchange: 'bitget',
                symbol,
                price: parseFloat(t.price || t.p || 0),
                qty: parseFloat(t.size || t.qty || t.amount || 0),
                side: (t.side || t.direction || '').toLowerCase().includes('sell') ? 'sell' : 'buy',
                ts: t.ts || t.time || Date.now()
              });
            }
          }
        } else if (exchange === 'gate') {
          const data = await pRetry(() => fetchGateTrades(symbol), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              // t: { time, price, amount, side? }
              insertTrade({
                id: `${symbol}-gate-${t.trade_id || t.id || Math.random()}`,
                exchange: 'gate',
                symbol,
                price: parseFloat(t.price),
                qty: parseFloat(t.amount || t.size || 0),
                side: (t.side || '').toLowerCase().includes('sell') ? 'sell' : 'buy',
                ts: t.time || Date.now()
              });
            }
          }
        } else if (exchange === 'huobi') {
          const data = await pRetry(() => fetchHuobiTrades(symbol), { retries: 2 });
          if (Array.isArray(data) && data.length) {
            for (const t of data) {
              // t likely shape { price, amount, direction, ts }
              insertTrade({
                id: `${symbol}-huobi-${t.ts || Math.random()}`,
                exchange: 'huobi',
                symbol,
                price: parseFloat(t.price || t.price),
                qty: parseFloat(t.amount || t.qty || 0),
                side: (t.direction || '').toLowerCase().includes('sell') ? 'sell' : 'buy',
                ts: t.ts || Date.now()
              });
            }
          }
        } else {
          // unknown exchange — skip
          console.warn('unknown exchange in backfill:', exchange);
        }

        // progress: mark current window done
        current = wEnd + 1;
        await updateJob(jobId, { current_ts: current, status: 'running' });
        // polite delay
        await sleep(250);
      } catch (errWindow) {
        console.error('window fetch error', { jobId, symbol, exchange, current, err: errWindow && errWindow.message });
        await updateJob(jobId, { status: 'failed', message: String(errWindow) });
        throw errWindow;
      }
    }
    await updateJob(jobId, { status: 'done', current_ts: end, message: 'completed' });
    return true;
  } catch (err) {
    await updateJob(jobId, { status: 'failed', message: String(err) });
    throw err;
  }
}

// --- API Endpoints ---

// serve UI static
app.use('/', express.static(path.join(__dirname, '../web')));

// health
app.get('/api/health', (req, res) => res.json({ ok: true, now: Date.now() }));

// symbols list
app.get('/api/symbols', (req,res) => {
  const rows = db.prepare('SELECT symbol, last_seen_ts FROM symbols ORDER BY last_seen_ts DESC').all();
  res.json({ symbols: rows.map(r => ({ symbol: r.symbol, lastSeen: r.last_seen_ts })) });
});

// current price (tries Binance -> OKX -> Bybit)
app.get('/api/current-price', async (req, res) => {
  const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
  try {
    try {
      const r = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`, { timeout: 8000 });
      if (r && r.data && r.data.price) return res.json({ price: parseFloat(r.data.price), source: 'binance' });
    } catch (e) {}
    try {
      const instId = symbol.replace(/USDT$/i, '-USDT-SWAP');
      const r2 = await axios.get(`https://www.okx.com/api/v5/market/ticker?instId=${instId}`, { timeout: 8000 });
      if (r2 && r2.data && r2.data.data && r2.data.data[0] && r2.data.data[0].last) {
        return res.json({ price: parseFloat(r2.data.data[0].last), source: 'okx' });
      }
    } catch (e) {}
    try {
      const r3 = await axios.get(`https://api.bybit.com/v2/public/tickers?symbol=${symbol}`, { timeout: 8000 });
      if (r3 && r3.data && r3.data.result && r3.data.result[0] && r3.data.result[0].last_price) {
        return res.json({ price: parseFloat(r3.data.result[0].last_price), source: 'bybit' });
      }
    } catch (e) {}
    return res.status(500).json({ error: 'no price source available' });
  } catch (err) {
    return res.status(500).json({ error: String(err) });
  }
});

// top-clusters (aggregate across exchanges)
app.get('/api/top-clusters', (req, res) => {
  try {
    const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
    let exchanges = (req.query.exchanges || 'all').split(',').map(s=>s.trim().toLowerCase()).filter(Boolean);
    const periodMs = Number(req.query.periodMs) || 24 * 3600 * 1000;
    const bucket = Number(req.query.bucket) || 1.0;
    const limit = Number(req.query.limit) || 500;
    const sort = (req.query.sort || 'volume_desc').toLowerCase();

    // if 'all' or exchange list includes 'all', query all exchanges recorded in DB OR fallback to defaults
    if (exchanges.includes('all')) {
      const rows = db.prepare('SELECT DISTINCT exchange FROM trades WHERE symbol = ?').all(symbol);
      exchanges = rows.map(r => r.exchange);
      if (!exchanges || !exchanges.length) {
        // default set
        exchanges = ['binance','binance-spot','okx','bybit','kucoin','bitget','gate','huobi'];
      }
    }

    // build SQL
    if (!exchanges.length) exchanges = ['binance'];
    const placeholders = exchanges.map(()=>'?').join(',');
    const since = Date.now() - periodMs;
    const sql = `SELECT price, qty, ts FROM trades WHERE symbol = ? AND exchange IN (${placeholders}) AND ts >= ?`;
    const params = [symbol, ...exchanges, since];
    const rows = db.prepare(sql).all(...params);

    if (!rows || !rows.length) return res.json({ symbol, exchanges, clusters: [] });

    const buckets = new Map();
    for (const r of rows) {
      const pb = roundToBucket(r.price, bucket);
      const key = pb.toFixed(8);
      const v = buckets.get(key) || { price: pb, volume: 0, lastTs: 0 };
      v.volume += r.qty;
      v.lastTs = Math.max(v.lastTs, r.ts);
      buckets.set(key, v);
    }

    let arr = Array.from(buckets.values());
    if (sort === 'price_asc') arr.sort((a,b) => a.price - b.price);
    else if (sort === 'price_desc') arr.sort((a,b) => b.price - a.price);
    else arr.sort((a,b) => {
      if (b.volume !== a.volume) return b.volume - a.volume;
      return a.price - b.price;
    });

    arr = arr.slice(0, limit);
    return res.json({ symbol, exchanges, clusters: arr });
  } catch (err) {
    console.error('/api/top-clusters error', err && err.message);
    return res.status(500).json({ error: String(err) });
  }
});

// POST /api/backfill
// Body: { symbol, exchanges:[...], startTs?, endTs?, years? }
app.post('/api/backfill', async (req,res) => {
  try {
    const KEY = process.env.BACKFILL_KEY;
    if (KEY) {
      const provided = req.headers['x-api-key'];
      if (!provided || provided !== KEY) {
        return res.status(403).json({ error: 'forbidden' });
      }
    }
    const body = req.body || {};
    const symbol = (body.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol required' });
    const exch = Array.isArray(body.exchanges) && body.exchanges.length ? body.exchanges.map(e=>e.toLowerCase()) : (body.exchanges || body.exchange ? (Array.isArray(body.exchanges) ? body.exchanges : [body.exchange]) : ['binance']);
    let startTs = Number(body.startTs) || null;
    let endTs = Number(body.endTs) || Date.now();
    if (body.years && !startTs) {
      const yrs = Number(body.years) || 1;
      startTs = Date.now() - yrs * 365 * 24 * 3600 * 1000;
    }
    if (!startTs) startTs = Date.now() - 24*3600*1000;

    const jobIds = [];
    for (const ex of exch) {
      // map alias 'binance' -> appropriate connector 'binance' (futures); allow 'binance-spot'
      const normalized = ex.toLowerCase();
      const jid = await createBackfillJob({ symbol, exchange: normalized, startTs, endTs });
      jobIds.push(jid);
    }
    return res.json({ ok: true, message: 'backfill started', jobs: jobIds });
  } catch (err) {
    console.error('/api/backfill error', err && err.message);
    return res.status(500).json({ error: String(err) });
  }
});

// job status
app.get('/api/backfill/status/:id', (req,res) => {
  const id = req.params.id;
  const row = db.prepare('SELECT * FROM backfill_jobs WHERE id = ?').get(id);
  if (!row) return res.status(404).json({ error: 'job not found' });
  res.json({ job: row });
});

// list recent jobs
app.get('/api/backfill/jobs', (req,res) => {
  const rows = db.prepare('SELECT * FROM backfill_jobs ORDER BY updated_at DESC LIMIT 50').all();
  res.json({ jobs: rows });
});

// start server
const port = process.env.PORT || 3000;
app.listen(port, ()=> {
  console.log('Server listening on', port);
});
