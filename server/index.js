/**
 * Hybrid Fast Trend-Origin Detector backend
 * - Aggregates exchange trades into minute bins
 * - Computes buy/sell delta per price-bucket
 * - Exposes endpoints: backfill, backfill status, current-price, top-zones
 *
 * Notes:
 * - This is an MVP focused on correctness and clarity.
 * - In production add authentication, rate-limits, retries, and queueing.
 */
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const Database = require('better-sqlite3');
const pRetry = require('p-retry');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(bodyParser.json());

// --- DB init ---
const DB_FILE = path.join(__dirname, 'data.sqlite');
const db = new Database(DB_FILE);

// tables:
// minute_bins: aggregated per symbol, exchange, minute_ts, price_bucket -> buyVol, sellVol
db.exec(`
CREATE TABLE IF NOT EXISTS minute_bins (
  id TEXT PRIMARY KEY,
  symbol TEXT,
  exchange TEXT,
  minute_ts INTEGER,
  bucket REAL,
  buy_vol REAL DEFAULT 0,
  sell_vol REAL DEFAULT 0,
  last_ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_minute_symbol_ts ON minute_bins(symbol, minute_ts);
CREATE INDEX IF NOT EXISTS idx_minute_symbol_bucket ON minute_bins(symbol, bucket);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  symbol TEXT,
  exchange TEXT,
  status TEXT,
  created_at INTEGER,
  updated_at INTEGER,
  message TEXT
);
`);

// helper: upsert bin
const upsertBin = db.prepare(`
INSERT INTO minute_bins (id, symbol, exchange, minute_ts, bucket, buy_vol, sell_vol, last_ts)
VALUES (@id,@symbol,@exchange,@minute_ts,@bucket,@buy_vol,@sell_vol,@last_ts)
ON CONFLICT(id) DO UPDATE SET
  buy_vol = minute_bins.buy_vol + @buy_vol,
  sell_vol = minute_bins.sell_vol + @sell_vol,
  last_ts = MAX(minute_bins.last_ts, @last_ts)
`);

// insert job
const insertJob = db.prepare(`INSERT INTO jobs (id,symbol,exchange,status,created_at,updated_at,message) VALUES (@id,@symbol,@exchange,@status,@created_at,@updated_at,@message)`);
const updateJob = db.prepare(`UPDATE jobs SET status=@status, updated_at=@updated_at, message=@message WHERE id=@id`);
const getJob = db.prepare(`SELECT * FROM jobs WHERE id = ?`);

// --- config ---
const PRICE_BUCKET_SIZE = 1.0; // default price bucket; can be changed by query param
const DEFAULT_LIMIT = 300;

// --- exchange fetchers (minimal) ---
// Each fetcher returns an array of trades: { price: number, qty: number, side: 'buy'|'sell', ts: epoch_ms }

async function fetchBinanceTrades(symbol, limit=1000, startTime=null, endTime=null){
  // symbol like BTCUSDT, use aggTrades which returns 'p' price, 'q' qty, 'm' maker flag (true means seller maker)
  const qs = { limit };
  if (startTime) qs.startTime = startTime;
  if (endTime) qs.endTime = endTime;
  const url = `https://fapi.binance.com/fapi/v1/aggTrades?symbol=${symbol}&${new URLSearchParams(qs).toString()}`;
  const r = await axios.get(url, { timeout: 20000 });
  // r.data is array
  return r.data.map(t => ({
    price: parseFloat(t.p),
    qty: parseFloat(t.q),
    side: (t.m === true) ? 'sell' : 'buy',
    ts: t.T || Date.now()
  }));
}

async function fetchOKXTrades(symbol, limit=100){
  // OKX uses different symbol conventions; user must supply correct instId if needed.
  // We'll try typical futures convention: symbol like BTCUSDT -> BTC-USDT-SWAP
  let instId = symbol;
  if (!symbol.includes('-')) {
    instId = symbol.replace(/USDT$/i, '-USDT-SWAP');
  }
  const url = `https://www.okx.com/api/v5/market/trades?instId=${instId}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  // r.data.data is array of { side, px, sz, ts }
  if (!r.data || !r.data.data) return [];
  return (r.data.data || []).map(t => ({
    price: parseFloat(t.px || t.p),
    qty: parseFloat(t.sz || t.sz || 0),
    side: (String(t.side || '').toLowerCase()==='sell') ? 'sell' : 'buy',
    ts: Number(t.ts) || Date.now()
  }));
}

async function fetchBybitTrades(symbol, limit=200){
  // Bybit linear: symbol same e.g. BTCUSDT
  const url = `https://api.bybit.com/public/linear/recent-trading-records?symbol=${symbol}&limit=${limit}`;
  const r = await axios.get(url, { timeout: 20000 });
  if (!r.data || !r.data.result) return [];
  return (r.data.result || []).map(t => ({
    price: parseFloat(t.price),
    qty: parseFloat(t.qty || 0),
    side: (t.side === 'Sell' || t.side === 'sell') ? 'sell' : 'buy',
    ts: Number(t.trade_time_ms) || Date.now()
  }));
}

// Map exchange -> fetcher
const FETCHERS = {
  binance: fetchBinanceTrades,
  okx: fetchOKXTrades,
  bybit: fetchBybitTrades
};

// --- aggregation helpers ---
function toMinuteTs(ts) {
  return Math.floor(ts / 60000) * 60000;
}
function bucketPrice(price, bucketSize) {
  return Math.round(price / bucketSize) * bucketSize;
}

// process trades into DB minute_bins
function ingestTrades(trades, symbol, exchange, bucketSize = PRICE_BUCKET_SIZE) {
  // trades: array of {price, qty, side, ts}
  const now = Date.now();
  const insert = db.transaction((rows) => {
    for (const t of rows) {
      const minute_ts = toMinuteTs(t.ts || now);
      const bucket = bucketPrice(t.price, bucketSize);
      const id = `${symbol}::${exchange}::${minute_ts}::${bucket}`;
      const obj = {
        id, symbol, exchange, minute_ts, bucket,
        buy_vol: t.side === 'buy' ? (t.qty || 0) : 0,
        sell_vol: t.side === 'sell' ? (t.qty || 0) : 0,
        last_ts: t.ts || now
      };
      upsertBin.run(obj);
    }
  });
  insert(trades);
}

// --- backfill job runner (simple) ---
async function runBackfillJob(jobId, symbol, exchange, years=1, bucketSize=PRICE_BUCKET_SIZE) {
  const start = Date.now();
  updateJob.run({ id: jobId, status: 'running', updated_at: Date.now(), message: 'starting' });
  try {
    // We'll fetch recent trades in windows. For safety we limit the number of requests.
    // Strategy: fetch last N minutes windows until years satisfied or limit reached.
    const now = Date.now();
    const from = now - (years * 365 * 24 * 3600 * 1000);
    const windowMs = 60 * 60 * 1000; // 1 hour per request for binance aggTrades (safe)
    for (let s = from; s < now; s += windowMs) {
      const e = Math.min(now, s + windowMs - 1);
      // call fetcher with retries
      const fetcher = FETCHERS[exchange];
      if (!fetcher) {
        updateJob.run({ id: jobId, status: 'failed', updated_at: Date.now(), message: `no fetcher for ${exchange}` });
        return;
      }
      try {
        const trades = await pRetry(() => fetcher(symbol, 1000, s, e), { retries: 2, minTimeout: 400 });
        if (Array.isArray(trades) && trades.length) {
          ingestTrades(trades, symbol, exchange, bucketSize);
          updateJob.run({ id: jobId, status: 'running', updated_at: Date.now(), message: `processed window ${new Date(s).toISOString()}` });
        } else {
          updateJob.run({ id: jobId, status: 'running', updated_at: Date.now(), message: `no trades window ${new Date(s).toISOString()}` });
        }
      } catch (err) {
        // log and continue
        console.warn('fetch window err', exchange, symbol, s, err.message || err);
        updateJob.run({ id: jobId, status: 'running', updated_at: Date.now(), message: `error window ${new Date(s).toISOString()}: ${String(err.message||err)}` });
      }
      // small pause to be nice
      await new Promise(r => setTimeout(r, 200));
    }

    updateJob.run({ id: jobId, status: 'done', updated_at: Date.now(), message: 'backfill complete' });
  } catch (err) {
    console.error('job error', err);
    updateJob.run({ id: jobId, status: 'failed', updated_at: Date.now(), message: String(err) });
  } finally {
    console.log(`job ${jobId} finished in ${(Date.now()-start)/1000}s`);
  }
}

// --- API routes ---

// serve frontend static
app.use('/', express.static(path.join(__dirname, '../web')));

// POST /api/backfill
// body: { symbol, exchanges: ['binance','okx'], years: 1, bucketSize: 1 }
app.post('/api/backfill', (req, res) => {
  const body = req.body || {};
  const symbol = (body.symbol || '').toUpperCase();
  const exchanges = body.exchanges || (body.exchange ? [body.exchange] : ['binance']);
  const years = Number(body.years) || 1;
  const bucketSize = Number(body.bucketSize) || PRICE_BUCKET_SIZE;

  if (!symbol) return res.status(400).json({ error: 'symbol required' });

  const jobs = [];
  for (const ex of exchanges) {
    const jid = uuidv4();
    const now = Date.now();
    insertJob.run({ id: jid, symbol, exchange: ex, status: 'queued', created_at: now, updated_at: now, message: 'queued' });
    // start job (fire-and-forget)
    runBackfillJob(jid, symbol, ex, years, bucketSize).catch(e => console.error(e));
    jobs.push(jid);
  }
  res.json({ ok: true, jobs });
});

// GET /api/backfill/status/:id
app.get('/api/backfill/status/:id', (req, res) => {
  const id = req.params.id;
  const j = getJob.get(id);
  if (!j) return res.status(404).json({error:'job not found'});
  res.json({ job: j });
});

// GET /api/current-price?symbol=BTCUSDT
// we try Binance public ticker first; fallback to last minute bin price
app.get('/api/current-price', async (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase();
  if (!symbol) return res.status(400).json({error:'symbol required'});
  try {
    // Binance ticker (works for many symbols)
    const r = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`, { timeout: 5000 });
    if (r.data && r.data.price) return res.json({ price: Number(r.data.price) });
  } catch (e) {
    // ignore and fallback
  }
  // fallback: derive from last minute_bins
  const row = db.prepare(`SELECT bucket, minute_ts FROM minute_bins WHERE symbol = ? ORDER BY minute_ts DESC LIMIT 1`).get(symbol);
  if (row) return res.json({ price: row.bucket });
  return res.status(404).json({error:'price not available'});
});

// GET /api/top-zones?symbol=BTCUSDT&exchanges=binance,okx&periodMs=86400000&bucket=1&limit=300&sort=volume_desc
app.get('/api/top-zones', (req, res) => {
  const symbol = (req.query.symbol || '').toUpperCase();
  const exStr = req.query.exchanges || 'binance';
  const exchanges = exStr.split(',').map(s=>s.trim()).filter(Boolean);
  const periodMs = Number(req.query.periodMs) || (24*3600*1000);
  const bucketSize = Number(req.query.bucket) || PRICE_BUCKET_SIZE;
  const limit = Number(req.query.limit) || DEFAULT_LIMIT;
  const sort = req.query.sort || 'delta_desc'; // delta_desc | volume_desc | price_asc

  if (!symbol) return res.status(400).json({error:'symbol required'});

  const since = Date.now() - periodMs;
  // aggregate across exchanges, minute_ts >= since
  const rows = db.prepare(`
    SELECT bucket,
           SUM(buy_vol) as buy_vol,
           SUM(sell_vol) as sell_vol,
           MAX(last_ts) as lastTs
    FROM minute_bins
    WHERE symbol = ? AND minute_ts >= ? AND (@exchanges_clause)
    GROUP BY bucket
  `).all; // we will build query manually because sqlite doesn't support array param easily

  // Build manual query string
  const exClause = exchanges.map(e => `'${e.replace(/'/g,"''")}'`).join(',');
  const q = `
    SELECT bucket,
           SUM(buy_vol) as buy_vol,
           SUM(sell_vol) as sell_vol,
           MAX(last_ts) as lastTs
    FROM minute_bins
    WHERE symbol = ? AND minute_ts >= ? AND exchange IN (${exClause})
    GROUP BY bucket
  `;
  const data = db.prepare(q).all(symbol, since);

  // compute delta and volume
  const zones = data.map(r => {
    const buy = Number(r.buy_vol || 0);
    const sell = Number(r.sell_vol || 0);
    const delta = buy - sell;
    const vol = buy + sell;
    return { price: Number(r.bucket), buy, sell, delta, volume: vol, lastTs: r.lastTs || null };
  });

  // Sorting
  if (sort === 'delta_desc') zones.sort((a,b)=> Math.abs(b.delta) - Math.abs(a.delta));
  else if (sort === 'volume_desc') zones.sort((a,b)=> b.volume - a.volume);
  else if (sort === 'price_asc') zones.sort((a,b)=> a.price - b.price);
  else if (sort === 'price_desc') zones.sort((a,b)=> b.price - a.price);

  res.json({ clusters: zones.slice(0, limit) });
});

// GET /api/symbols -> returns known symbols in DB (last seen time)
app.get('/api/symbols', (req,res) => {
  const rows = db.prepare(`SELECT symbol, MAX(minute_ts) as lastSeen FROM minute_bins GROUP BY symbol ORDER BY lastSeen DESC LIMIT 500`).all();
  res.json({ symbols: rows.map(r => ({symbol: r.symbol, lastSeen: r.lastSeen})) });
});

const port = process.env.PORT || 3000;
app.listen(port, ()=> console.log('Trend-Origin server listening on', port));
