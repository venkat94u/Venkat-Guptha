import express from "express";
import axios from "axios";
import cors from "cors";

const app = express();
app.use(cors());
app.use(express.json());

// ----------- Binance Klines Fetcher ----------
async function fetchBinance(symbol, interval, limit = 1000) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const r = await axios.get(url);
  return r.data.map(c => ({
    openTime: c[0],
    open: Number(c[1]),
    high: Number(c[2]),
    low: Number(c[3]),
    close: Number(c[4]),
    volume: Number(c[5]),
  }));
}

// ----------- SIMPLE DELTA SPIKE DETECTOR -----------
function detectDeltaSpikes(candles, threshold = 2.0) {
  const zones = [];

  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i - 1];
    const cur = candles[i];

    const delta = Math.abs(cur.close - prev.close);

    if (delta >= threshold) {
      zones.push({
        price: cur.close,
        delta,
        time: cur.openTime
      });
    }
  }

  return zones;
}

// ----------- CURRENT PRICE -----------
app.get("/api/price", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();
    const url = `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`;
    const r = await axios.get(url);
    return res.json({ price: Number(r.data.price) });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// ----------- ZONE FINDER -----------
app.get("/api/zones", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();
    const interval = req.query.interval || "1h";
    const threshold = Number(req.query.threshold || 2);

    const candles = await fetchBinance(symbol, interval, 1000);
    const spikes = detectDeltaSpikes(candles, threshold);

    return res.json({ spikes });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

// ----------- ROOT -----------
app.get("/", (req, res) => {
  res.send("Hybrid Delta Spike Detector Running");
});

// ----------- START SERVER -----------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log("Server running on port", PORT));
