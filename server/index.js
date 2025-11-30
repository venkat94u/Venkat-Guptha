import express from "express";
import cors from "cors";
import axios from "axios";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());

// Serve UI
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "../web/index.html"));
});

// --------------------------
// Get current price (Binance)
// --------------------------
app.get("/api/price", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();
    const r = await axios.get(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`
    );
    res.json({ price: Number(r.data.price) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------
// Fetch Binance Klines
// --------------------------
async function fetchKlines(symbol, interval, limit = 500) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const r = await axios.get(url);
  return r.data.map(c => ({
    time: c[0],
    open: Number(c[1]),
    close: Number(c[4])
  }));
}

// --------------------------
// Delta Spike Detection
// --------------------------
function detectSpikes(candles, threshold) {
  let result = [];

  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i - 1];
    const cur = candles[i];

    const delta = Math.abs(cur.close - prev.close);

    if (delta >= threshold) {
      result.push({
        price: cur.close,
        delta,
        time: cur.time
      });
    }
  }

  return result;
}

// --------------------------
// API: zones
// --------------------------
app.get("/api/zones", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();
    const interval = req.query.interval || "1h";
    const threshold = Number(req.query.threshold || 1);

    const candles = await fetchKlines(symbol, interval, 500);
    const zones = detectSpikes(candles, threshold);

    res.json({ zones });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log("Server running on PORT", PORT));
