import express from "express";
import axios from "axios";
import cors from "cors";

const app = express();
app.use(cors());

const PORT = process.env.PORT || 3000;

// ----------------------------
// Fetch 3 months klines
// ----------------------------
async function fetch3MonthsKlines(symbol, interval = "1m") {
  let all = [];
  let limit = 1000;
  let endTime = Date.now();
  let threeMonthsMs = 90 * 24 * 60 * 60 * 1000;
  let startTime = endTime - threeMonthsMs;

  while (true) {
    const url =
      `https://api.binance.com/api/v3/klines` +
      `?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=${limit}`;

    const { data } = await axios.get(url);
    if (data.length === 0) break;

    for (let c of data) {
      all.push({
        time: c[0],
        open: Number(c[1]),
        high: Number(c[2]),
        low: Number(c[3]),
        close: Number(c[4])
      });
    }

    endTime = data[0][0]; // move backwards
    if (endTime <= startTime) break;

    if (all.length > 150000) break; // safety cap
  }

  return all.reverse();
}

// ----------------------------
// Extract Strong DELTA SPIKES
// ----------------------------
function extractStrongDeltaZones(candles, currentPrice) {
  let spikes = [];

  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i - 1];
    const cur = candles[i];

    const delta = Math.abs(cur.close - prev.close);

    if (delta < 5) continue; // minimum threshold

    spikes.push({
      price: cur.close,
      delta,
      time: cur.time,
      distance: Math.abs(cur.close - currentPrice)
    });
  }

  if (spikes.length === 0) return { above: [], below: [] };

  // keep only strongest 2%
  const sorted = spikes.sort((a, b) => b.delta - a.delta);
  const top = sorted.slice(0, Math.floor(sorted.length * 0.02));

  // remove levels too close (< 50 points)
  const clean = [];
  for (let z of top) {
    if (!clean.some(c => Math.abs(c.price - z.price) < 50)) {
      clean.push(z);
    }
  }

  // split above/below
  const above = clean
    .filter(z => z.price > currentPrice)
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 30);

  const below = clean
    .filter(z => z.price < currentPrice)
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 30);

  return { above, below };
}

// ----------------------------
// API endpoint
// ----------------------------
app.get("/", (req, res) => {
  res.send("Hybrid Delta Spike Detector Running");
});

app.get("/api/zones", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();

    const priceRes = await axios.get(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`
    );

    const currentPrice = Number(priceRes.data.price);

    const candles = await fetch3MonthsKlines(symbol);

    const zones = extractStrongDeltaZones(candles, currentPrice);

    res.json({
      symbol,
      currentPrice,
      above: zones.above,
      below: zones.below
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => console.log("Server running on port " + PORT));
