import express from "express";
import axios from "axios";
import cors from "cors";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.static("web"));   // serve frontend

// ---------------------------------------------------------
// Fetch 3 months of 1m candles with timeout + safety
// ---------------------------------------------------------
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

    let data = [];

    try {
      const res = await axios.get(url, { timeout: 5000 });
      data = res.data;
    } catch (err) {
      console.log("❌ Binance timeout or rate limit → stopping early");
      break;
    }

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

    endTime = data[0][0]; // move backward
    if (endTime <= startTime) break;
    if (all.length > 150000) break; // safety limit
  }

  return all.reverse();
}

// ---------------------------------------------------------
// Extract strongest Delta Spike S/R Zones
// ---------------------------------------------------------
function extractStrongDeltaZones(candles, currentPrice) {
  let spikes = [];

  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i - 1];
    const cur = candles[i];

    const delta = Math.abs(cur.close - prev.close);
    if (delta < 5) continue;  // minimum spike threshold

    spikes.push({
      price: cur.close,
      delta,
      time: cur.time,
      distance: Math.abs(cur.close - currentPrice)
    });
  }

  if (spikes.length === 0) return { above: [], below: [] };

  // strongest 2%
  const sorted = spikes.sort((a, b) => b.delta - a.delta);
  const top = sorted.slice(0, Math.floor(sorted.length * 0.02));

  // remove levels < 50 points apart
  const clean = [];
  for (let z of top) {
    if (!clean.some(c => Math.abs(c.price - z.price) < 50)) {
      clean.push(z);
    }
  }

  // closest ABOVE
  const above = clean
    .filter(z => z.price > currentPrice)
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 30);

  // closest BELOW
  const below = clean
    .filter(z => z.price < currentPrice)
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 30);

  return { above, below };
}

// ---------------------------------------------------------
// Routes
// ---------------------------------------------------------
app.get("/", (req, res) => {
  res.send("Hybrid Delta Spike Detector Running");
});

// S/R zones endpoint
app.get("/api/zones", async (req, res) => {
  try {
    const symbol = (req.query.symbol || "BTCUSDT").toUpperCase();

    // current price
    const priceRes = await axios.get(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`,
      { timeout: 4000 }
    );
    const currentPrice = Number(priceRes.data.price);

    // historical candles
    const candles = await fetch3MonthsKlines(symbol);

    const zones = extractStrongDeltaZones(candles, currentPrice);

    res.json({
      symbol,
      currentPrice,
      above: zones.above,
      below: zones.below
    });

  } catch (err) {
    console.log("❌ Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------
app.listen(PORT, () => console.log("Server running on port " + PORT));
