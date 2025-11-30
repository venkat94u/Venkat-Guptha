import express from "express";
import axios from "axios";
import cors from "cors";

const app = express();
app.use(cors());

const PORT = process.env.PORT || 3000;

/* -----------------------------
   FETCH 3 MONTHS BINANCE KLINES
--------------------------------*/
async function fetch3MonthsKlines(symbol, interval = "1m") {
  let all = [];
  let limit = 1000;
  let endTime = Date.now();
  let threeMonthsMs = 90 * 24 * 60 * 60 * 1000;
  let startTime = endTime - threeMonthsMs;

  while (true) {
    const { data } = await axios.get(
      `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=${limit}`
    );

    if (data.length === 0) break;

    for (let c of data) {
      all.push({
        time: c[0],
        open: Number(c[1]),
        high: Number(c[2]),
        low: Number(c[3]),
        close: Number(c[4]),
      });
    }

    endTime = data[0][0];
    if (endTime <= startTime) break;
    if (all.length > 160000) break;
  }

  return all.reverse();
}

/* ---------------------------
   CONFLUENCE SCORE ENGINE
----------------------------*/
function scoreZone(zone, allZones, currentPrice) {
  let score = 0;

  // 1. Delta Strength (0–30)
  let deltaScore = Math.min(1, zone.delta / 100) * 30;

  // 2. Cluster Density (0–25)
  let nearby = allZones.filter(
    z => Math.abs(z.price - zone.price) < 40 && z !== zone
  ).length;
  let clusterScore = Math.min(1, nearby / 5) * 25;

  // 3. Recency (0–20)
  let ageDays = (Date.now() - zone.time) / (1000 * 60 * 60 * 24);
  let recencyScore = Math.max(0, 1 - ageDays / 90) * 20;

  // 4. Reaction Score (0–25)
  // If price bounced here before
  let reactionScore = (nearby > 2 ? 25 : nearby > 0 ? 15 : 5);

  score = deltaScore + clusterScore + recencyScore + reactionScore;

  return Math.round(score);
}

/* ---------------------------
   DELTA SPIKE EXTRACTION
----------------------------*/
function extractStrongDeltaZones(candles, currentPrice) {
  let spikes = [];

  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i - 1];
    const cur = candles[i];
    const delta = Math.abs(cur.close - prev.close);

    if (delta < 5) continue;

    spikes.push({
      price: cur.close,
      delta,
      time: cur.time,
      distance: Math.abs(cur.close - currentPrice),
    });
  }

  spikes.sort((a, b) => b.delta - a.delta);

  // Keep top 4% major moves
  const strong = spikes.slice(0, Math.floor(spikes.length * 0.04));

  // Remove noise (<50 points cluster)
  const clean = [];
  for (let z of strong) {
    if (!clean.some(c => Math.abs(c.price - z.price) < 50)) {
      clean.push(z);
    }
  }

  // Add scoring
  for (let z of clean) {
    z.score = scoreZone(z, clean, currentPrice);
  }

  // Split above/below
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

/* ---------------------------
   API ROUTE
----------------------------*/
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
      below: zones.below,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------------------------*/
app.listen(PORT, () =>
  console.log("Delta Spike + Scoring + Confluence Server Running")
);
