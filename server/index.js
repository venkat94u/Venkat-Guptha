// server/index.js
try{
const r = await axios.get(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${symbol}`, { timeout: 8000 });
return res.json({ price: Number(r.data.price) });
}catch(err){
return res.status(500).json({ error: String(err.message || err) });
}
});


// compute spikes and return top zones
app.get('/api/top-zones', (req,res)=>{
try{
const symbol = (req.query.symbol || '').toUpperCase();
const periodMs = Number(req.query.periodMs) || (365*24*3600*1000); // used only if we had time filter (not in this simple buckets table)
const bucketSize = Number(req.query.bucket) || 1;
const limit = Number(req.query.limit) || 300;
if(!symbol) return res.status(400).json({ error:'symbol required' });


const rows = db.prepare('SELECT bucket_price, buy_volume, sell_volume, last_ts FROM buckets WHERE symbol = ? ORDER BY bucket_price').all(symbol);
if(!rows || !rows.length) return res.json({ zones: [] });


// compute delta for each bucket
const deltas = rows.map(r=>({ price: Number(r.bucket_price), buy:r.buy_volume||0, sell:r.sell_volume||0, delta: (r.buy_volume||0) - (r.sell_volume||0), lastTs: r.last_ts || 0 }));


// compute stats: mean and stddev on absolute delta magnitude
const vals = deltas.map(d=>Math.abs(d.delta));
const mean = vals.reduce((a,b)=>a+b,0)/vals.length;
const std = Math.sqrt(vals.reduce((a,b)=>a + Math.pow(b-mean,2),0)/vals.length);


// spike threshold: mean + 3*std OR relative threshold (e.g. 0.01 * sum volume) - keep both
const threshold = Math.max(mean + 3*std, mean * 4);


const spikes = deltas.filter(d=> Math.abs(d.delta) >= threshold ).sort((a,b)=>Math.abs(b.delta) - Math.abs(a.delta)).slice(0, limit);


// If spikes are empty, provide a fallback: top N by abs delta (still useful)
const zones = (spikes.length ? spikes : deltas.sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0, limit)).map(z=>({ price: z.price, delta: z.delta, volume: z.buy + z.sell, lastTs: z.lastTs }));


res.json({ zones });
}catch(e){
res.status(500).json({ error: String(e) });
}
});


// simple symbols endpoint: list distinct symbols present in DB
app.get('/api/symbols', (req,res)=>{
const rows = db.prepare('SELECT symbol, MAX(last_ts) as lastSeen FROM buckets GROUP BY symbol ORDER BY symbol').all();
const symbols = rows.map(r=>({ symbol: r.symbol, lastSeen: r.lastSeen }));
res.json({ symbols });
});


// static web
app.use('/', express.static(path.join(__dirname, '..', 'web')));


app.listen(PORT, ()=> console.log('Server listening on', PORT));
