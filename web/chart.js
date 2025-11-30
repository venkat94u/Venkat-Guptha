async function loadData() {
  let res = await fetch("/api/zones?symbol=BTCUSDT");
  let data = await res.json();

  document.getElementById("zones").textContent =
    JSON.stringify(data, null, 2);

  buildChart(data);
}

function buildChart(data) {
  const chart = LightweightCharts.createChart(document.getElementById("chart"), {
    layout: { background: { color: '#0b0f16' }, textColor: 'white' },
    grid: { vertLines: { color: '#222' }, horzLines: { color: '#222' } },
  });

  const priceLine = chart.addLineSeries({
    color: "white",
    lineWidth: 2,
  });

  priceLine.setData([{ time: Math.floor(Date.now()/1000), value: data.currentPrice }]);

  // Add zones
  function addZone(z, isAbove) {
    let color =
      z.score > 80 ? (isAbove ? "#ff4d4d" : "#00cc66") :
      z.score > 60 ? "#ffa64d" :
      "#999";

    priceLine.createPriceLine({
      price: z.price,
      color,
      lineWidth: z.score > 70 ? 3 : 1,
      title: `Δ ${z.delta} — Score ${z.score}`,
    });
  }

  data.above.forEach(z => addZone(z, true));
  data.below.forEach(z => addZone(z, false));
}
