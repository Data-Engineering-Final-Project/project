const POLL_MS = 5000;
const TICKERS = [
  "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOG", "AMD", "NFLX", "JPM",
  "BAC", "XOM", "UNH", "JNJ", "COST", "WMT", "DIS", "PEP", "KO", "INTC",
  "CRM", "ORCL", "CSCO", "V", "MA", "PFE", "MRK", "NKE", "ADBE", "AVGO",
];

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// Date filter: applies to the heatmap/spikes/timeline panels (anything
// aggregating historical anomalies). Not applied to the live ticker strip
// or the predictor -- "latest state" is what those mean regardless of
// which period you're inspecting elsewhere on the page.
let activeFilter = { start: null, end: null };

function dateParams() {
  const parts = [];
  if (activeFilter.start) parts.push(`start_date=${activeFilter.start}`);
  if (activeFilter.end) parts.push(`end_date=${activeFilter.end}`);
  return parts.length ? "&" + parts.join("&") : "";
}

function initFilterBar() {
  document.getElementById("filter-apply").addEventListener("click", () => {
    const start = document.getElementById("filter-start").value;
    const end = document.getElementById("filter-end").value;
    if (!start && !end) return;
    activeFilter = { start: start || null, end: end || null };
    document.getElementById("filter-status").textContent =
      `Showing ${start || "the beginning"} to ${end || "now"}`;
    updateHeatmap(); updateSpikes(); updateTimeline();
  });

  document.getElementById("filter-reset").addEventListener("click", () => {
    activeFilter = { start: null, end: null };
    document.getElementById("filter-start").value = "";
    document.getElementById("filter-end").value = "";
    document.getElementById("filter-status").textContent = "Showing all-time data";
    updateHeatmap(); updateSpikes(); updateTimeline();
  });
}

function fmt(n, decimals = 2) {
  return typeof n === "number" ? n.toFixed(decimals) : "—";
}

function timeAgo(iso) {
  const seconds = Math.max(0, (Date.now() - new Date(iso + "Z")) / 1000);
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

// ---- Ticker strip (live feed) ----
async function updateTickerStrip() {
  const el = document.getElementById("ticker-strip");
  try {
    const rows = await fetchJSON("/api/live-feed?limit=15");
    el.innerHTML = rows.map(r => `
      <span class="ticker-item"><b>${r.ticker}</b> $${fmt(r.price)} · vol ${r.volume.toLocaleString()} · ${timeAgo(r.event_time)}</span>
    `).join("");
  } catch (e) {
    el.innerHTML = `<span class="error">live feed unavailable</span>`;
  }
}

// ---- Sector heatmap ----
async function updateHeatmap() {
  const el = document.getElementById("heatmap");
  try {
    const rows = await fetchJSON("/api/sector-heatmap?_=1" + dateParams());
    if (!rows.length) { el.innerHTML = `<div class="empty">No anomalies yet</div>`; return; }
    const ratios = rows.map(r => r.avg_volume_ratio);
    const min = Math.min(...ratios), max = Math.max(...ratios);
    el.innerHTML = rows.map(r => {
      const t = max > min ? (r.avg_volume_ratio - min) / (max - min) : 0.5;
      // interpolate amber (low) -> red (high)
      const hue = 40 - t * 40; // 40 (amber) down to 0 (red)
      return `
        <div class="heat-cell" style="background: hsl(${hue}, 85%, 55%)">
          <div class="sector-name">${r.sector}</div>
          <div class="sector-ratio">${fmt(r.avg_volume_ratio)}x</div>
          <div class="sector-count">${r.anomaly_count} anomalies</div>
        </div>`;
    }).join("");
  } catch (e) {
    el.innerHTML = `<div class="error">heatmap unavailable</div>`;
  }
}

// ---- Top spikes table ----
async function updateSpikes() {
  const tbody = document.querySelector("#spikes-table tbody");
  try {
    const rows = await fetchJSON("/api/top-spikes?limit=10" + dateParams());
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td><b>${r.ticker}</b></td>
        <td>${r.sector}</td>
        <td>${fmt(r.volume_ratio)}x</td>
        <td>${fmt(r.rsi_value, 1)}</td>
        <td><span class="badge badge-${r.target_label}">${r.target_label ? "±5% hit" : "no move"}</span></td>
      </tr>`).join("");
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="5" class="error">spikes table unavailable</td></tr>`;
  }
}

// ---- Late arrivals timeline ----
async function updateTimeline() {
  const el = document.getElementById("timeline");
  try {
    const rows = await fetchJSON("/api/late-arrivals?limit=8" + dateParams());
    if (!rows.length) { el.innerHTML = `<div class="empty">No ratings yet</div>`; return; }
    el.innerHTML = rows.map(r => `
      <div class="timeline-item">
        <div class="timeline-delay">${fmt(r.delay_hours, 1)}h late</div>
        <div class="timeline-ticker">${r.ticker}</div>
        <div class="timeline-detail">${r.rating_text} (score ${r.sentiment_score}) — event at ${r.event_time.replace("T", " ").slice(0, 16)}</div>
      </div>`).join("");
  } catch (e) {
    el.innerHTML = `<div class="error">timeline unavailable</div>`;
  }
}

// ---- ML predictor ----
function initTickerSelect() {
  const select = document.getElementById("ticker-select");
  select.innerHTML = TICKERS.map(t => `<option value="${t}">${t}</option>`).join("");
  select.addEventListener("change", () => updatePredictor(select.value));
}

async function updatePredictor(ticker) {
  const el = document.getElementById("predictor-result");
  el.innerHTML = "Loading...";
  try {
    const r = await fetchJSON(`/api/predict/${ticker}`);
    const pct = Math.round(r.probability * 100);
    el.innerHTML = `
      <div class="probability" style="color: ${r.predicted_label ? "var(--green)" : "var(--muted)"}">${pct}%</div>
      <div class="label">probability of a ≥5% move within 5 trading days</div>
      <div class="feature-row"><span>Volume ratio</span><span>${fmt(r.features.volume_ratio)}x</span></div>
      <div class="feature-row"><span>RSI</span><span>${fmt(r.features.rsi_value, 1)}</span></div>
      <div class="feature-row"><span>Volatility index</span><span>${fmt(r.features.volatility_index)}</span></div>
      <div class="feature-row"><span>Sentiment score</span><span>${r.features.sentiment_score ?? "neutral (no coverage yet)"}</span></div>
    `;
  } catch (e) {
    el.innerHTML = `<div class="error">No anomaly data for ${ticker} yet</div>`;
  }
}

// ---- boot ----
function refreshAll() {
  updateTickerStrip();
  updateHeatmap();
  updateSpikes();
  updateTimeline();
}

initTickerSelect();
initFilterBar();
updatePredictor(TICKERS[0]);
refreshAll();
setInterval(refreshAll, POLL_MS);
setInterval(() => updatePredictor(document.getElementById("ticker-select").value), POLL_MS * 3);
