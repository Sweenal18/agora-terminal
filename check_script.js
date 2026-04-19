
const API = window.location.hostname === 'localhost' ? 'http://localhost:8000' : 'https://api.agora-terminal.com';
let currentSymbol = null;
let currentInfo = null;
let currentScreenData = null;
let tvChart = null;

let researchSearchTimeout;
let researchResults = [];
let researchActiveIdx = -1;

function initResearchSearch() {
  const input = document.getElementById('symbolInput');
  const dropdown = document.getElementById('researchDropdown');

  input.addEventListener('input', () => {
    const q = input.value.trim();
    researchActiveIdx = -1;
    const browse = document.getElementById("chipBrowse");
    if (browse && browse.classList.contains("visible")) {
      renderBrowseList(q, false);
      return;
    }
    if (q.length < 1) { dropdown.classList.remove('visible'); return; }
    clearTimeout(researchSearchTimeout);
    researchSearchTimeout = setTimeout(async () => {
      try {
        const resp = await fetch(API + '/api/screener/search?q=' + encodeURIComponent(q) + '&limit=7');
        const data = await resp.json();
        researchResults = data.results || [];
        if (!researchResults.length) {
          dropdown.innerHTML = '<div class="search-item"><span class="si-name">No results for "' + q + '"</span></div>';
        } else {
          dropdown.innerHTML = researchResults.map((r, i) => {
            const chg = r.change_pct != null ? (r.change_pct >= 0 ? '+' : '') + r.change_pct.toFixed(2) + '%' : '';
            const chgColor = r.change_pct >= 0 ? 'var(--green)' : 'var(--red)';
            const price = r.price != null ? '$' + r.price.toFixed(2) : '';
            const pinned = pinnedSymbols.includes(r.symbol);
            return '<div class="search-item" data-idx="' + i + '">' +
              '<span class="si-sym">' + r.symbol + '</span>' +
              '<span class="si-name">' + (r.company_name || '') + '</span>' +
              '<span class="si-price">' + price + '</span>' +
              (chg ? '<span class="si-chg" style="color:' + chgColor + '">' + chg + '</span>' : '') +
              '<span class="si-pin" data-sym="' + r.symbol + '" style="margin-left:auto;padding:0 6px;cursor:pointer;font-size:15px;color:' + (pinned ? 'var(--teal)' : 'var(--text-dim)') + '">' + (pinned ? '&#9733;' : '&#9734;') + '</span>' +
              '</div>';
          }).join('');
          dropdown.querySelectorAll('.search-item').forEach((el, i) => {
            el.addEventListener('click', () => {
              dropdown.classList.remove('visible');
              currentSymbol = researchResults[i].symbol;
              document.getElementById('symbolInput').value = '';
              loadSymbol();
            });
          });
        }
        dropdown.classList.add('visible');
      } catch(e) { console.error(e); }
    }, 200);
  });

  input.addEventListener('focus', () => {
    const browse = document.getElementById("chipBrowse");
    if (browse && browse.classList.contains("visible")) return;
    if (researchResults.length > 0) dropdown.classList.add('visible');
  });

  input.addEventListener('keydown', e => {
    const items = dropdown.querySelectorAll('.search-item');
    if (e.key === 'ArrowDown') { e.preventDefault(); researchActiveIdx = Math.min(researchActiveIdx + 1, items.length - 1); items.forEach((el, i) => el.classList.toggle('active', i === researchActiveIdx)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); researchActiveIdx = Math.max(researchActiveIdx - 1, 0); items.forEach((el, i) => el.classList.toggle('active', i === researchActiveIdx)); }
    else if (e.key === 'Enter') {
      e.preventDefault();
      if (researchActiveIdx >= 0 && researchResults[researchActiveIdx]) {
        currentSymbol = researchResults[researchActiveIdx].symbol;
        dropdown.classList.remove('visible');
      }
      input.value = '';
      loadSymbol();
    }
    else if (e.key === 'Escape') { dropdown.classList.remove('visible'); }
  });

  document.addEventListener('mousedown', e => {
    if (!e.target.closest('.search-wrap')) {
      dropdown.classList.remove('visible');
    }
  });
}

function setSymbol(sym) {
  document.getElementById('symbolInput').value = '';
  currentSymbol = sym;
  loadSymbol();
}

const DEFAULT_PINS = ["AAPL","NVDA","MSFT","GOOGL","TSLA","BTC","ETH","^GSPC","^IXIC","GOLD","EURUSD"];
const PIN_KEY = "agora_pinned_chips";
const ALL_SYMBOLS_EXTRA = [
  {symbol:"^GSPC",name:"S&P 500",type:"index"},{symbol:"^IXIC",name:"NASDAQ",type:"index"},
  {symbol:"^DJI",name:"Dow Jones",type:"index"},{symbol:"^RUT",name:"Russell 2000",type:"index"},
  {symbol:"BTC",name:"Bitcoin",type:"crypto"},{symbol:"ETH",name:"Ethereum",type:"crypto"},
  {symbol:"SOL",name:"Solana",type:"crypto"},
  {symbol:"EURUSD",name:"EUR / USD",type:"forex"},{symbol:"GBPUSD",name:"GBP / USD",type:"forex"},
  {symbol:"USDJPY",name:"USD / JPY",type:"forex"},
  {symbol:"GOLD",name:"Gold",type:"commodity"},{symbol:"SILVER",name:"Silver",type:"commodity"},
  {symbol:"OIL",name:"Crude Oil",type:"commodity"}
];
let allSymbols = [];
let pinnedSymbols = JSON.parse(localStorage.getItem(PIN_KEY) || "null") || [...DEFAULT_PINS];

function getPinnedLabel(sym) {
  const found = allSymbols.find(s => s.symbol === sym) || ALL_SYMBOLS_EXTRA.find(s => s.symbol === sym);
  if (found) return found.name || found.symbol;
  const labels = {"^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"Dow Jones","^RUT":"Russell 2000","EURUSD":"EUR/USD","GBPUSD":"GBP/USD","USDJPY":"USD/JPY"};
  return labels[sym] || sym;
}

function savePins() { localStorage.setItem(PIN_KEY, JSON.stringify(pinnedSymbols)); }

function renderPinnedChips() {
  const container = document.getElementById("pinnedChips");
  if (container && !container._wheelSet) {
    container._wheelSet = true;
    container.addEventListener("wheel", e => { e.preventDefault(); container.scrollLeft += e.deltaY; }, {passive:false});
  }
  if (!container) return;
  container.innerHTML = "";
  pinnedSymbols.forEach(sym => {
    const chip = document.createElement("div");
    chip.className = "chip" + (sym === currentSymbol ? " active-chip" : "");
    chip.textContent = sym === "^GSPC" ? "S&P 500" : sym === "^IXIC" ? "NASDAQ" : sym === "^DJI" ? "Dow Jones" : sym === "^RUT" ? "Russell 2000" : sym === "EURUSD" ? "EUR/USD" : sym === "GBPUSD" ? "GBP/USD" : sym === "USDJPY" ? "USD/JPY" : sym;
    if (sym === currentSymbol) { chip.style.color="var(--teal)"; chip.style.borderColor="var(--teal-dim)"; chip.style.background="var(--teal-glow)"; }
    chip.onclick = () => setSymbol(sym);
    chip.oncontextmenu = (e) => { e.preventDefault(); pinnedSymbols = pinnedSymbols.filter(s => s !== sym); savePins(); renderPinnedChips(); };
    container.appendChild(chip);
  });
}

function updateActiveChip() { renderPinnedChips(); }

async function initBrowse() {
  try {
    const res = await fetch(API + "/api/chart/symbols");
    const data = await res.json();
    allSymbols = (data.symbols || []).filter(s => !ALL_SYMBOLS_EXTRA.find(e => e.symbol === s.symbol));
  } catch(e) { allSymbols = []; }
  renderPinnedChips();
  setupBrowseDropdown();
}

function setupBrowseDropdown() {
  const btn = document.getElementById("chipAddBtn");
  const browse = document.getElementById("chipBrowse");
  if (!btn || !browse) return;

  btn.onclick = (e) => {
    e.stopPropagation();
    if (browse.classList.contains("visible")) { browse.classList.remove("visible"); return; }
    renderBrowseList(document.getElementById("symbolInput").value.trim());
    browse.classList.add("visible");
  };

  document.addEventListener("click", e => {
    if (!e.target.closest("#chipBrowse") && !e.target.closest("#chipAddBtn") && !e.target.closest(".search-wrap")) browse.classList.remove("visible");
  });
}

function renderBrowseList(filter, keepScroll) {
  const browse = document.getElementById("chipBrowse");
  const scrollTop = keepScroll ? browse.scrollTop : 0;
  const q = filter.toLowerCase();
  const groups = [
    { label: "EQUITIES", items: allSymbols },
    { label: "INDICES", items: ALL_SYMBOLS_EXTRA.filter(s => s.type === "index") },
    { label: "CRYPTO", items: ALL_SYMBOLS_EXTRA.filter(s => s.type === "crypto") },
    { label: "FOREX", items: ALL_SYMBOLS_EXTRA.filter(s => s.type === "forex") },
    { label: "COMMODITIES", items: ALL_SYMBOLS_EXTRA.filter(s => s.type === "commodity") },
  ];
  let html = ``;
  groups.forEach(g => {
    const filtered = g.items.filter(s => !q || s.symbol.toLowerCase().includes(q) || (s.name||"").toLowerCase().includes(q));
    if (!filtered.length) return;
    html += `<div class="browse-group"><div class="browse-group-label">${g.label}</div>`;
    filtered.forEach(s => {
      const pinned = pinnedSymbols.includes(s.symbol);
      html += `<div class="browse-item${pinned?" pinned":""}" data-sym="${s.symbol}">
        <span class="browse-item-sym">${s.symbol}</span>
        <span class="browse-item-name">${s.name||""}</span>
        <span class="browse-item-pin" data-pin="${s.symbol}">${pinned?"★":"☆"}</span>
      </div>`;
    });
    html += "</div>";
  });
  browse.innerHTML = html;

  browse.querySelectorAll(".browse-item").forEach(el => {
    el.addEventListener("click", (e) => {
      if (e.target.closest(".browse-item-pin")) return;
      setSymbol(el.dataset.sym);
      browse.classList.remove("visible");
    });
  });

  browse.querySelectorAll(".browse-item-pin").forEach(el => {
    el.addEventListener("click", (e) => {
      e.stopPropagation();
      const sym = el.dataset.pin;
      if (pinnedSymbols.includes(sym)) {
        pinnedSymbols = pinnedSymbols.filter(s => s !== sym);
      } else {
        pinnedSymbols.push(sym);
      }
      savePins();
      renderPinnedChips();
      renderBrowseList("", true);
    });
  });

  browse.scrollTop = scrollTop;
}

async function loadSymbol() {
  const inputVal = document.getElementById('symbolInput').value.trim();
  const raw = inputVal ? inputVal.toUpperCase().split(' ')[0] : currentSymbol;
  if (!raw) return;
  currentSymbol = raw;
  updateActiveChip();
  document.getElementById('chatCtxSymbol').textContent = raw;
  document.getElementById('chatCtxName').textContent = '';
  document.getElementById('sendBtn').disabled = false;
  document.getElementById('emptyLeft').style.display = 'none';
  document.getElementById('companyData').style.display = 'block';
  document.getElementById('lpSymbol').textContent = raw;
  document.getElementById('lpName').textContent = 'Loading...';
  document.getElementById('headerBar').style.display = 'flex';
  resetChatChips();

  const [infoRes, screenRes] = await Promise.allSettled([
    fetch(`${API}/api/chart/info/${raw}`).then(r => r.json()),
    fetch(`${API}/api/screener/screen?symbol=${raw}&limit=1`).then(r => r.json()),
  ]);

  const info = infoRes.status === 'fulfilled' ? infoRes.value : null;
  const sd = screenRes.status === 'fulfilled'
    ? ((screenRes.value.data || [])[0] || null) : null;

  currentInfo = info;
  currentScreenData = sd;

  if (info && !info.error) {
    populateLeft(info, sd);
    populateMetricsStrip(sd);
    populateFinancials(info, sd);
    updateChatContext(info);
  } else {
    document.getElementById('lpName').textContent = 'Symbol not found in database';
  }
  if (info && info.sector) loadPeerStrip(info.sector, raw);
  await loadChart(raw);
}

const INDEX_NAMES = {
  '^GSPC': 'S&P 500', '^IXIC': 'NASDAQ Composite', '^DJI': 'Dow Jones',
  '^RUT': 'Russell 2000', '^VIX': 'VIX', '^N225': 'Nikkei 225',
  'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'SOL': 'Solana',
  'EURUSD': 'EUR / USD', 'GBPUSD': 'GBP / USD', 'USDJPY': 'USD / JPY',
  'GOLD': 'Gold Futures', 'OIL': 'Crude Oil', 'SILVER': 'Silver'
};

function clearLeft() {
  ['sClose','sChange1d','sBeta','sMktCap','sLastDate',
   'sRoe','sPb','sEv','sDiv','sRoic','sCr',
   'tMa20','tMa50','tMa200','t1w','t1m'].forEach(id => {
    const e = document.getElementById(id);
    if (e) { e.textContent = '—'; e.className = 'stat-value'; }
  });
  document.getElementById('w52fill').style.width = '0%';
  document.getElementById('w52marker').style.left = '0%';
  document.getElementById('w52low').textContent = '—';
  document.getElementById('w52high').textContent = '—';
  document.getElementById('w52cur').textContent = '—';
  document.getElementById('lpPrice').textContent = '—';
  document.getElementById('lpChange').textContent = '—';
  document.getElementById('lpChange').className = 'price-change';
  document.getElementById('lpMeta').innerHTML = '';
}

function isEquity(info) {
  return info && info.sector && info.sector !== 'Crypto' && info.asset_class === 'equity';
}

function populateLeft(info, sd) {
  clearLeft();
  const displayName = INDEX_NAMES[info.symbol] || info.name || info.symbol;
  document.getElementById('lpSymbol').textContent = INDEX_NAMES[info.symbol] ? INDEX_NAMES[info.symbol] : info.symbol;
  document.getElementById('lpName').textContent = info.name && info.name !== info.symbol ? info.name : '';
  const meta = document.getElementById('lpMeta');
  meta.innerHTML = '';
  [info.sector, info.industry, info.market_cap_bucket, info.asset_class].filter(Boolean)
    .filter(v => v !== 'Unknown' && v !== 'unknown').forEach(v => {
      const t = document.createElement('div');
      t.className = 'meta-tag';
      t.textContent = v.replace(/_/g,' ').toUpperCase();
      meta.appendChild(t);
    });
  const price = info.last_close, chg = info.daily_return_pct;
  document.getElementById('lpPrice').textContent = price ? '$' + price.toFixed(2) : '—';
  const chgEl = document.getElementById('lpChange');
  if (chg !== null && chg !== undefined) {
    chgEl.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '% today';
    chgEl.className = 'price-change ' + (chg >= 0 ? 'pos' : 'neg');
  } else {
    chgEl.textContent = 'Chart data via Yahoo Finance';
    chgEl.className = 'price-change neu';
  }
  document.getElementById('headerName').textContent = INDEX_NAMES[info.symbol] || info.name || info.symbol || '';
  document.getElementById('headerPrice').textContent = price ? (price > 1000 ? price.toFixed(2) : '$' + price.toFixed(2)) : '';
  if (chg !== null && chg !== undefined) {
    const hc = document.getElementById('headerChange');
    hc.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';
    hc.className = 'change-bar ' + (chg >= 0 ? 'pos' : 'neg');
  }
  // Show/hide sections based on asset type
  const equity = isEquity(info);
  document.querySelectorAll('.stats-section').forEach((s, i) => {
    if (i === 0) s.style.display = equity ? '' : 'none'; // Price & Market
    if (i === 1) s.style.display = equity ? '' : 'none'; // Fundamentals
    if (i === 2) s.style.display = equity ? '' : 'none'; // Technical
  });
  document.querySelector('.week52-bar-wrap').style.display = equity ? '' : 'none';
  const lo = info.week_52_low, hi = info.week_52_high;
  if (lo && hi && price) {
    const pct = Math.max(0, Math.min(100, ((price - lo) / (hi - lo)) * 100));
    document.getElementById('w52fill').style.width = pct + '%';
    document.getElementById('w52marker').style.left = pct + '%';
    document.getElementById('w52low').textContent = '$' + lo.toFixed(2);
    document.getElementById('w52high').textContent = '$' + hi.toFixed(2);
    document.getElementById('w52cur').textContent = '$' + price.toFixed(2);
  }
  setText('sClose', price ? '$' + price.toFixed(2) : '—');
  setColored('sChange1d', chg, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
  setText('sBeta', info.beta ? info.beta.toFixed(3) : '—');
  setText('sMktCap', fmtCap(info.market_cap));
  setText('sLastDate', info.last_trade_date || '—');
  setColored('sRoe', info.roe ? info.roe * 100 : null, v => v.toFixed(1) + '%');
  setText('sPb', info.price_to_book ? info.price_to_book.toFixed(1) : '—');
  setText('sDiv', info.dividend_yield ? (info.dividend_yield * 100).toFixed(2) + '%' : '—');
  if (sd) {
    setText('sEv', sd.ev_to_ebitda ? sd.ev_to_ebitda.toFixed(1) : '—');
    setColored('sRoic', sd.roic ? sd.roic * 100 : null, v => v.toFixed(1) + '%');
    setText('sCr', sd.current_ratio ? sd.current_ratio.toFixed(2) : '—');
    setColored('tMa20', sd.ma20 && sd.price ? ((sd.price - sd.ma20) / sd.ma20 * 100) : null, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
    setColored('tMa50', sd.pct_from_ma50, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
    setColored('tMa200', sd.ma200 && sd.price ? ((sd.price - sd.ma200) / sd.ma200 * 100) : null, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
    setColored('t1w', sd.change_1w_pct, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
    setColored('t1m', sd.change_1m_pct, v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%');
  }
}

function populateMetricsStrip(sd) {
  if (!sd) return;
  setText('mOpen', sd.price ? '$' + sd.price.toFixed(2) : '—');
  setText('mHigh', sd.week_52_high ? '$' + sd.week_52_high.toFixed(2) : '—');
  setText('mLow', sd.week_52_low ? '$' + sd.week_52_low.toFixed(2) : '—');
  setText('mClose', sd.price ? '$' + sd.price.toFixed(2) : '—');
  setText('mVol', sd.volume ? fmtVolume(sd.volume) : '—');
  const el = document.getElementById('mMa50');
  if (sd.pct_from_ma50 !== null && sd.pct_from_ma50 !== undefined) {
    el.textContent = (sd.pct_from_ma50 >= 0 ? '+' : '') + sd.pct_from_ma50.toFixed(2) + '%';
    el.className = 'metric-cell-value ' + (sd.pct_from_ma50 >= 0 ? 'pos' : 'neg');
  }
}

function populateFinancials(info, sd) {
  const fc = document.getElementById('financialsContent');
  const p = (v, d=2) => v !== null && v !== undefined ? v.toFixed(d) : '—';
  const pct = (v, d=1) => v !== null && v !== undefined ? (v * 100).toFixed(d) + '%' : '—';
  fc.innerHTML = `
    <div class="fin-section">
      <div class="fin-section-title">Company Overview</div>
      <div class="fin-grid">
        <div class="fin-cell"><div class="fin-cell-label">Market Cap</div><div class="fin-cell-value">${fmtCap(info.market_cap)}</div><div class="fin-cell-sub">${info.market_cap_bucket ? info.market_cap_bucket.replace(/_/g,' ').toUpperCase() : ''}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">Sector</div><div class="fin-cell-value" style="font-size:15px;">${info.sector || '—'}</div><div class="fin-cell-sub">${info.industry || ''}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">Beta</div><div class="fin-cell-value">${p(info.beta, 3)}</div><div class="fin-cell-sub">Market sensitivity</div></div>
      </div>
    </div>
    <div class="fin-section">
      <div class="fin-section-title">Valuation Multiples</div>
      <div class="fin-grid">
        <div class="fin-cell"><div class="fin-cell-label">Price / Book</div><div class="fin-cell-value">${p(info.price_to_book)}x</div></div>
        <div class="fin-cell"><div class="fin-cell-label">Price / Sales</div><div class="fin-cell-value">${sd ? p(sd.price_to_sales) + 'x' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">EV / EBITDA</div><div class="fin-cell-value">${sd ? p(sd.ev_to_ebitda) + 'x' : '—'}</div></div>
      </div>
    </div>
    <div class="fin-section">
      <div class="fin-section-title">Profitability</div>
      <div class="fin-grid">
        <div class="fin-cell"><div class="fin-cell-label">ROE</div><div class="fin-cell-value">${pct(info.roe)}</div><div class="fin-cell-sub">Return on Equity</div></div>
        <div class="fin-cell"><div class="fin-cell-label">ROIC</div><div class="fin-cell-value">${sd ? pct(sd.roic) : '—'}</div><div class="fin-cell-sub">Return on Invested Capital</div></div>
        <div class="fin-cell"><div class="fin-cell-label">Dividend Yield</div><div class="fin-cell-value">${pct(info.dividend_yield, 2)}</div></div>
      </div>
    </div>
    <div class="fin-section">
      <div class="fin-section-title">Financial Health</div>
      <div class="fin-grid">
        <div class="fin-cell"><div class="fin-cell-label">Current Ratio</div><div class="fin-cell-value">${sd ? p(sd.current_ratio) : '—'}</div><div class="fin-cell-sub">&gt;1 = liquid</div></div>
        <div class="fin-cell"><div class="fin-cell-label">Volume Ratio</div><div class="fin-cell-value">${sd && sd.volume_ratio ? p(sd.volume_ratio) + 'x' : '—'}</div><div class="fin-cell-sub">vs 20D avg</div></div>
        <div class="fin-cell"><div class="fin-cell-label">52W High</div><div class="fin-cell-value">$${p(info.week_52_high)}</div><div class="fin-cell-sub">Low: $${p(info.week_52_low)}</div></div>
      </div>
    </div>
    <div class="fin-section">
      <div class="fin-section-title">Technical Position</div>
      <div class="fin-grid">
        <div class="fin-cell"><div class="fin-cell-label">vs MA20</div><div class="fin-cell-value">${sd && sd.ma20 && sd.price ? (sd.price >= sd.ma20 ? '+' : '') + ((sd.price - sd.ma20)/sd.ma20*100).toFixed(2) + '%' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">vs MA50</div><div class="fin-cell-value">${sd && sd.pct_from_ma50 !== null ? (sd.pct_from_ma50 >= 0 ? '+' : '') + sd.pct_from_ma50.toFixed(2) + '%' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">vs MA200</div><div class="fin-cell-value">${sd && sd.ma200 && sd.price ? (sd.price >= sd.ma200 ? '+' : '') + ((sd.price - sd.ma200)/sd.ma200*100).toFixed(2) + '%' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">1W Return</div><div class="fin-cell-value">${sd && sd.change_1w_pct !== null ? (sd.change_1w_pct >= 0 ? '+' : '') + sd.change_1w_pct.toFixed(2) + '%' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">1M Return</div><div class="fin-cell-value">${sd && sd.change_1m_pct !== null ? (sd.change_1m_pct >= 0 ? '+' : '') + sd.change_1m_pct.toFixed(2) + '%' : '—'}</div></div>
        <div class="fin-cell"><div class="fin-cell-label">From 52W Low</div><div class="fin-cell-value">${sd && sd.pct_from_52w_low !== null ? '+' + sd.pct_from_52w_low.toFixed(2) + '%' : '—'}</div></div>
      </div>
    </div>`;
}

let currentTimeframe = '1Y';

function setTimeframe(tf) {
  currentTimeframe = tf;
  document.querySelectorAll('.tf-btn').forEach(function(b) {
    b.classList.toggle('active', b.textContent === tf);
  });
  if (currentSymbol) loadChart(currentSymbol);
}

async function loadChart(symbol) {
  document.getElementById('overviewEmpty').style.display = 'none';
  const wrap = document.getElementById('overviewChart');
  wrap.style.display = 'flex';
  const chartEl = document.getElementById('tv-chart');
  if (tvChart) { tvChart.remove(); tvChart = null; }
  tvChart = LightweightCharts.createChart(chartEl, {
    width: chartEl.offsetWidth, height: chartEl.offsetHeight || 400,
    layout: { background: { color: 'transparent' }, textColor: '#7A8BAD' },
    grid: { vertLines: { color: '#1A2540' }, horzLines: { color: '#1A2540' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#1A2540' },
    timeScale: { borderColor: '#1A2540', timeVisible: true },
  });
  const cs = tvChart.addCandlestickSeries({
    upColor: '#00E676', downColor: '#FF3D5A',
    borderUpColor: '#00E676', borderDownColor: '#FF3D5A',
    wickUpColor: '#00E676', wickDownColor: '#FF3D5A',
  });
  try {
    const res = await fetch(`${API}/api/chart/ohlcv/${symbol}?timeframe=${currentTimeframe}&limit=2000`);
    const json = await res.json();
    if (json.data && json.data.length) {
      cs.setData(json.data);
      tvChart.timeScale().fitContent();
      const last = json.data[json.data.length - 1];
      setText('mOpen', '$' + last.open.toFixed(2));
      setText('mHigh', '$' + last.high.toFixed(2));
      setText('mLow', '$' + last.low.toFixed(2));
      setText('mClose', '$' + last.close.toFixed(2));
      setText('mVol', fmtVolume(last.volume));
    }
      if (typeof agoraTrackAction === "function") agoraTrackAction();
  } catch(e) { console.error('Chart error', e); }
  new ResizeObserver(() => { if (tvChart) tvChart.resize(chartEl.offsetWidth, chartEl.offsetHeight); }).observe(chartEl);
}

function updateChatContext(info) {
  document.getElementById('chatCtxSymbol').textContent = info.symbol;
  document.getElementById('chatCtxName').textContent = info.name ? '— ' + info.name : '';
  resetChatChips();
}

function resetChatChips() {
  const sym = currentSymbol || 'this stock';
  const chips = document.getElementById('chatChips');
  chips.innerHTML = '';
  [`What is ${sym}'s ROE vs sector average?`, `Show ${sym} price trend last 30 days`,
   `Compare ${sym} P/B ratio to Technology peers`, `Is ${sym} trading above its MA200?`,
   `Show top 5 stocks by market cap in ${sym}'s sector`].forEach(q => {
    const c = document.createElement('div');
    c.className = 'chat-chip'; c.textContent = q;
    c.onclick = () => { document.getElementById('chatInput').value = q; sendChat(); };
    chips.appendChild(c);
  });
}

function handleChatKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
}

function isConversational(q) {
  const q2 = q.trim().toLowerCase();
  const dataP = [/^(show|list|get|find|display|what is the|what are the|how much|how many|give me the)/,/\b(price trend|moving average|ma20|ma50|ma200|market cap|volume|52.week|top \d|bottom \d)\b/];
  if (dataP.some(p => p.test(q2))) return false;
  const chatP = [/^(is |are |was |were |will |would |should |could |can |do |does |did )/,/good (stock|investment|buy|time|idea)/,/worth (buying|investing|holding)/,/(why|what happened|explain|thoughts|opinion|overvalued|undervalued|risky)/,/good time|right time|safe to/,/best stock|best investment|invest right now|invest today/];
  return chatP.some(p => p.test(q2));
}

async function sendChat() {
  const input = document.getElementById('chatInput');
  const question = input.value.trim();
  if (!question || !currentSymbol) return;
  const btn = document.getElementById('sendBtn');
  btn.disabled = true; input.value = '';
  const contextQ = currentInfo
    ? `[Context: analyzing ${currentSymbol} (${currentInfo.name || currentSymbol}), sector: ${currentInfo.sector || 'unknown'}] ${question}`
    : question;
  appendMsg('user', question);
  const thinkId = appendThinking();
  try {
    if (isConversational(question)) {
      const ctx = currentInfo ? `${currentSymbol} (${currentInfo.name||currentSymbol}), sector: ${currentInfo.sector||"unknown"}, price: $${currentInfo.last_close||"N/A"}, change: ${currentInfo.daily_return_pct ? currentInfo.daily_return_pct.toFixed(2)+"%" : "N/A"}` : currentSymbol;
      const res = await fetch(`${API}/api/ai/chat`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({question, context: ctx}) });
      const data = await res.json();
      removeEl(thinkId);
      if (data.error) { appendMsg("ai", "Error: " + data.error); } else { appendMsg("ai", data.answer); }
      if (typeof agoraTrackAction === "function") agoraTrackAction();
    } else {
      const res = await fetch(`${API}/api/ai/query`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({question: contextQ}) });
      const data = await res.json(); removeEl(thinkId);
      if (data.error) { appendMsg("ai", "Error: " + data.error); } else { appendAIResult(data); }
      if (typeof agoraTrackAction === "function") agoraTrackAction();
    }
  } catch(e) { removeEl(thinkId); appendMsg("ai", "Connection error — is the API running?"); }
  btn.disabled = false; input.focus();
}
function appendMsg(role, text) {
  const msgs = document.getElementById('chatMessages');
  const d = document.createElement('div');
  d.className = 'msg ' + role;
  d.innerHTML = `<div class="msg-avatar">${role === 'ai' ? 'AI' : 'YOU'}</div><div class="msg-bubble">${esc(text)}</div>`;
  msgs.appendChild(d); msgs.scrollTop = msgs.scrollHeight;
}

function fmtHeader(col) {
  return col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

function appendAIResult(data) {
  const msgs = document.getElementById('chatMessages');
  const d = document.createElement('div');
  d.className = 'msg ai';
  let tbl = '';
  if (data.results && data.results.length) {
    const cols = Object.keys(data.results[0]);
    tbl = `<div class="results-mini"><table><thead><tr>${cols.map(c=>`<th>${fmtHeader(c)}</th>`).join('')}</tr></thead><tbody>${
      data.results.slice(0,8).map(row=>`<tr>${cols.map(c=>`<td>${fmtCell(row[c],c)}</td>`).join('')}</tr>`).join('')
    }</tbody></table>${data.row_count > 8 ? `<div style="font-family:var(--mono);font-size:15px;color:var(--text-dim);margin-top:6px;">+${data.row_count-8} more rows</div>` : ''}</div>`;
  }
  d.innerHTML = `<div class="msg-avatar">AI</div><div class="msg-bubble"><div style="color:var(--text-secondary);font-size:15px;margin-bottom:6px;">${data.row_count} row${data.row_count!==1?'s':''} · ${data.duration_ms}ms</div>${tbl}<div class="sql-toggle" onclick="var b=this.nextElementSibling;b.style.display=b.style.display=='none'?'block':'none';this.textContent=b.style.display=='none'?'+ Show SQL':'-  Hide SQL'">+ Show SQL</div><div class="sql-block" style="display:none;background:var(--bg-deep);border:1px solid var(--border);border-radius:4px;padding:10px 12px;margin-top:4px;font-family:var(--mono);font-size:11px;color:var(--text-secondary);white-space:pre-wrap;word-break:break-all;max-height:200px;overflow-y:auto;">${esc(data.sql)}</div></div>`;
  msgs.appendChild(d); msgs.scrollTop = msgs.scrollHeight;
}

function appendThinking() {
  const msgs = document.getElementById('chatMessages');
  const id = 'think_' + Date.now();
  const d = document.createElement('div');
  d.className = 'msg ai'; d.id = id;
  d.innerHTML = `<div class="msg-avatar">AI</div><div class="msg-bubble"><div class="thinking-dots"><span></span><span></span><span></span></div></div>`;
  msgs.appendChild(d); msgs.scrollTop = msgs.scrollHeight;
  return id;
}

function removeEl(id) { const e = document.getElementById(id); if (e) e.remove(); }


async function loadFilings(symbol) {
  const el = document.getElementById("filings-list");
  el.innerHTML = `<div style="color:var(--text-dim);font-family:var(--mono);font-size:15px;">Loading filings for ${symbol}...</div>`;
  try {
    const res = await fetch(`${API}/api/filings/${symbol}?forms=10-K,10-Q,8-K&limit=5`);
    const data = await res.json();
    if (!data.filings || data.filings.length === 0) {
      el.innerHTML = `<div style="color:var(--text-dim);font-family:var(--mono);font-size:15px;">No filings found for ${symbol}.</div>`;
      return;
    }
    el.innerHTML = data.filings.map(f => `
      <div style="border:1px solid var(--border);border-radius:6px;padding:16px;margin-bottom:16px;background:var(--bg-panel);">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">
          <span style="font-family:var(--mono);font-size:13px;font-weight:700;color:var(--teal);background:rgba(0,188,212,0.1);padding:3px 8px;border-radius:4px;">${f.form}</span>
          <span style="font-family:var(--mono);font-size:13px;color:var(--text-secondary);">${f.date}</span>
          <a href="${f.edgar_url}" target="_blank" style="font-family:var(--mono);font-size:12px;color:var(--text-dim);text-decoration:none;margin-left:auto;" onmouseover="this.style.color='var(--teal)'" onmouseout="this.style.color='var(--text-dim)'">VIEW ON EDGAR ↗</a>
        </div>
        <div style="font-family:var(--sans,system-ui);font-size:14px;line-height:1.6;color:var(--text-primary);">${f.summary || "Summary unavailable."}</div>
      </div>
    `).join("");
  } catch(e) {
    el.innerHTML = `<div style="color:var(--text-dim);font-family:var(--mono);font-size:15px;">Error loading filings: ${e.message}</div>`;
  }
}

function switchTab(name) {
  if (name === 'filings' && currentSymbol) loadFilings(currentSymbol);
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', ['overview','financials','ai','filings'][i] === name));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (name === 'overview' && tvChart) {
    setTimeout(() => { const el = document.getElementById('tv-chart'); tvChart.resize(el.offsetWidth, el.offsetHeight); }, 50);
  }
}

function setText(id, val) { const e = document.getElementById(id); if (e) e.textContent = val; }
function setColored(id, val, fmt) {
  const e = document.getElementById(id); if (!e) return;
  if (val === null || val === undefined) { e.textContent = '—'; e.className = 'stat-value'; return; }
  e.textContent = fmt(val); e.className = 'stat-value ' + (val >= 0 ? 'pos' : 'neg');
}
function fmtCap(n) {
  if (!n) return '—';
  if (n >= 1e12) return '$' + (n/1e12).toFixed(2) + 'T';
  if (n >= 1e9) return '$' + (n/1e9).toFixed(1) + 'B';
  return '$' + (n/1e6).toFixed(0) + 'M';
}
function fmtVolume(n) {
  if (!n) return '—';
  if (n >= 1e9) return (n/1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  return n.toLocaleString();
}
const PCT_COLS = new Set(['roe','roic','dividend_yield','daily_return_pct',
  'change_1d_pct','change_1w_pct','change_1m_pct','price_range_pct',
  'volume_ratio','pct_from_ma50','pct_from_52w_high','pct_from_52w_low']);

function isPctCol(col) {
  if (!col) return false;
  const c = col.toLowerCase();
  if (PCT_COLS.has(c)) return true;
  // Match derived names like sector_avg_roe, avg_roe, avg_roic etc.
  return /(^|_)(roe|roic|yield|return_pct|change_pct|div_yield)($|_)/.test(c);
}

function fmtCell(v, col) {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') {
    if (Math.abs(v) > 1e9) return fmtCap(v);
    if (isPctCol(col)) {
      const pct = v * 100;
      return (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%';
    }
    if (Math.abs(v) < 10 && !Number.isInteger(v)) return v.toFixed(3);
    return v.toFixed(2);
  }
  if (typeof v === "string" && v.match(/^\d{4}-\d{2}-\d{2}T/)) return v.substring(0, 10);
  return esc(String(v));
}
function esc(s) { return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }

function signOut() { localStorage.removeItem('agora_token'); localStorage.removeItem('agora_user'); window.location.href = '../auth/index.html'; }

function toggleUserMenu() {
  const menu = document.getElementById('user-menu');
  menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
}

document.addEventListener('click', (e) => {
  if (!e.target.closest('#user-avatar') && !e.target.closest('#user-menu')) {
    const menu = document.getElementById('user-menu');
    if (menu) menu.style.display = 'none';
  }
});

(function() {
  const user = JSON.parse(localStorage.getItem('agora_user') || 'null');
  if (user) { document.getElementById('user-nav').style.display = 'flex'; document.getElementById('user-email').textContent = user.email; const initials = user.email.substring(0,2).toUpperCase(); document.getElementById('user-avatar').textContent = initials; }
  else { document.getElementById('signin-nav').style.display = 'block'; }
  initResearchSearch();
  initBrowse();
  const params = new URLSearchParams(window.location.search);
  const sym = params.get('symbol');
  const aiQuery = params.get('ai');
  const loadSym = sym ? sym.toUpperCase() : 'AAPL';
  document.getElementById('symbolInput').value = loadSym;
  setTimeout(() => {
    loadSymbol();
    if (aiQuery) {
      setTimeout(() => {
        switchTab('ai');
        const chatInput = document.getElementById('chatInput');
        chatInput.value = decodeURIComponent(aiQuery);
        document.getElementById('sendBtn').disabled = false;
        setTimeout(sendChat, 300);
      }, 1500);
    }
  }, 100);
})();

async function loadPeerStrip(sector, currentSymbol) {
  const strip = document.getElementById("peerStrip");
  if (!strip) return;
  strip.innerHTML = "<span style=\"color:var(--text-dim);font-size:11px;font-family:var(--mono);padding:4px 0;\">PEERS</span>";
  try {
    const res = await fetch(`${API}/api/screener/screen?sector=${encodeURIComponent(sector)}&limit=7`);
    const data = await res.json();
    const peers = (data.data || []).filter(p => p.symbol !== currentSymbol).slice(0, 6);
    peers.forEach(p => {
      const chg = p.change_1w_pct;
      const chgStr = chg != null ? (chg >= 0 ? "+" : "") + chg.toFixed(2) + "%" : "—";
      const chgClass = chg == null ? "" : chg >= 0 ? "pos" : "neg";
      const card = document.createElement("div");
      card.className = "peer-card";
      card.innerHTML = `<span class="peer-ticker">${p.symbol}</span><span class="peer-chg ${chgClass}">${chgStr}</span>`;
      card.onclick = () => { document.getElementById("symbolInput").value = p.symbol; loadSymbol(p.symbol); };
      strip.appendChild(card);
    });
  } catch(e) { console.error("Peer strip error", e); }
}
