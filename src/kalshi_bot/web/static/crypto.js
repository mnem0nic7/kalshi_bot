(function () {
  const bootstrapEl = document.getElementById("crypto-bootstrap");
  const gridEl = document.getElementById("crypto-grid");
  const statusEl = document.getElementById("crypto-status-strip");
  const refreshBtn = document.getElementById("crypto-refresh");
  const sortEl = document.getElementById("crypto-sort");

  const state = {
    payload: bootstrapEl ? JSON.parse(bootstrapEl.textContent || "{}") : { markets: [] },
    loading: false,
  };

  function money(value, digits) {
    if (value === null || value === undefined || value === "") return "n/a";
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    return `$${number.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: 0 })}`;
  }

  function price(value) {
    if (value === null || value === undefined || value === "") return "n/a";
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    return number.toFixed(4);
  }

  function timeLabel(value) {
    if (!value) return "close unknown";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "close unknown";
    return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  }

  function formatVolume(value) {
    if (value === null || value === undefined) return "$0 vol";
    const number = Number(value);
    if (!Number.isFinite(number)) return `${value} vol`;
    return `$${number.toLocaleString()} vol`;
  }

  function assetColor(symbol) {
    const colors = {
      BTC: "#ff9f1c",
      ETH: "#6f8cff",
      SOL: "#28d7ba",
      XRP: "#9aa4b2",
      DOGE: "#d5b72f",
      BNB: "#f3ba2f",
      HYPE: "#00b894",
      BCH: "#8ac926",
      ADA: "#3f7cff",
    };
    return colors[symbol] || "#d8dee7";
  }

  function sortedMarkets() {
    const markets = [...((state.payload || {}).markets || [])];
    const mode = sortEl ? sortEl.value : "trending";
    if (mode === "closing") {
      markets.sort((a, b) => String(a.close_time || "").localeCompare(String(b.close_time || "")));
    } else if (mode === "volume") {
      markets.sort((a, b) => Number(b.volume || 0) - Number(a.volume || 0));
    } else if (mode === "asset") {
      markets.sort((a, b) => String(a.asset_symbol || "").localeCompare(String(b.asset_symbol || "")));
    } else {
      markets.sort((a, b) => Math.abs(Number((b.signal || {}).edge_bps || 0)) - Math.abs(Number((a.signal || {}).edge_bps || 0)));
    }
    return markets;
  }

  function renderStatus() {
    const gate = (state.payload || {}).replay_gate || {};
    const settings = (state.payload || {}).settings || {};
    const markets = (state.payload || {}).markets || [];
    const items = [
      ["Markets", markets.length],
      ["Replay Gate", gate.status || "missing"],
      ["Trading", settings.crypto_trading_enabled ? "enabled" : "disabled"],
      ["Updated", timeLabel((state.payload || {}).updated_at)],
    ];
    statusEl.innerHTML = items
      .map(([label, value]) => `<div class="crypto-status-pill"><span>${label}</span><strong>${value}</strong></div>`)
      .join("");
  }

  function card(market) {
    const signal = market.signal || {};
    const gateStatus = ((state.payload || {}).replay_gate || {}).status || "missing";
    const room = market.active_room;
    const side = signal.recommended_side;
    const target = market.target_price_dollars ? money(market.target_price_dollars, 4) : "target n/a";
    const title = `${market.asset_symbol || "CRYPTO"} 15 min · ${target} target`;
    const iconText = String(market.asset_symbol || "?").slice(0, 2);
    const iconColor = assetColor(market.asset_symbol);
    return `
      <article class="crypto-card" data-market="${market.market_ticker}">
        <div class="crypto-card-header">
          <div>
            <div class="crypto-asset">
              <span class="crypto-icon" style="background:${iconColor}">${iconText}</span>
              <span class="crypto-symbol">${market.asset_symbol || "CRYPTO"}</span>
            </div>
            <h2 class="crypto-title">${title}</h2>
            <div class="crypto-meta">
              <span class="crypto-live-dot"></span>
              <span class="crypto-live-label">${market.status || "live"}</span>
              <span>${timeLabel(market.close_time)}</span>
            </div>
          </div>
          ${
            room
              ? `<a class="crypto-room-button" href="/rooms/${room.id}">Room</a>`
              : `<button class="crypto-room-button" type="button" data-room-market="${market.market_ticker}">Room</button>`
          }
        </div>
        <div class="crypto-sides">
          <div class="crypto-side-row">
            <span class="crypto-side-name">Up</span>
            <span class="crypto-side-price">${price(market.yes_ask_dollars || market.yes_bid_dollars)}</span>
            <span class="crypto-badge crypto-badge-up">${side === "yes" ? "✓" : "×"}</span>
          </div>
          <div class="crypto-side-row">
            <span class="crypto-side-name">Down</span>
            <span class="crypto-side-price">${price(market.no_ask_dollars)}</span>
            <span class="crypto-badge crypto-badge-down">${side === "no" ? "✓" : "×"}</span>
          </div>
        </div>
        <div class="crypto-card-footer">
          <span>${formatVolume(market.volume)}</span>
          <span class="crypto-gate crypto-gate-${gateStatus}">${gateStatus}</span>
          <span>${Math.abs(Number(signal.edge_bps || 0))} bps</span>
        </div>
      </article>
    `;
  }

  function render() {
    renderStatus();
    const markets = sortedMarkets();
    gridEl.innerHTML = markets.length ? markets.map(card).join("") : '<div class="crypto-empty">No crypto markets discovered yet.</div>';
  }

  async function refresh() {
    if (state.loading) return;
    state.loading = true;
    try {
      const response = await fetch("/api/crypto/markets?frequency=15m", { headers: { Accept: "application/json" } });
      if (response.ok) {
        state.payload = await response.json();
        render();
      }
    } finally {
      state.loading = false;
    }
  }

  async function createRoom(marketTicker) {
    const response = await fetch(`/api/crypto/markets/${encodeURIComponent(marketTicker)}/rooms`, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) return;
    const payload = await response.json();
    if (payload.redirect) window.location.href = payload.redirect;
  }

  gridEl.addEventListener("click", (event) => {
    const button = event.target.closest("[data-room-market]");
    if (button) {
      createRoom(button.getAttribute("data-room-market"));
    }
  });

  if (refreshBtn) refreshBtn.addEventListener("click", refresh);
  if (sortEl) sortEl.addEventListener("change", render);

  render();
  window.setInterval(refresh, 30000);
})();
