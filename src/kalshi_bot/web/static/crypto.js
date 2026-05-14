(function () {
  const bootstrapEl = document.getElementById("crypto-bootstrap");
  const gridEl = document.getElementById("crypto-grid");
  const statusEl = document.getElementById("crypto-status-strip");
  const refreshBtn = document.getElementById("crypto-refresh");
  const sortEl = document.getElementById("crypto-sort");
  const alertEl = document.getElementById("crypto-alert");

  const state = {
    payload: bootstrapEl ? JSON.parse(bootstrapEl.textContent || "{}") : { markets: [] },
    loading: false,
  };

  function showAlert(message) {
    if (!alertEl) return;
    alertEl.textContent = message;
    alertEl.hidden = false;
  }

  function clearAlert() {
    if (!alertEl) return;
    alertEl.textContent = "";
    alertEl.hidden = true;
  }

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

  function quoteTripletForSide(market, side) {
    const yesBid = numberOrNull(market && market.yes_bid_dollars);
    const yesAsk = numberOrNull(market && market.yes_ask_dollars);
    const noBid = numberOrNull(market && market.no_bid_dollars);
    const noAsk = numberOrNull(market && market.no_ask_dollars);
    let midYes = numberOrNull(market && market.mid_yes_dollars);
    if (midYes === null && yesBid !== null && yesAsk !== null) {
      midYes = (yesBid + yesAsk) / 2;
    }

    const normalizedSide = String(side || "").toLowerCase();
    if (normalizedSide === "yes") {
      return {
        bid: yesBid,
        ask: yesAsk,
        mid: midYes,
      };
    }

    const downBid = noBid !== null ? noBid : (yesAsk !== null ? 1 - yesAsk : null);
    const downAsk = noAsk !== null ? noAsk : (yesBid !== null ? 1 - yesBid : null);
    const downMid = midYes !== null ? 1 - midYes : (downBid !== null && downAsk !== null ? (downBid + downAsk) / 2 : null);
    return {
      bid: downBid,
      ask: downAsk,
      mid: downMid,
    };
  }

  function quoteTripletLabel(market, side) {
    const quote = quoteTripletForSide(market, side);
    const spreadBps =
      quote.ask !== null && quote.bid !== null
        ? Math.round((quote.ask - quote.bid) * 10000)
        : null;
    const spreadLabel = spreadBps === null ? "spread n/a" : `spread ${signedBps(spreadBps)}`;
    return `bid ${price(quote.bid)} / ask ${price(quote.ask)} / mid ${price(quote.mid)} / ${spreadLabel}`;
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

  function escapeHtml(value) {
    return String(value === null || value === undefined ? "" : value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[char]);
  }

  function modeLabel(mode) {
    if (mode === "live") return "Live";
    if (mode === "off") return "Off";
    return "Shadow";
  }

  function blockerText(market) {
    const blockers = Array.isArray(market.live_blockers) ? market.live_blockers : [];
    return blockers.slice(0, 3).join(" ");
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

  function numberOrNull(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function signedBps(value) {
    const number = numberOrNull(value);
    if (number === null) return "n/a";
    const rounded = Math.round(number);
    if (rounded > 0) return `+${rounded}bps`;
    return `${rounded}bps`;
  }

  function signalTrace(signal) {
    return signal && typeof signal.candidate_trace === "object" && signal.candidate_trace !== null
      ? signal.candidate_trace
      : {};
  }

  function isLiveCandidate(candidate) {
    if (!candidate || typeof candidate !== "object") return false;
    const status = String(candidate.candidate_status || candidate.status || "").toLowerCase();
    return candidate.live_eligible === true || status === "live_quality" || status === "selected" || status === "tradeable";
  }

  function candidateForSide(signal, side) {
    const trace = signalTrace(signal);
    const candidates = Array.isArray(trace.candidates) ? trace.candidates : [];
    const normalized = String(side || "").toLowerCase();
    return candidates.find((candidate) => String(candidate.side || "").toLowerCase() === normalized) || null;
  }

  function visibleCandidate(signal) {
    const trace = signalTrace(signal);
    const candidates = Array.isArray(trace.candidates) ? trace.candidates : [];
    const side = String(signal.recommended_side || trace.selected_side || "").toLowerCase();
    if (side) {
      const sideCandidate = candidateForSide(signal, side);
      if (sideCandidate) return sideCandidate;
    }
    return candidates.find((candidate) => Number(candidate.rank) === 1) || candidates[0] || null;
  }

  function tradeCandidate(signal) {
    const side = String((signal || {}).recommended_side || "").toLowerCase();
    const action = String((signal || {}).recommended_action || "").toLowerCase();
    const sideCandidate = side ? candidateForSide(signal, side) : null;
    if (side && action && action !== "none") {
      return sideCandidate || { side, live_eligible: true };
    }
    if (isLiveCandidate(sideCandidate)) return sideCandidate;
    const trace = signalTrace(signal);
    const candidates = Array.isArray(trace.candidates) ? trace.candidates : [];
    return candidates.find(isLiveCandidate) || null;
  }

  function sideLabel(side) {
    return String(side || "").toLowerCase() === "no" ? "Down" : "Up";
  }

  function edgeMetrics(signal) {
    const trace = signalTrace(signal);
    const candidate = visibleCandidate(signal) || {};
    return candidateEdgeMetrics(candidate, trace, signal);
  }

  function candidateEdgeMetrics(candidate, trace, signal) {
    candidate = candidate || {};
    trace = trace || {};
    signal = signal || {};
    const rawBps = numberOrNull(
      candidate.edge_bps ?? trace.selected_edge_bps ?? signal.edge_bps,
    );
    const expectedNetEdge = numberOrNull(candidate.expected_net_edge ?? trace.expected_net_edge);
    const netBps = expectedNetEdge === null ? null : Math.round(expectedNetEdge * 10000);
    const runtimeThresholds =
      candidate.runtime_thresholds && typeof candidate.runtime_thresholds === "object"
        ? candidate.runtime_thresholds
        : {};
    const neededBps = numberOrNull(runtimeThresholds.min_fee_adjusted_edge_bps ?? trace.min_edge_bps);
    return {
      rawBps,
      netBps,
      neededBps,
      sortBps: netBps === null ? rawBps : netBps,
    };
  }

  function candidateEdgeSummary(candidate, trace, signal) {
    const metrics = candidateEdgeMetrics(candidate || {}, trace || {}, signal || {});
    const parts = [];
    if (metrics.netBps !== null) parts.push(`net ${signedBps(metrics.netBps)}`);
    if (metrics.neededBps !== null) parts.push(`need ${signedBps(metrics.neededBps)}`);
    if (!parts.length && metrics.rawBps !== null) parts.push(`raw ${signedBps(metrics.rawBps)}`);
    return parts.join(" / ");
  }

  function edgeSummary(signal) {
    const metrics = edgeMetrics(signal || {});
    const parts = [];
    if (metrics.rawBps !== null) parts.push(`raw ${signedBps(metrics.rawBps)}`);
    if (metrics.netBps !== null) parts.push(`net ${signedBps(metrics.netBps)}`);
    if (metrics.neededBps !== null) parts.push(`need ${signedBps(metrics.neededBps)}`);
    return parts.length ? parts.join(" / ") : "edge n/a";
  }

  function humanizeCode(value) {
    const code = String(value || "");
    const labels = {
      broad_shadow_exploration: "net edge below live min; shadow only",
      fee_adjusted_edge_below_live_min: "net edge below live min",
      contract_price_below_crypto_min: "contract price below crypto min",
      remaining_payout_below_crypto_min: "remaining payout below crypto min",
      spread_above_live_max: "spread above live max",
      crypto_market_too_early_for_live_entry: "market too early for live entry",
      crypto_market_too_late_for_live_entry: "market too late for live entry",
      positive_fee_adjusted_live_quality_edge: "normal fee-adjusted edge",
      late_high_confidence_directional_entry: "late high-confidence market-anchored choice",
      empirical_bucket_not_allowed: "empirical bucket not allowed",
      empirical_bucket_missing: "empirical bucket unknown",
      empirical_bucket_under_sampled: "empirical bucket under-sampled",
      empirical_bucket_negative_pnl: "empirical bucket negative P&L",
      empirical_bucket_low_win_rate: "empirical bucket below win-rate floor",
      empirical_bucket_allowed: "empirical bucket passed",
      crypto_position_add_on_allowed: "add-on allowed",
      crypto_position_add_on_blocked: "add-on blocked",
      crypto_entry_window_unknown: "entry window unknown",
    };
    return labels[code] || code.replace(/_/g, " ");
  }

  function empiricalBucketLabel(candidate, trace) {
    const gate =
      (candidate && candidate.empirical_bucket_gate && typeof candidate.empirical_bucket_gate === "object"
        ? candidate.empirical_bucket_gate
        : null) ||
      (trace && trace.empirical_bucket_gate && typeof trace.empirical_bucket_gate === "object"
        ? trace.empirical_bucket_gate
        : null);
    if (!gate) return "";
    const status = String(gate.status || "").toLowerCase();
    if (status === "allowed") return "bucket passed";
    if (status === "blocked") return `bucket blocked: ${humanizeCode(gate.reason)}`;
    if (status === "unknown") return "bucket unknown";
    return "";
  }

  function addOnLabel(market) {
    const risk = market && market.active_room ? market.active_room.latest_risk : null;
    const addOn = risk && risk.crypto_position_add_on && typeof risk.crypto_position_add_on === "object"
      ? risk.crypto_position_add_on
      : null;
    if (!addOn) return "";
    return addOn.reason === "crypto_position_add_on_allowed"
      ? "add-on allowed"
      : `add-on blocked: ${humanizeCode(addOn.reason)}`;
  }

  function candidateBlockerText(candidate, trace) {
    trace = trace || {};
    if (!candidate) return "";
    const status = String(candidate.candidate_status || candidate.status || trace.candidate_status || "").toLowerCase();
    const reason = candidate.reason || (Array.isArray(candidate.reasons) ? candidate.reasons[0] : null) || trace.selection_reason;
    const liveEligible = candidate.live_eligible;
    const metrics = candidateEdgeMetrics(candidate, trace, {});
    if (status && !["live_quality", "selected", "tradeable"].includes(status)) {
      return reason ? humanizeCode(reason) : humanizeCode(status);
    }
    if (liveEligible === false && reason) return humanizeCode(reason);
    if (metrics.netBps !== null && metrics.neededBps !== null && metrics.netBps < metrics.neededBps) {
      return "fee adjusted edge below live min";
    }
    return "";
  }

  function candidateBlocker(signal) {
    const trace = signalTrace(signal || {});
    const candidate = visibleCandidate(signal || {});
    return candidateBlockerText(candidate, trace);
  }

  function candidateStatusLabel(candidate, trace) {
    if (!candidate) return "no candidate";
    const blocker = candidateBlockerText(candidate, trace);
    if (blocker) return `blocked: ${blocker}`;
    const status = String(candidate.candidate_status || candidate.status || "").toLowerCase();
    if (status === "live_quality" || candidate.live_eligible === true) return "live quality";
    if (status === "exploratory_shadow") return "shadow only";
    return status ? humanizeCode(status) : "available";
  }

  function sideDiagnostic(signal, side) {
    const trace = signalTrace(signal || {});
    const candidate = candidateForSide(signal || {}, side);
    if (!candidate) return "no model candidate";
    const edge = candidateEdgeSummary(candidate, trace, signal);
    const status = candidateStatusLabel(candidate, trace);
    const bucket = empiricalBucketLabel(candidate, trace);
    return [edge, status, bucket].filter(Boolean).join(" · ");
  }

  function signalState(signal, signalBlocker) {
    if (signalBlocker) return { text: "blocked", className: "blocked" };
    const candidate = tradeCandidate(signal || {});
    if (candidate && (candidate.candidate_status === "live_quality" || candidate.live_eligible === true)) {
      return { text: "passed", className: "passed" };
    }
    return { text: "watching", className: "mixed" };
  }

  function trendScore(market) {
    const metrics = edgeMetrics((market || {}).signal || {});
    return Math.abs(Number(metrics.sortBps || 0));
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
      markets.sort((a, b) => trendScore(b) - trendScore(a));
    }
    return markets;
  }

  function renderStatus() {
    const gate = (state.payload || {}).replay_gate || {};
    const settings = (state.payload || {}).settings || {};
    const counts = (state.payload || {}).asset_mode_counts || {};
    const signalSummary = (state.payload || {}).signal_summary || {};
    const markets = (state.payload || {}).markets || [];
    const items = [
      ["Markets", markets.length],
      ["Replay Gate", gate.status || "missing"],
      ["Trading", settings.crypto_trading_enabled ? "enabled" : "disabled"],
      ["Autonomy", settings.crypto_autonomy_enabled ? "enabled" : "disabled"],
      ["Live Assets", counts.live || 0],
      ["Normal Edge", signalSummary.normal_edge_trade_count || 0],
      ["Late Bypass", signalSummary.late_high_confidence_trade_count || 0],
      ["Buckets", `${signalSummary.empirical_bucket_allowed_count || 0}/${signalSummary.empirical_bucket_blocked_count || 0}/${signalSummary.empirical_bucket_unknown_count || 0}`],
      ["Updated", timeLabel((state.payload || {}).updated_at)],
    ];
    statusEl.innerHTML = items
      .map(([label, value]) => `<div class="crypto-status-pill"><span>${label}</span><strong>${value}</strong></div>`)
      .join("");
  }

  function card(market) {
    const signal = market.signal || {};
    const trace = signalTrace(signal);
    const room = market.active_room;
    const selectedCandidate = tradeCandidate(signal);
    const side = selectedCandidate ? String(selectedCandidate.side || "").toLowerCase() : null;
    const blockedCandidate = visibleCandidate(signal);
    const blockedSide = blockedCandidate ? String(blockedCandidate.side || trace.selected_side || "").toLowerCase() : trace.selected_side;
    const target = market.target_price_dollars ? money(market.target_price_dollars, 4) : "target n/a";
    const title = `${market.asset_symbol || "CRYPTO"} 15 min · ${target} target`;
    const iconText = String(market.asset_symbol || "?").slice(0, 2);
    const iconColor = assetColor(market.asset_symbol);
    const mode = market.asset_mode || ((state.payload || {}).asset_modes || {})[market.asset_symbol] || "shadow";
    const blockers = blockerText(market);
    const blockersHtml = mode === "live" && blockers
      ? `<div class="crypto-live-blockers">Live blocked: ${escapeHtml(blockers)}</div>`
      : "";
    const signalBlocker = candidateBlocker(signal);
    const selectedSideLabel = blockedSide ? sideLabel(blockedSide) : "Candidate";
    const signalBlockerHtml = signalBlocker
      ? `<div class="crypto-signal-blocker">Signal blocked on ${escapeHtml(selectedSideLabel)}: ${escapeHtml(signalBlocker)}</div>`
      : "";
    const addOn = addOnLabel(market);
    const addOnHtml = addOn ? `<div class="crypto-signal-blocker">${escapeHtml(addOn)}</div>` : "";
    const signalStatus = signalState(signal, signalBlocker);
    const upQuote = quoteTripletForSide(market, "yes");
    const downQuote = quoteTripletForSide(market, "no");
    return `
      <article class="crypto-card crypto-card-${mode}" data-market="${escapeHtml(market.market_ticker)}">
        <div class="crypto-card-header">
          <div>
            <div class="crypto-asset">
              <span class="crypto-icon" style="background:${iconColor}">${escapeHtml(iconText)}</span>
              <span class="crypto-symbol">${escapeHtml(market.asset_symbol || "CRYPTO")}</span>
              <span class="crypto-mode crypto-mode-${mode}">${modeLabel(mode)}</span>
            </div>
            <h2 class="crypto-title">${escapeHtml(title)}</h2>
            <div class="crypto-meta">
              <span class="crypto-live-dot"></span>
              <span class="crypto-live-label">${escapeHtml(market.status || "live")}</span>
              <span>${escapeHtml(timeLabel(market.close_time))}</span>
            </div>
          </div>
          <div class="crypto-card-actions">
            <select class="crypto-mode-select" data-asset-mode="${escapeHtml(market.asset_symbol || "")}" data-current-mode="${escapeHtml(mode)}" aria-label="${escapeHtml(market.asset_symbol || "crypto")} mode">
              <option value="off"${mode === "off" ? " selected" : ""}>Off</option>
              <option value="shadow"${mode === "shadow" ? " selected" : ""}>Shadow</option>
              <option value="live"${mode === "live" ? " selected" : ""}>Live</option>
            </select>
            ${
              room
                ? `<a class="crypto-room-button" href="/rooms/${escapeHtml(room.id)}">Room</a>`
                : `<button class="crypto-room-button" type="button" data-room-market="${escapeHtml(market.market_ticker)}">Room</button>`
            }
          </div>
        </div>
        ${blockersHtml}
        ${signalBlockerHtml}
        ${addOnHtml}
        <div class="crypto-sides">
          <div class="crypto-side-row ${side === "yes" ? "crypto-side-selected" : ""}">
            <span class="crypto-side-copy">
              <span class="crypto-side-name">Up</span>
              <span class="crypto-side-detail">${escapeHtml(sideDiagnostic(signal, "yes"))}</span>
              <span class="crypto-side-quote">${escapeHtml(quoteTripletLabel(market, "yes"))}</span>
            </span>
            <span class="crypto-side-price">${price(upQuote.ask ?? upQuote.bid)}</span>
            <span class="crypto-badge crypto-badge-up">${side === "yes" ? "✓" : "×"}</span>
          </div>
          <div class="crypto-side-row ${side === "no" ? "crypto-side-selected" : ""}">
            <span class="crypto-side-copy">
              <span class="crypto-side-name">Down</span>
              <span class="crypto-side-detail">${escapeHtml(sideDiagnostic(signal, "no"))}</span>
              <span class="crypto-side-quote">${escapeHtml(quoteTripletLabel(market, "no"))}</span>
            </span>
            <span class="crypto-side-price">${price(downQuote.ask ?? downQuote.bid)}</span>
            <span class="crypto-badge crypto-badge-down">${side === "no" ? "✓" : "×"}</span>
          </div>
        </div>
        <div class="crypto-card-footer">
          <span>${escapeHtml(formatVolume(market.volume))}</span>
          <span class="crypto-gate crypto-gate-${escapeHtml(signalStatus.className)}">${escapeHtml(signalStatus.text)}</span>
          <span class="crypto-edge-summary">${escapeHtml(edgeSummary(signal))}</span>
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
        clearAlert();
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

  async function errorMessage(response, symbol) {
    let message = `Could not set ${symbol} mode.`;
    try {
      const payload = await response.json();
      if (payload.detail) message = payload.detail;
      if (payload.error) message = payload.error;
    } catch (_err) {
      // Keep the generic message when the server did not return JSON.
    }
    return message;
  }

  async function setAssetMode(symbol, mode, select) {
    if (!symbol) return;
    const previousMode = select ? select.getAttribute("data-current-mode") : null;
    clearAlert();
    if (select) select.disabled = true;
    try {
      const response = await fetch(`/api/crypto/assets/${encodeURIComponent(symbol)}/mode`, {
        method: "PATCH",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      if (response.ok) {
        await refresh();
        return;
      }
      if (select && previousMode) select.value = previousMode;
      showAlert(await errorMessage(response, symbol));
      render();
    } catch (_err) {
      if (select && previousMode) select.value = previousMode;
      showAlert(`Could not set ${symbol} mode.`);
      render();
    } finally {
      if (select && select.isConnected) select.disabled = false;
    }
  }

  gridEl.addEventListener("click", (event) => {
    const button = event.target.closest("[data-room-market]");
    if (button) {
      createRoom(button.getAttribute("data-room-market"));
    }
  });

  gridEl.addEventListener("change", (event) => {
    const select = event.target.closest("[data-asset-mode]");
    if (select) {
      setAssetMode(select.getAttribute("data-asset-mode"), select.value, select);
    }
  });

  if (refreshBtn) refreshBtn.addEventListener("click", refresh);
  if (sortEl) sortEl.addEventListener("change", render);

  render();
  window.setInterval(refresh, 30000);
})();
