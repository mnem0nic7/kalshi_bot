(function () {
  "use strict";

  const REFRESH_MS = 30_000;

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  // Tab switching
  document.querySelectorAll(".dash-tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const env = btn.dataset.env;
      document.querySelectorAll(".dash-tab").forEach((b) => {
        b.classList.toggle("is-active", b === btn);
        b.setAttribute("aria-selected", b === btn ? "true" : "false");
      });
      document.querySelectorAll(".dash-panel").forEach((panel) => {
        const active = panel.dataset.env === env;
        panel.classList.toggle("is-active", active);
        panel.hidden = !active;
      });
    });
  });

  // Relative timestamps
  function formatAge(isoString) {
    if (!isoString) return "—";
    const diffMs = Date.now() - new Date(isoString).getTime();
    const s = Math.floor(diffMs / 1000);
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return new Date(isoString).toLocaleDateString();
  }

  function refreshTimestamps() {
    document.querySelectorAll("[data-timestamp]").forEach((node) => {
      const ts = node.dataset.timestamp;
      if (ts) node.textContent = formatAge(ts);
    });
  }

  function setLastRefreshed() {
    const node = document.getElementById("dash-last-refreshed");
    if (node) node.textContent = new Date().toLocaleTimeString();
  }

  function pill(text, tone) {
    const map = { good: "status-good", bad: "status-bad", warning: "status-warning", neutral: "status-neutral" };
    return el("span", ["status-pill", "alert-pill", map[tone] || ""].join(" ").trim(), text);
  }

  function toneClass(tone) {
    if (tone === "good") return "value-positive";
    if (tone === "bad") return "value-negative";
    return "value-neutral";
  }

  function renderSummary(panel, summary) {
    if (!panel) return;
    panel.querySelectorAll("[data-summary-key]").forEach((node) => {
      const key = node.dataset.summaryKey;
      node.textContent = (summary && key && summary[key]) || "—";
      const toneKey = node.dataset.summaryTone;
      if (toneKey) {
        node.classList.remove("value-positive", "value-negative", "value-neutral");
        node.classList.add(toneClass((summary && summary[toneKey]) || "neutral"));
      }
    });
  }

  function renderActiveRooms(card, rooms) {
    const section = card.querySelector(".active-rooms-section");
    if (!section) return;
    const countLabel = section.querySelector(".active-rooms-count");
    if (countLabel) countLabel.textContent = `${rooms.length} active`;

    let listEl = section.querySelector(".active-rooms-list");
    const emptyEl = section.querySelector("p.empty-state");
    if (!rooms.length) {
      if (listEl) listEl.remove();
      if (!emptyEl) section.appendChild(el("p", "empty-state", "No active rooms."));
      return;
    }
    if (emptyEl) emptyEl.remove();
    if (!listEl) {
      listEl = el("ul", "active-rooms-list");
      section.appendChild(listEl);
    }
    listEl.replaceChildren(
      ...rooms.map((r) => {
        const li = el("li", "active-room-row");
        li.append(el("span", "mono active-room-ticker", r.market_ticker));
        li.append(el("span", "status-pill status-neutral active-room-stage", r.stage));
        if (r.updated_at) {
          const ts = el("span", "muted-label", formatAge(r.updated_at));
          ts.dataset.timestamp = r.updated_at;
          li.appendChild(ts);
        }
        return li;
      })
    );
  }

  function renderAlerts(card, alerts) {
    const header = card.querySelector(".dash-card-header");
    const errors = alerts.filter((a) => a.severity === "error");
    const warnings = alerts.filter((a) => a.severity === "warning");
    const pillEl = header.querySelector(".status-pill");
    if (pillEl) {
      if (errors.length) {
        pillEl.className = "status-pill status-bad";
        pillEl.textContent = `${errors.length} error${errors.length !== 1 ? "s" : ""}`;
      } else if (warnings.length) {
        pillEl.className = "status-pill status-warning";
        pillEl.textContent = `${warnings.length} warning${warnings.length !== 1 ? "s" : ""}`;
      } else {
        pillEl.className = "status-pill status-good";
        pillEl.textContent = "Clear";
      }
    }

    let list = card.querySelector(".alert-list");
    const empty = card.querySelector("p.empty-state");
    if (!alerts.length) {
      if (list) list.remove();
      if (!empty) card.appendChild(el("p", "empty-state", "No recent alerts."));
      return;
    }
    if (empty) empty.remove();
    if (!list) {
      list = el("div", "alert-list");
      card.appendChild(list);
    }
    list.replaceChildren(
      ...alerts.map((a) => {
        const tone = a.severity === "error" ? "bad" : a.severity === "warning" ? "warning" : "neutral";
        const body = el("div", "alert-body");
        body.append(el("strong", null, a.summary));
        const meta = el("span", "muted-label", a.source + (a.created_at ? " · " : ""));
        if (a.created_at) {
          const ts = el("span", null, formatAge(a.created_at));
          ts.dataset.timestamp = a.created_at;
          meta.appendChild(ts);
        }
        body.appendChild(meta);
        const row = el("div", "alert-row");
        row.append(pill(a.severity, tone), body);
        return row;
      })
    );
  }

  function renderPositions(card, positions) {
    const header = card.querySelector(".dash-card-header");
    const countLabel = header.querySelector(".muted-label");
    if (countLabel) countLabel.textContent = `${positions.length} position${positions.length !== 1 ? "s" : ""}`;

    const empty = card.querySelector("p.empty-state");
    if (!positions.length) {
      const wrap = card.querySelector(".table-wrap");
      if (wrap) wrap.remove();
      if (!empty) card.appendChild(el("p", "empty-state", "No open positions."));
      return;
    }
    if (empty) empty.remove();

    let tableWrap = card.querySelector(".table-wrap");
    if (!tableWrap) {
      tableWrap = el("div", "table-wrap");
      card.appendChild(tableWrap);
    }

    const table = el("table", "positions-table");
    const thead = el("thead");
    const headerRow = el("tr");
    ["Market", "Side", "Contracts", "Avg Price", "Now", "Notional", "P/L"].forEach((h) => headerRow.appendChild(el("th", null, h)));
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = el("tbody");
    positions.forEach((pos) => {
      const tr = el("tr");
      const marketTd = el("td");
      const marketWrap = el("div", "position-market");
      marketWrap.appendChild(el("span", "mono", pos.market_ticker));
      if (pos.model_quality_status === "warn") {
        const flags = [];
        const count = parseFloat(pos.count_fp || "0");
        const cap = pos.recommended_size_cap_fp ? parseFloat(pos.recommended_size_cap_fp) : null;
        if (pos.trade_regime === "near_threshold") flags.push("Near Threshold");
        if (pos.trade_regime === "longshot_yes" || pos.trade_regime === "longshot_no") flags.push("Longshot");
        if (pos.warn_only_blocked) flags.push("Broken Book");
        if (cap != null && Number.isFinite(cap) && count > cap) flags.push("Oversized");
        if (flags.length) {
          const flagWrap = el("div", "position-flags");
          if (Array.isArray(pos.model_quality_reasons) && pos.model_quality_reasons.length) {
            flagWrap.title = pos.model_quality_reasons.join(" ");
          }
          flags.forEach((flag) => flagWrap.appendChild(el("span", "position-flag", flag)));
          marketWrap.appendChild(flagWrap);
        }
      }
      marketTd.appendChild(marketWrap);
      const sidePill = el("span", `status-pill ${pos.side === "yes" ? "status-good" : "status-warning"}`, pos.side);
      const sideTd = el("td");
      sideTd.appendChild(sidePill);
      const pnlTd = el("td", `mono ${toneClass(pos.unrealized_pnl_tone)}`.trim(), pos.unrealized_pnl_display || "—");
      tr.append(
        marketTd,
        sideTd,
        el("td", "mono", pos.count_fp),
        el("td", "mono", pos.average_price_display || "—"),
        el("td", "mono", pos.current_price_display || "—"),
        el("td", "mono", pos.notional_display || "—"),
        pnlTd
      );
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    tableWrap.replaceChildren(table);
  }

  function renderCapitalBuckets(card, summary) {
    if (!card) return;
    const line = card.querySelector(".bucket-usage-line");
    const buckets = summary && summary.capital_buckets;
    if (!line || !buckets) return;
    line.textContent = `Safe ${buckets.safe_used_display || "—"} used · Risky ${buckets.risky_used_display || "—"} / ${buckets.risky_limit_display || "—"}`;
  }

  async function refresh(env) {
    try {
      const resp = await fetch(`/api/dashboard/${env}`);
      if (!resp.ok) return;
      const data = await resp.json();
      const panel = document.querySelector(`.dash-panel[data-env="${env}"]`);
      if (!panel) return;
      renderSummary(panel.querySelector(".dash-summary"), data.portfolio || {});
      renderActiveRooms(panel.querySelector(".dash-card-alerts"), data.active_rooms || []);
      renderAlerts(panel.querySelector(".dash-card-alerts"), data.alerts || []);
      renderCapitalBuckets(panel.querySelector(".dash-card-positions"), data.positions_summary || {});
      renderPositions(panel.querySelector(".dash-card-positions"), data.positions || []);
    } catch (_) {
      // skip on network error
    }
  }

  async function refreshAll() {
    await Promise.all([refresh("demo"), refresh("production")]);
    setLastRefreshed();
    refreshTimestamps();
  }

  refreshTimestamps();
  setLastRefreshed();
  setInterval(refreshAll, REFRESH_MS);
  setInterval(refreshTimestamps, 15_000);
})();
