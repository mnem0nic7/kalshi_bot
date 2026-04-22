(function () {
  "use strict";

  const REFRESH_MS = 30_000;
  const STRATEGY_COLORS = ["#f9d77e", "#8be8c8", "#7db2ff", "#ffb48f", "#d9a4ff"];

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  function setTestId(node, value) {
    if (node && value) node.setAttribute("data-testid", value);
    return node;
  }

  function clearNode(node, children) {
    if (!node) return;
    node.replaceChildren(...children);
  }

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

  function toneClass(tone) {
    if (tone === "good") return "value-positive";
    if (tone === "bad") return "value-negative";
    return "value-neutral";
  }

  function pill(text, tone) {
    const map = { good: "status-good", bad: "status-bad", warning: "status-warning", neutral: "status-neutral" };
    return el("span", ["status-pill", "alert-pill", map[tone] || ""].join(" ").trim(), text);
  }

  function tradeProposalTone(status) {
    const normalized = String(status || "").trim().toLowerCase();
    if (normalized === "approved") return "good";
    if (normalized === "blocked") return "bad";
    if (normalized === "review") return "warning";
    return "neutral";
  }

  function statusPillClass(tone) {
    if (tone === "good") return "status-good";
    if (tone === "bad") return "status-bad";
    if (tone === "warning") return "status-warning";
    return "status-neutral";
  }

  function proposalRiskReasonText(proposal) {
    if (!proposal || proposal.risk_status !== "blocked" || !Array.isArray(proposal.risk_reasons) || !proposal.risk_reasons.length) {
      return "";
    }
    return proposal.risk_reasons.map((reason) => String(reason || "").trim()).filter(Boolean).join(" ");
  }

  function parseBootstrap() {
    const node = document.getElementById("strategies-bootstrap");
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_) {
      return null;
    }
  }

  const STRATEGY_FOCUS_MODES = ["cities", "strategies", "review"];
  const REVIEW_QUEUE_ORDER = [
    "drifted_assignment",
    "ready_for_approval",
    "evidence_weakened",
    "aligned",
    "waiting_for_evidence",
  ];
  const STRATEGY_CITY_FILTERS = [
    "all",
    "actionable",
    "needs_review",
    "mismatch",
    "unassigned",
    "low_confidence",
    "no_outcomes",
  ];
  const ACTIONABLE_RECOMMENDATION_STATUSES = new Set(["strong_recommendation", "lean_recommendation"]);
  const LOW_CONFIDENCE_RECOMMENDATION_STATUSES = new Set(["too_close", "low_sample"]);
  const CODEX_PROVIDER_MODEL_FALLBACKS = {
    gemini: ["gemini-2.5-pro", "gemini-2.5-flash"],
    codex: ["gpt-4o"],
  };

  function reviewPriority(status) {
    const index = REVIEW_QUEUE_ORDER.indexOf(status || "");
    return index === -1 ? REVIEW_QUEUE_ORDER.length : index;
  }

  function strategyFocusLabel(mode, count) {
    if (mode === "cities") return "Cities";
    if (mode === "strategies") return "Strategies";
    return count > 0 ? `Review Queue (${count})` : "Review Queue";
  }

  function strategyCityFilterLabel(filterKey) {
    if (filterKey === "actionable") return "Actionable";
    if (filterKey === "needs_review") return "Needs Review";
    if (filterKey === "mismatch") return "Mismatch";
    if (filterKey === "unassigned") return "Unassigned";
    if (filterKey === "low_confidence") return "Low Confidence";
    if (filterKey === "no_outcomes") return "No Outcomes";
    return "All";
  }

  function normalizeStrategyFocusMode(mode, summary) {
    if (mode === "review" && !(summary && summary.review_available)) return "cities";
    if (!STRATEGY_FOCUS_MODES.includes(mode || "")) return "cities";
    return mode;
  }

  function citySearchText(row) {
    const assignment = row && row.assignment ? row.assignment.strategy_name : "";
    const recommendation = row && row.recommendation ? row.recommendation.strategy_name : "";
    return [
      row && row.series_ticker,
      row && row.city_label,
      row && row.location_name,
      assignment,
      recommendation,
      row && row.best_strategy,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
  }

  function cityMatchesSearch(row, query) {
    if (!query) return true;
    return citySearchText(row).includes(query);
  }

  function cityMatchesFilter(row, filterKey, reviewAvailable) {
    const recommendationStatus = row && row.recommendation ? row.recommendation.status : null;
    const review = row && row.review ? row.review : {};
    if (filterKey === "actionable") {
      return ACTIONABLE_RECOMMENDATION_STATUSES.has(recommendationStatus);
    }
    if (filterKey === "needs_review") {
      return Boolean(reviewAvailable && review && review.needs_review);
    }
    if (filterKey === "mismatch") {
      return row && row.assignment_context_status === "differs_from_recommendation";
    }
    if (filterKey === "unassigned") {
      return !(row && row.assignment && row.assignment.strategy_name);
    }
    if (filterKey === "low_confidence") {
      return LOW_CONFIDENCE_RECOMMENDATION_STATUSES.has(recommendationStatus);
    }
    if (filterKey === "no_outcomes") {
      return recommendationStatus === "no_outcomes";
    }
    return true;
  }

  function countCityFilters(rows, reviewAvailable) {
    const counts = {
      all: rows.length,
      actionable: 0,
      needs_review: 0,
      mismatch: 0,
      unassigned: 0,
      low_confidence: 0,
      no_outcomes: 0,
    };
    rows.forEach((row) => {
      STRATEGY_CITY_FILTERS.forEach((filterKey) => {
        if (filterKey === "all") return;
        if (cityMatchesFilter(row, filterKey, reviewAvailable)) {
          counts[filterKey] += 1;
        }
      });
    });
    return counts;
  }

  function analysisSummaryCounts(rows) {
    return rows.reduce((acc, row) => {
      const recommendationStatus = row && row.recommendation ? row.recommendation.status : null;
      if (ACTIONABLE_RECOMMENDATION_STATUSES.has(recommendationStatus)) acc.actionable += 1;
      if (row && row.assignment_context_status === "differs_from_recommendation") acc.mismatch += 1;
      if (LOW_CONFIDENCE_RECOMMENDATION_STATUSES.has(recommendationStatus)) acc.lowConfidence += 1;
      return acc;
    }, { actionable: 0, mismatch: 0, lowConfidence: 0 });
  }

  function reviewQueueCount(rows, reviewAvailable) {
    if (!reviewAvailable) return 0;
    return rows.reduce((count, row) => {
      return count + (row && row.review && row.review.status ? 1 : 0);
    }, 0);
  }

  function cityRowsForReview(payload) {
    const rows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix.slice() : [];
    rows.sort((a, b) => {
      const reviewDiff = reviewPriority((a.review || {}).status) - reviewPriority((b.review || {}).status);
      if (reviewDiff !== 0) return reviewDiff;
      const sortDiff = (a.sort_priority || 0) - (b.sort_priority || 0);
      if (sortDiff !== 0) return sortDiff;
      const assignmentGapDiff = (b.gap_to_assignment ?? -1) - (a.gap_to_assignment ?? -1);
      if (assignmentGapDiff !== 0) return assignmentGapDiff;
      return String(a.series_ticker || "").localeCompare(String(b.series_ticker || ""));
    });
    return rows;
  }

  function selectDefaultReviewCity(payload) {
    const rows = cityRowsForReview(payload);
    if (!rows.length) return null;
    return rows[0] || null;
  }

  const dashboardRoot = document.getElementById("dashboard");
  const dashboardMode = dashboardRoot?.dataset.dashboardMode || "combined";

  function currentDashboardEnv() {
    if (dashboardMode === "single_site") {
      return dashboardRoot?.dataset.activeEnv || "demo";
    }
    return document.querySelector('.dash-tab.is-active[data-tab-mode="local"]')?.dataset.env || dashboardRoot?.dataset.activeEnv || "demo";
  }

  const strategyState = {
    payload: parseBootstrap(),
    windowDays: 180,
    selectedSeriesTicker: null,
    selectedStrategyName: null,
    codexContextSeriesTicker: null,
    codexContextStrategyName: null,
    sortKey: "priority",
    focusMode: "cities",
    searchQuery: "",
    cityFilter: "all",
    codexMode: "evaluate",
    codexProvider: null,
    codexModel: "",
    codexRunId: null,
    codexPrompt: "",
    codexSubmitting: false,
    codexRunDetail: null,
    codexPollTimer: null,
    codexMessage: null,
    fetching: false,
    dirty: false,
    approvalSubmitting: false,
    approvalMessage: null,
    approvalNotes: {},
    explicitSelection: false,
  };

  if (strategyState.payload && strategyState.payload.summary) {
    strategyState.windowDays = strategyState.payload.summary.window_days || 180;
    strategyState.focusMode = "cities";
    if (strategyState.payload.detail_context && strategyState.payload.detail_context.type === "city") {
      strategyState.selectedSeriesTicker = strategyState.payload.detail_context.selected_series_ticker || null;
      strategyState.codexContextSeriesTicker = strategyState.selectedSeriesTicker;
    }
  }

  document.querySelectorAll('.dash-tab[data-tab-mode="local"]').forEach((btn) => {
    btn.addEventListener("click", () => {
      const env = btn.dataset.env;
      if (!env) return;
      document.querySelectorAll(".dash-tab").forEach((b) => {
        b.classList.toggle("is-active", b === btn);
        b.setAttribute("aria-selected", b === btn ? "true" : "false");
      });
      document.querySelectorAll(".dash-panel").forEach((panel) => {
        const active = panel.dataset.env === env;
        panel.classList.toggle("is-active", active);
        panel.hidden = !active;
      });
      if (dashboardRoot) dashboardRoot.dataset.activeEnv = env;
      if (env === "strategies") {
        if (strategyState.dirty || !strategyState.payload) {
          loadStrategies();
        } else {
          renderStrategies(strategyState.payload);
        }
      }
    });
  });

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
      }),
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
      }),
    );
  }

  function renderPositions(card, positions, summary) {
    const header = card.querySelector(".dash-card-header");
    const countLabel = header.querySelector(".positions-count-label");
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
        pnlTd,
      );
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    if (summary && summary.total_value_dollars) {
      const tfoot = el("tfoot", "positions-totals");
      const tr = el("tr");
      const labelTd = el("td", "totals-label", summary.total_value_label || "Total");
      labelTd.setAttribute("colspan", "5");
      const pnlDisplay = summary.total_value_is_marked ? (summary.total_unrealized_pnl_display || "—") : "—";
      const pnlTone = summary.total_value_is_marked ? toneClass(summary.total_unrealized_pnl_tone) : "";
      tr.append(
        labelTd,
        el("td", "mono", summary.total_value_display || "—"),
        el("td", `mono ${pnlTone}`.trim(), pnlDisplay),
      );
      tfoot.appendChild(tr);
      table.appendChild(tfoot);
    }

    tableWrap.replaceChildren(table);
  }

  function renderRecentTradeProposals(card, proposals) {
    if (!card) return;
    const countLabel = card.querySelector(".trade-proposals-count-label");
    if (countLabel) countLabel.textContent = `${proposals.length} ticket${proposals.length !== 1 ? "s" : ""}`;

    const empty = card.querySelector("p.empty-state");
    if (!proposals.length) {
      const wrap = card.querySelector(".table-wrap");
      if (wrap) wrap.remove();
      if (!empty) card.appendChild(el("p", "empty-state", "No recent proposed tickets."));
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
    ["Market", "Side", "Yes Price", "Contracts", "Status", "Risk", "Approved Notional"].forEach((h) => {
      headerRow.appendChild(el("th", null, h));
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = el("tbody");
    proposals.forEach((proposal) => {
      const tr = el("tr");
      const marketTd = el("td", "mono");
      const marketWrap = el("div", "proposal-market-cell");
      marketWrap.appendChild(el("span", null, proposal.market_ticker || "—"));
      if (proposal.updated_at) {
        const timeNode = el("span", "muted-label proposal-updated-at", formatAge(proposal.updated_at));
        timeNode.dataset.timestamp = proposal.updated_at;
        timeNode.title = proposal.updated_at;
        marketWrap.appendChild(timeNode);
      }
      marketTd.appendChild(marketWrap);
      const sideTd = el("td");
      const sideTone = proposal.side_tone || (proposal.side === "yes" ? "good" : proposal.side === "no" ? "warning" : "neutral");
      sideTd.appendChild(el("span", `status-pill ${statusPillClass(sideTone)}`, proposal.side || "—"));
      const statusTd = el("td");
      const statusTone = proposal.status_tone || tradeProposalTone(proposal.status);
      statusTd.appendChild(el("span", `status-pill ${statusPillClass(statusTone)}`, proposal.status || "—"));
      const riskTd = el("td");
      const riskTone = proposal.risk_status_tone || tradeProposalTone(proposal.risk_status);
      const riskWrap = el("div", "proposal-risk-cell");
      riskWrap.appendChild(el("span", `status-pill ${statusPillClass(riskTone)}`, proposal.risk_status || "—"));
      const riskReasonText = proposalRiskReasonText(proposal);
      if (riskReasonText) {
        riskWrap.appendChild(el("div", "proposal-risk-reasons", riskReasonText));
      }
      riskTd.appendChild(riskWrap);
      tr.append(
        marketTd,
        sideTd,
        el("td", "mono", proposal.yes_price_dollars || "—"),
        el("td", "mono", proposal.count_fp || "—"),
        statusTd,
        riskTd,
        el("td", "mono", proposal.approved_notional_dollars || "—"),
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

  async function refreshEnv(env) {
    try {
      const resp = await fetch(`/api/dashboard/${env}`);
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        if (redirectToLoginIfRequired(resp, body)) return;
        return;
      }
      const data = await resp.json();
      const panel = document.querySelector(`.dash-panel[data-env="${env}"]`);
      if (!panel) return;
      renderSummary(panel.querySelector(".dash-summary"), {
        ...(data.portfolio || {}),
        daily_pnl_display: data.daily_pnl_display,
        daily_pnl_line_display: data.daily_pnl_line_display,
        daily_pnl_tone: data.daily_pnl_tone,
      });
      renderActiveRooms(panel.querySelector(".dash-card-alerts"), data.active_rooms || []);
      renderAlerts(panel.querySelector(".dash-card-alerts"), data.alerts || []);
      renderCapitalBuckets(panel.querySelector(".dash-card-positions"), data.positions_summary || {});
      renderPositions(panel.querySelector(".dash-card-positions"), data.positions || [], data.positions_summary || {});
      renderRecentTradeProposals(panel.querySelector(".dash-card-proposals"), data.recent_trade_proposals || []);
    } catch (_) {
      // skip on network error
    }
  }

  function strategyQueryParams(options) {
    const params = new URLSearchParams();
    params.set("window_days", String(options.windowDays || strategyState.windowDays || 180));
    if (options.seriesTicker) params.set("series_ticker", options.seriesTicker);
    if (options.strategyName) params.set("strategy_name", options.strategyName);
    return params.toString();
  }

  function createSummaryStat(label, valueNode, detailNode) {
    const article = el("article", "dash-stat");
    article.append(el("span", "muted-label", label));
    article.append(valueNode);
    if (detailNode) article.append(detailNode);
    return article;
  }

  function strategyStatValue(text, extraClass) {
    return el("strong", ["dash-stat-value", extraClass || ""].join(" ").trim(), text);
  }

  function strategyStatDetail(text, extraClass) {
    return el("span", ["dash-stat-detail", extraClass || "", "muted-label"].join(" ").trim(), text);
  }

  function recommendationTone(status) {
    if (status === "strong_recommendation") return "good";
    if (status === "lean_recommendation") return "warning";
    if (status === "too_close") return "warning";
    if (status === "low_sample") return "warning";
    return "neutral";
  }

  function reviewTone(status) {
    if (status === "ready_for_approval") return "good";
    if (status === "drifted_assignment") return "warning";
    if (status === "evidence_weakened") return "warning";
    if (status === "aligned") return "neutral";
    if (status === "waiting_for_evidence") return "neutral";
    return "neutral";
  }

  function recommendationModeLabel(mode, summary) {
    if (mode === "recommendation_only" && summary && summary.manual_approval_enabled) return "Manual approval";
    if (mode === "recommendation_only") return "Recommendation only";
    return mode || "—";
  }

  function setApprovalMessage(seriesTicker, tone, text) {
    strategyState.approvalMessage = seriesTicker && text ? { seriesTicker, tone: tone || "neutral", text } : null;
  }

  function approvalMessageNode(seriesTicker) {
    const message = strategyState.approvalMessage;
    if (!message || message.seriesTicker !== seriesTicker) return null;
    const node = el(
      "div",
      `strategy-approval-message is-${message.tone || "neutral"}`,
      message.text,
    );
    node.dataset.seriesTicker = seriesTicker;
    return setTestId(node, "strategy-approval-message");
  }

  function responseErrorMessage(body, fallbackText) {
    if (body && typeof body.message === "string" && body.message) return body.message;
    if (body && Array.isArray(body.detail) && body.detail.length) {
      const first = body.detail[0];
      if (first && typeof first.msg === "string") return first.msg;
    }
    return fallbackText;
  }

  function currentPageLoginPath() {
    const path = `${window.location.pathname || "/"}${window.location.search || ""}${window.location.hash || ""}`;
    return path && !path.startsWith("/login") ? path : "/";
  }

  function fallbackLoginUrl() {
    return `/login?next=${encodeURIComponent(currentPageLoginPath())}`;
  }

  function authRequiredLoginUrl(body) {
    const fallbackUrl = fallbackLoginUrl();
    if (!body || body.error !== "auth_required") return fallbackUrl;
    if (typeof body.login_url !== "string" || !body.login_url) return fallbackUrl;
    try {
      const url = new URL(body.login_url, window.location.origin);
      if (url.origin !== window.location.origin || url.pathname !== "/login") return fallbackUrl;
      const nextPath = url.searchParams.get("next");
      if (!nextPath || nextPath.startsWith("/api/")) {
        url.searchParams.set("next", currentPageLoginPath());
      }
      return `${url.pathname}${url.search}${url.hash}`;
    } catch (_) {
      return fallbackUrl;
    }
  }

  function redirectToLoginIfRequired(response, body) {
    if (!response || response.status !== 401) return false;
    const loginUrl = authRequiredLoginUrl(body);
    window.location.assign(loginUrl);
    return true;
  }

  function renderStrategiesSummary(payload) {
    const container = document.getElementById("strategies-summary");
    if (!container) return;
    const summary = payload && payload.summary ? payload.summary : {};
    const rows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix : [];
    const counts = analysisSummaryCounts(rows);
    const windowValue = strategyStatValue(summary.window_display || "—");
    const windowDetail = strategyStatDetail(summary.source_mode === "live_eval" ? "Live replay evaluation" : "Stored regression snapshot");

    const lastValue = strategyStatValue("—");
    if (summary.last_regression_run) {
      const ts = el("span", null, formatAge(summary.last_regression_run));
      ts.dataset.timestamp = summary.last_regression_run;
      lastValue.textContent = "";
      lastValue.appendChild(ts);
    }
    const lastDetail = strategyStatDetail("Most recent stored regression");

    const citiesValue = strategyStatValue(String(rows.length || 0));
    const citiesDetail = strategyStatDetail("Cities evaluated in the active window");

    const actionableValue = strategyStatValue(String(counts.actionable || 0), counts.actionable > 0 ? "value-positive" : "");
    const actionableDetail = strategyStatDetail("Strong or lean recommendations");

    const mismatchValue = strategyStatValue(String(counts.mismatch || 0), counts.mismatch > 0 ? "value-negative" : "");
    const mismatchDetail = strategyStatDetail("Assignments that differ from the latest recommendation");

    const lowConfidenceValue = strategyStatValue(String(counts.lowConfidence || 0), counts.lowConfidence > 0 ? "value-negative" : "");
    const lowConfidenceDetail = strategyStatDetail("Cities still blocked by sample size or runner-up proximity");

    const bestValue = strategyStatValue(summary.best_strategy_name || "—");
    const bestDetail = strategyStatDetail(summary.best_strategy_win_rate_display || "Top overall win rate");

    const assignValue = strategyStatValue(summary.assignments_covered_display || "—");
    const assignDetail = strategyStatDetail("Canonical assignments covered");

    const stats = [
      createSummaryStat("Window", windowValue, windowDetail),
      createSummaryStat("Last Regression Run", lastValue, lastDetail),
      createSummaryStat("Cities Evaluated", citiesValue, citiesDetail),
      createSummaryStat("Actionable Cities", actionableValue, actionableDetail),
      createSummaryStat("Assignment Mismatches", mismatchValue, mismatchDetail),
      createSummaryStat("Low-Confidence Cities", lowConfidenceValue, lowConfidenceDetail),
      createSummaryStat("Best Strategy", bestValue, bestDetail),
      createSummaryStat("Assignments Covered", assignValue, assignDetail),
    ];

    clearNode(container, stats);
  }

  function renderWindowFilter(summary) {
    const container = document.getElementById("strategies-window-filter");
    if (!container) return;
    const buttons = (summary.window_options || [30, 90, 180]).map((windowDays) => {
      const button = el("button", `strategy-filter-pill${windowDays === summary.window_days ? " is-active" : ""}`, `${windowDays}d`);
      button.type = "button";
      button.dataset.windowDays = String(windowDays);
      button.setAttribute("aria-selected", windowDays === summary.window_days ? "true" : "false");
      button.addEventListener("click", () => {
        if (strategyState.fetching || windowDays === strategyState.windowDays) return;
        loadStrategies({
          windowDays,
          focusMode: strategyState.focusMode,
          explicitSelection: strategyState.explicitSelection,
        });
      });
      return button;
    });
    clearNode(container, buttons);
  }

  function renderFocusPanels(summary) {
    const focusMode = normalizeStrategyFocusMode(strategyState.focusMode, summary || {});
    strategyState.focusMode = focusMode;
    const review = document.getElementById("strategies-focus-review");
    const cities = document.getElementById("strategies-focus-cities");
    const strategies = document.getElementById("strategies-focus-strategies");
    if (review) review.hidden = focusMode !== "review";
    if (cities) cities.hidden = focusMode !== "cities";
    if (strategies) strategies.hidden = focusMode !== "strategies";
  }

  function renderFocusSwitch(payload) {
    const container = document.getElementById("strategies-focus-switch");
    if (!container) return;
    const summary = payload && payload.summary ? payload.summary : {};
    const allowedModes = summary.review_available
      ? STRATEGY_FOCUS_MODES
      : STRATEGY_FOCUS_MODES.filter((mode) => mode !== "review");
    strategyState.focusMode = normalizeStrategyFocusMode(strategyState.focusMode, summary || {});
    const queueCount = reviewQueueCount(Array.isArray(payload && payload.city_matrix) ? payload.city_matrix : [], summary.review_available);
    const buttons = allowedModes.map((mode) => {
      const button = el(
        "button",
        `strategy-focus-pill${mode === strategyState.focusMode ? " is-active" : ""}`,
        strategyFocusLabel(mode, queueCount),
      );
      button.type = "button";
      button.dataset.focusMode = mode;
      button.setAttribute("aria-selected", mode === strategyState.focusMode ? "true" : "false");
      button.addEventListener("click", () => {
        if (strategyState.fetching) return;
        strategyState.focusMode = mode;
        renderStrategies(strategyState.payload);
      });
      return button;
    });
    clearNode(container, buttons);
  }

  function syncStrategySelections(payload) {
    const detail = payload && payload.detail_context ? payload.detail_context : {};
    if (detail.type === "city" && detail.selected_series_ticker) {
      strategyState.selectedSeriesTicker = detail.selected_series_ticker;
      if (!strategyState.codexContextSeriesTicker) {
        strategyState.codexContextSeriesTicker = detail.selected_series_ticker;
      }
      return;
    }
    if (detail.type === "strategy" && detail.selected_strategy_name) {
      if (strategyState.focusMode === "strategies" || strategyState.explicitSelection) {
        strategyState.selectedStrategyName = detail.selected_strategy_name;
      }
      if (!strategyState.codexContextStrategyName) {
        strategyState.codexContextStrategyName = detail.selected_strategy_name;
      }
    }
  }

  function setDetailLoadingState(metaId, containerId, metaText, loadingText) {
    const meta = document.getElementById(metaId);
    const container = document.getElementById(containerId);
    if (meta) meta.textContent = metaText;
    if (container) clearNode(container, [el("p", "empty-state", loadingText)]);
  }

  function selectCitiesDetailTarget(rows) {
    if (!rows.length) return null;
    return rows.find((row) => row.series_ticker === strategyState.selectedSeriesTicker) || rows[0] || null;
  }

  function selectStrategyDetailTarget(payload) {
    const leaderboard = Array.isArray(payload && payload.leaderboard) ? payload.leaderboard : [];
    if (!leaderboard.length) return null;
    return leaderboard.find((row) => row.name === strategyState.selectedStrategyName)
      || leaderboard.find((row) => row.name === (payload.summary || {}).best_strategy_name)
      || leaderboard[0]
      || null;
  }

  function ensureCitiesDetail(payload, rows) {
    if (strategyState.focusMode !== "cities") return false;
    const targetRow = selectCitiesDetailTarget(rows);
    if (!targetRow || !targetRow.series_ticker) {
      strategyState.selectedSeriesTicker = null;
      return false;
    }
    const detail = payload && payload.detail_context ? payload.detail_context : {};
    if (strategyState.selectedSeriesTicker !== targetRow.series_ticker) {
      strategyState.selectedSeriesTicker = targetRow.series_ticker;
    }
    if (detail.type === "city" && detail.selected_series_ticker === targetRow.series_ticker) {
      return false;
    }
    setDetailLoadingState(
      "strategies-cities-detail-meta",
      "strategies-cities-detail",
      `Loading ${targetRow.series_ticker} city research brief...`,
      "Loading city research brief...",
    );
    loadStrategies({
      windowDays: strategyState.windowDays,
      seriesTicker: targetRow.series_ticker,
      strategyName: null,
      focusMode: "cities",
      explicitSelection: false,
    });
    return true;
  }

  function ensureReviewDetail(payload) {
    if (!payload || !payload.summary || !payload.summary.review_available) return false;
    if (strategyState.focusMode !== "review") return false;
    const reviewRows = cityRowsForReview(payload);
    const targetRow = reviewRows.find((row) => row.series_ticker === strategyState.selectedSeriesTicker) || selectDefaultReviewCity(payload);
    if (!targetRow || !targetRow.series_ticker) return false;
    const detail = payload && payload.detail_context ? payload.detail_context : {};
    if (strategyState.selectedSeriesTicker !== targetRow.series_ticker) {
      strategyState.selectedSeriesTicker = targetRow.series_ticker;
    }
    if (detail.type === "city" && detail.selected_series_ticker === targetRow.series_ticker) {
      return false;
    }
    setDetailLoadingState(
      "strategies-review-detail-meta",
      "strategies-review-detail",
      `Loading ${targetRow.series_ticker} decision brief...`,
      "Loading decision brief...",
    );
    loadStrategies({
      windowDays: strategyState.windowDays,
      seriesTicker: targetRow.series_ticker,
      strategyName: null,
      focusMode: "review",
      explicitSelection: false,
    });
    return true;
  }

  function ensureStrategiesDetail(payload) {
    if (strategyState.focusMode !== "strategies") return false;
    const targetStrategy = selectStrategyDetailTarget(payload);
    if (!targetStrategy || !targetStrategy.name) {
      strategyState.selectedStrategyName = null;
      return false;
    }
    const detail = payload && payload.detail_context ? payload.detail_context : {};
    if (strategyState.selectedStrategyName !== targetStrategy.name) {
      strategyState.selectedStrategyName = targetStrategy.name;
    }
    if (detail.type === "strategy" && detail.selected_strategy_name === targetStrategy.name) {
      return false;
    }
    setDetailLoadingState(
      "strategies-detail-meta",
      "strategies-detail",
      `Loading ${targetStrategy.name} strategy drilldown...`,
      "Loading strategy drilldown...",
    );
    loadStrategies({
      windowDays: strategyState.windowDays,
      strategyName: targetStrategy.name,
      seriesTicker: null,
      focusMode: "strategies",
      explicitSelection: Boolean(strategyState.explicitSelection),
    });
    return true;
  }

  function ensureFocusDetailSelection(payload, rows) {
    if (strategyState.focusMode === "cities") return ensureCitiesDetail(payload, rows);
    if (strategyState.focusMode === "review") return ensureReviewDetail(payload);
    return ensureStrategiesDetail(payload);
  }

  function metricListItem(label, value, extraClass) {
    const row = el("div", "strategy-metric-item");
    row.append(el("span", "muted-label", label));
    row.append(el("strong", extraClass || "", value));
    return row;
  }

  function renderReviewQueue(payload) {
    const card = document.getElementById("strategies-review-queue-card");
    const container = document.getElementById("strategies-review-queue");
    const meta = document.getElementById("strategies-review-queue-meta");
    if (!card || !container) return;

    const summary = payload && payload.summary ? payload.summary : {};
    if (!summary.review_available) {
      card.hidden = true;
      clearNode(container, []);
      if (meta) meta.textContent = "The latest 180d assignment review workflow";
      return;
    }

    const rows = Array.isArray(payload.city_matrix) ? payload.city_matrix : [];
    const groups = [
      { status: "drifted_assignment", title: "Needs review: drifted assignment" },
      { status: "ready_for_approval", title: "Ready for approval" },
      { status: "evidence_weakened", title: "Needs review: weakened evidence" },
      { status: "aligned", title: "Aligned assignments" },
      { status: "waiting_for_evidence", title: "Waiting for evidence" },
    ];
    const sections = [];
    let totalQueued = 0;

    groups.forEach((group) => {
      const groupRows = rows.filter((row) => row.review && row.review.status === group.status);
      if (!groupRows.length) return;
      totalQueued += groupRows.length;

      const section = el("section", "strategy-review-group");
      const header = el("div", "strategy-review-group-header");
      header.append(el("h3", null, group.title), pill(String(groupRows.length), reviewTone(group.status)));
      section.appendChild(header);

      const list = el("div", "strategy-review-list");
      groupRows.forEach((row) => {
        const review = row.review || {};
        const recommendation = row.recommendation || {};
        const item = el("button", "strategy-review-item");
        item.type = "button";
        item.dataset.seriesTicker = row.series_ticker;
        item.addEventListener("click", () => {
          if (strategyState.fetching) return;
          loadStrategies({
            windowDays: strategyState.windowDays,
            seriesTicker: row.series_ticker,
            strategyName: null,
            focusMode: "review",
            explicitSelection: true,
          });
        });

        const top = el("div", "strategy-review-top");
        const title = el("div", "strategy-review-title");
        title.append(
          el("strong", "mono", row.series_ticker),
          el("span", "muted-label", row.city_label || row.location_name || row.series_ticker),
        );
        const pills = el("div", "strategy-review-pills");
        pills.append(
          row.assignment && row.assignment.strategy_name ? pill(`Assigned ${row.assignment.strategy_name}`, "neutral") : pill("Unassigned", "neutral"),
          recommendation.strategy_name ? pill(`Latest ${recommendation.strategy_name}`, recommendationTone(recommendation.status)) : pill("No winner", "neutral"),
          pill(review.label || "Review", reviewTone(review.status)),
        );
        if (recommendation.label) pills.append(pill(recommendation.label, recommendationTone(recommendation.status)));
        top.append(title, pills);

        const metrics = el("div", "strategy-review-metrics");
        metrics.append(
          el("span", "mono", `Gap ${row.gap_to_runner_up_display || "—"}`),
          el("span", "mono", `Resolved ${recommendation.resolved_trade_count_display || "0"}`),
          el("span", "mono", row.best_outcome_coverage_display || "Coverage —"),
        );
        if (row.assignment && row.assignment.assigned_at) {
          const ts = el("span", "muted-label");
          ts.dataset.timestamp = row.assignment.assigned_at;
          ts.textContent = `Assigned ${formatAge(row.assignment.assigned_at)}`;
          metrics.appendChild(ts);
        }

        item.append(top, metrics);
        list.appendChild(item);
      });

      section.appendChild(list);
      sections.push(section);
    });

    card.hidden = false;
    if (meta) {
      meta.textContent = totalQueued
        ? `${totalQueued} cities grouped by the latest ${summary.review_window_days || 180}d assignment review state`
        : `No cities are currently queued from the latest ${summary.review_window_days || 180}d review snapshot`;
    }
    clearNode(container, sections.length ? sections : [el("p", "empty-state", "No 180d assignment review work is queued right now.")]);
  }

  function renderThresholdGroups(groups) {
    const wrap = el("div", "strategy-threshold-groups");
    (groups || []).forEach((group) => {
      const section = el("section", "strategy-threshold-group");
      section.append(el("h4", null, group.label));
      const list = el("dl", "strategy-threshold-list");
      (group.items || []).forEach((item) => {
        list.append(el("dt", "muted-label", item.label), el("dd", "mono", item.value));
      });
      section.appendChild(list);
      wrap.appendChild(section);
    });
    return wrap;
  }

  function renderStrategyLeaderboard(leaderboard) {
    const container = document.getElementById("strategies-leaderboard");
    const meta = document.getElementById("strategies-leaderboard-meta");
    if (!container) return;
    if (!Array.isArray(leaderboard) || !leaderboard.length) {
      clearNode(container, [el("p", "empty-state", "No strategy results yet. Run regression after historical replay rooms are available.")]);
      if (meta) meta.textContent = "No leaderboard data yet";
      return;
    }

    if (meta) {
      meta.textContent = `${leaderboard.length} presets compared across the selected window`;
    }

    const cards = leaderboard.map((item) => {
      const isSelected = item.name === strategyState.selectedStrategyName || (!strategyState.selectedStrategyName && item.selected);
      const card = el("article", `strategy-card${isSelected ? " is-selected" : ""}`);
      const header = el("div", "strategy-card-header");
      const titleWrap = el("div", "strategy-card-title");
      const selectButton = el("button", "strategy-select-button");
      selectButton.type = "button";
      selectButton.dataset.strategyName = item.name;
      selectButton.textContent = item.name;
      selectButton.addEventListener("click", () => {
        if (strategyState.fetching) return;
        loadStrategies({
          windowDays: strategyState.windowDays,
          strategyName: item.name,
          seriesTicker: null,
          focusMode: "strategies",
          explicitSelection: true,
        });
      });
      titleWrap.append(selectButton);
      if (item.description) {
        titleWrap.append(el("p", "muted-label strategy-card-description", item.description));
      }
      header.append(titleWrap);

      const primaryMetrics = el("div", "strategy-card-primary-metrics");
      primaryMetrics.append(
        metricListItem("Win rate", item.overall_win_rate_display || "—", item.overall_win_rate >= 0.6 ? "value-positive" : item.overall_win_rate <= 0.35 ? "value-negative" : ""),
        metricListItem("Cities led", String(item.cities_led || 0)),
        metricListItem("Assigned cities", String(item.assigned_city_count || 0)),
        metricListItem("Outcome coverage", item.outcome_coverage_display || "—"),
        metricListItem("P/L", item.total_pnl_display || "—", item.total_pnl_dollars > 0 ? "value-positive" : item.total_pnl_dollars < 0 ? "value-negative" : ""),
      );

      const metrics = el("div", "strategy-card-metrics strategy-card-secondary-metrics");
      metrics.append(
        metricListItem("Trade rate", item.overall_trade_rate_display || "—"),
        metricListItem("Avg edge", item.avg_edge_bps_display || "—"),
        metricListItem("Rooms", item.total_rooms_evaluated_display || "0"),
        metricListItem("Sim trades", item.total_trade_count_display || "0"),
        metricListItem("Scored trades", item.total_resolved_trade_count_display || "0"),
        metricListItem("Unscored", item.total_unscored_trade_count_display || "0"),
      );

      const details = el("details", "strategy-threshold-details");
      const summary = el("summary", "muted-label", "Threshold snapshot");
      details.append(summary, renderThresholdGroups(item.threshold_groups || []));

      card.append(header, primaryMetrics, metrics, details);
      return card;
    });
    clearNode(container, [el("div", "strategy-card-grid")]);
    container.firstChild.replaceChildren(...cards);
  }

  function codexLabPayload(payload) {
    return payload && payload.codex_lab ? payload.codex_lab : {};
  }

  function codexProviderOptions(payload) {
    const lab = codexLabPayload(payload);
    return Array.isArray(lab.provider_options) ? lab.provider_options : [];
  }

  function codexProviderLabel(providerId, payload) {
    const option = codexProviderOptions(payload).find((item) => item.id === providerId);
    if (option && option.label) return option.label;
    if (providerId === "gemini") return "Gemini";
    if (providerId === "codex") return "Codex";
    if (providerId === "hosted") return "Hosted";
    return providerId || "AI";
  }

  function syncCodexSelection(payload, options) {
    const providerOptions = codexProviderOptions(payload);
    if (!providerOptions.length) {
      strategyState.codexProvider = null;
      if (options && options.resetModel) strategyState.codexModel = "";
      return null;
    }
    const requestedProvider = strategyState.codexProvider;
    const selectedProvider = providerOptions.find((item) => item.id === requestedProvider)
      || providerOptions.find((item) => item.id === codexLabPayload(payload).provider)
      || providerOptions[0];
    const providerChanged = strategyState.codexProvider !== selectedProvider.id;
    strategyState.codexProvider = selectedProvider.id;
    if (providerChanged || !strategyState.codexModel || (options && options.resetModel)) {
      strategyState.codexModel = selectedProvider.default_model || "";
    }
    return selectedProvider;
  }

  function codexSuggestedModels(provider) {
    const providerId = provider && provider.id ? String(provider.id || "").trim().toLowerCase() : "";
    const rawModels = [
      ...(provider && Array.isArray(provider.suggested_models) ? provider.suggested_models : []),
      provider && provider.default_model ? provider.default_model : null,
      ...((providerId && CODEX_PROVIDER_MODEL_FALLBACKS[providerId]) || []),
    ];
    const seen = new Set();
    const ordered = [];
    rawModels.forEach((modelName) => {
      const cleaned = String(modelName || "").trim();
      if (!cleaned || seen.has(cleaned)) return;
      seen.add(cleaned);
      ordered.push(cleaned);
    });
    return ordered;
  }

  function humanizeThresholdKey(key) {
    return String(key || "")
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  function thresholdGroupsFromObject(thresholds) {
    const groups = {
      risk: { label: "Risk", items: [] },
      trigger: { label: "Trigger", items: [] },
      strategy: { label: "Strategy", items: [] },
    };
    Object.entries(thresholds || {}).forEach(([key, value]) => {
      const groupKey = key.startsWith("risk_") ? "risk" : key.startsWith("trigger_") ? "trigger" : "strategy";
      groups[groupKey].items.push({ label: humanizeThresholdKey(key), value: String(value) });
    });
    return Object.values(groups).filter((group) => group.items.length);
  }

  function compactCodexRun(run) {
    if (!run) return null;
    return {
      id: run.id,
      mode: run.mode,
      status: run.status,
      trigger_source: run.trigger_source || "manual",
      window_days: run.window_days,
      series_ticker: run.series_ticker,
      strategy_name: run.strategy_name,
      provider: run.provider || null,
      model: run.model || null,
      created_at: run.created_at,
      updated_at: run.updated_at,
      summary: run.result && run.result.kind === "evaluate"
        ? ((run.result.evaluation || {}).summary || null)
        : run.result && run.result.kind === "suggest"
        ? (run.saved_strategy_name ? `Saved as inactive preset ${run.saved_strategy_name}.` : ((run.result.backtest || {}).summary || null))
        : null,
      saved_strategy_name: run.saved_strategy_name || null,
    };
  }

  function codexTriggerSourceLabel(triggerSource) {
    return triggerSource === "nightly" ? "Nightly" : "Manual";
  }

  function upsertInactiveCodexStrategy(run) {
    if (!strategyState.payload || !strategyState.payload.codex_lab || !run || !run.saved_strategy_name || run.saved_strategy_active) return;
    const codexLab = strategyState.payload.codex_lab;
    const existing = Array.isArray(codexLab.inactive_codex_strategies) ? codexLab.inactive_codex_strategies.slice() : [];
    if (existing.some((item) => item.name === run.saved_strategy_name)) return;
    const candidate = run.result && run.result.candidate ? run.result.candidate : {};
    existing.unshift({
      name: run.saved_strategy_name,
      description: candidate.description || null,
      created_at: run.finished_at || run.updated_at || run.created_at,
      labels: candidate.labels || [],
      rationale: candidate.rationale || null,
      source_run_id: run.id,
    });
    codexLab.inactive_codex_strategies = existing;
  }

  function mergeCodexRunIntoPayload(run) {
    if (!strategyState.payload || !run) return;
    if (!strategyState.payload.codex_lab) {
      strategyState.payload.codex_lab = {
        available: false,
        provider: "unavailable",
        model: null,
        provider_options: [],
        recent_runs: [],
        inactive_codex_strategies: [],
        creation_window_days: 180,
      };
    }
    const codexLab = strategyState.payload.codex_lab;
    const nextRuns = Array.isArray(codexLab.recent_runs) ? codexLab.recent_runs.slice() : [];
    const compact = compactCodexRun(run);
    const index = nextRuns.findIndex((item) => item.id === compact.id);
    if (index >= 0) nextRuns[index] = compact;
    else nextRuns.unshift(compact);
    codexLab.recent_runs = nextRuns.slice(0, 8);
    upsertInactiveCodexStrategy(run);
  }

  function stopCodexRunPolling() {
    if (strategyState.codexPollTimer) {
      window.clearTimeout(strategyState.codexPollTimer);
      strategyState.codexPollTimer = null;
    }
  }

  function scheduleCodexRunPolling(runId) {
    stopCodexRunPolling();
    strategyState.codexPollTimer = window.setTimeout(() => {
      void fetchCodexRunDetail(runId, { refreshAfterComplete: true });
    }, 2000);
  }

  async function fetchCodexRunDetail(runId, options) {
    if (!runId) return;
    try {
      const response = await fetch(`/api/strategies/codex/runs/${encodeURIComponent(runId)}`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        if (redirectToLoginIfRequired(response, body)) return;
        return;
      }
      const body = await response.json();
      strategyState.codexRunId = body.id || runId;
      strategyState.codexRunDetail = body;
      mergeCodexRunIntoPayload(body);
      if (body.status === "queued" || body.status === "running") {
        scheduleCodexRunPolling(body.id || runId);
      } else {
        stopCodexRunPolling();
        if (options && options.refreshAfterComplete && strategyState.payload) {
          await loadStrategies({
            windowDays: strategyState.windowDays,
            focusMode: strategyState.focusMode,
            explicitSelection: strategyState.explicitSelection,
          });
        } else if (strategyState.payload) {
          renderStrategies(strategyState.payload);
        }
      }
    } catch (_) {
      // skip transient polling failures
    }
  }

  function showCodexMessage(tone, text) {
    strategyState.codexMessage = text ? { tone: tone || "neutral", text } : null;
  }

  async function startCodexRun() {
    const payload = strategyState.payload;
    const codexLab = codexLabPayload(payload);
    if (!payload || !codexLab.available || strategyState.codexSubmitting) return;
    strategyState.codexSubmitting = true;
    showCodexMessage("neutral", strategyState.codexMode === "suggest" ? "Starting suggestion run..." : "Starting evaluation run...");
    if (strategyState.payload) renderStrategies(strategyState.payload);
    try {
      const response = await fetch("/api/strategies/codex/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: strategyState.codexMode,
          window_days: strategyState.windowDays,
          series_ticker: strategyState.codexContextSeriesTicker,
          strategy_name: strategyState.codexContextStrategyName,
          operator_brief: (strategyState.codexPrompt || "").trim() || null,
          provider: strategyState.codexProvider || null,
          model: (strategyState.codexModel || "").trim() || null,
        }),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        if (redirectToLoginIfRequired(response, body)) return;
        showCodexMessage(
          "bad",
          responseErrorMessage(body, `Strategy lab run failed to start (HTTP ${response.status}).`),
        );
        return;
      }
      if (!body.run_id) {
        showCodexMessage("bad", "Strategy lab run failed to start because the server returned an incomplete response.");
        return;
      }
      strategyState.codexRunId = body.run_id || null;
      strategyState.codexRunDetail = null;
      showCodexMessage("good", "Strategy lab run queued.");
      await loadStrategies({
        windowDays: strategyState.windowDays,
        focusMode: "strategies",
        explicitSelection: strategyState.explicitSelection,
      });
      await fetchCodexRunDetail(body.run_id, { refreshAfterComplete: true });
    } catch (error) {
      showCodexMessage(
        "bad",
        error instanceof Error && error.message
          ? `Strategy lab run failed to start. ${error.message}`
          : "Strategy lab run failed to start.",
      );
    } finally {
      strategyState.codexSubmitting = false;
      if (strategyState.payload) renderStrategies(strategyState.payload);
    }
  }

  async function acceptCodexSuggestion(runId) {
    if (!runId || strategyState.codexSubmitting) return;
    strategyState.codexSubmitting = true;
    showCodexMessage("neutral", "Saving suggested strategy...");
    if (strategyState.payload) renderStrategies(strategyState.payload);
    try {
      const response = await fetch(`/api/strategies/codex/runs/${encodeURIComponent(runId)}/accept`, { method: "POST" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        if (redirectToLoginIfRequired(response, body)) return;
        showCodexMessage("bad", responseErrorMessage(body, "Strategy suggestion could not be saved."));
        return;
      }
      showCodexMessage("good", `Saved ${body.strategy_name} as an inactive preset.`);
      await loadStrategies({
        windowDays: strategyState.windowDays,
        focusMode: "strategies",
        explicitSelection: strategyState.explicitSelection,
      });
      await fetchCodexRunDetail(runId);
    } catch (_) {
      showCodexMessage("bad", "Strategy suggestion could not be saved.");
    } finally {
      strategyState.codexSubmitting = false;
      if (strategyState.payload) renderStrategies(strategyState.payload);
    }
  }

  async function activateCodexStrategy(strategyName) {
    if (!strategyName || strategyState.codexSubmitting) return;
    strategyState.codexSubmitting = true;
    showCodexMessage("neutral", `Activating ${strategyName}...`);
    if (strategyState.payload) renderStrategies(strategyState.payload);
    try {
      const response = await fetch(`/api/strategies/${encodeURIComponent(strategyName)}/activate`, { method: "POST" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        if (redirectToLoginIfRequired(response, body)) return;
        showCodexMessage("bad", responseErrorMessage(body, "Strategy activation failed."));
        return;
      }
      showCodexMessage("good", `${body.strategy_name} is now active.`);
      await loadStrategies({
        windowDays: strategyState.windowDays,
        focusMode: "strategies",
        explicitSelection: strategyState.explicitSelection,
      });
      if (strategyState.codexRunId) await fetchCodexRunDetail(strategyState.codexRunId);
    } catch (_) {
      showCodexMessage("bad", "Strategy activation failed.");
    } finally {
      strategyState.codexSubmitting = false;
      if (strategyState.payload) renderStrategies(strategyState.payload);
    }
  }

  function openCodexLabContext(options) {
    if (options && Object.prototype.hasOwnProperty.call(options, "seriesTicker")) {
      strategyState.codexContextSeriesTicker = options.seriesTicker || null;
    }
    if (options && Object.prototype.hasOwnProperty.call(options, "strategyName")) {
      strategyState.codexContextStrategyName = options.strategyName || null;
    }
    if (options && Object.prototype.hasOwnProperty.call(options, "mode")) {
      strategyState.codexMode = options.mode || strategyState.codexMode;
    }
    strategyState.focusMode = "strategies";
    const strategiesTab = document.querySelector('.dash-tab[data-env="strategies"]');
    if (strategiesTab && currentDashboardEnv() !== "strategies") {
      strategiesTab.click();
    } else if (strategyState.payload) {
      renderStrategies(strategyState.payload);
    }
    const node = document.getElementById("strategies-codex-lab");
    if (node) node.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function renderCodexLab(payload) {
    const container = document.getElementById("strategies-codex-lab");
    const meta = document.getElementById("strategies-codex-meta");
    if (!container) return;
    const lab = codexLabPayload(payload);
    const activeProvider = syncCodexSelection(payload);
    const recentRuns = Array.isArray(lab.recent_runs) ? lab.recent_runs : [];
    const inactiveStrategies = Array.isArray(lab.inactive_codex_strategies) ? lab.inactive_codex_strategies : [];
    const selectedRun = strategyState.codexRunDetail && strategyState.codexRunDetail.id === strategyState.codexRunId
      ? strategyState.codexRunDetail
      : null;
    const activeProviderLabel = activeProvider ? activeProvider.label || codexProviderLabel(activeProvider.id, payload) : "AI";
    const readyLabel = `${activeProviderLabel} ready`;
    if (meta) {
      meta.textContent = lab.available
        ? `Using ${activeProviderLabel}${strategyState.codexModel ? ` · ${strategyState.codexModel}` : ""} for async evaluation and suggestion runs.`
        : "The strategy lab is unavailable in this environment. The rest of the Strategies page still works normally.";
    }

    const children = [];
    const controls = el("section", "strategy-codex-controls");
    const topRow = el("div", "strategy-codex-top-row");
    const availability = el("div", "strategy-codex-availability");
    availability.appendChild(
      pill(lab.available ? readyLabel : "Strategy lab unavailable", lab.available ? "good" : "bad"),
    );
    if (strategyState.codexModel) availability.appendChild(el("span", "muted-label mono", strategyState.codexModel));
    topRow.appendChild(availability);

    const modeSwitch = el("div", "strategy-codex-mode-switch");
    ["evaluate", "suggest"].forEach((mode) => {
      const button = el("button", `strategy-filter-pill${strategyState.codexMode === mode ? " is-active" : ""}`, mode === "evaluate" ? "Evaluate" : "Suggest");
      button.type = "button";
      button.disabled = strategyState.codexSubmitting;
      button.addEventListener("click", () => {
        if (strategyState.codexSubmitting || strategyState.codexMode === mode) return;
        strategyState.codexMode = mode;
        if (strategyState.payload) renderStrategies(strategyState.payload);
      });
      modeSwitch.appendChild(button);
    });
    topRow.appendChild(modeSwitch);
    controls.appendChild(topRow);

    const contextRow = el("div", "strategy-codex-context-row");
    contextRow.append(
      pill(`Window ${payload && payload.summary ? payload.summary.window_display || `${strategyState.windowDays}d` : `${strategyState.windowDays}d`}`, "neutral"),
      strategyState.codexContextSeriesTicker ? pill(`City ${strategyState.codexContextSeriesTicker}`, "neutral") : pill("No city context", "neutral"),
      strategyState.codexContextStrategyName ? pill(`Strategy ${strategyState.codexContextStrategyName}`, "neutral") : pill("No strategy context", "neutral"),
    );
    const clearContextButton = el("button", "strategy-inline-action", "Clear context");
    clearContextButton.type = "button";
    clearContextButton.disabled = strategyState.codexSubmitting;
    clearContextButton.addEventListener("click", () => {
      strategyState.codexContextSeriesTicker = null;
      strategyState.codexContextStrategyName = null;
      if (strategyState.payload) renderStrategies(strategyState.payload);
    });
    contextRow.appendChild(clearContextButton);
    controls.appendChild(contextRow);

    const providerRow = el("div", "strategy-codex-provider-row");
    const providerBlock = el("div", "strategy-codex-provider-block");
    const providerLabel = el("label", "muted-label", "Provider");
    providerLabel.setAttribute("for", "strategies-codex-provider");
    const providerSelect = setTestId(el("select", "strategy-codex-select"), "strategy-codex-provider");
    providerSelect.id = "strategies-codex-provider";
    providerSelect.disabled = !lab.available || strategyState.codexSubmitting;
    codexProviderOptions(payload).forEach((option) => {
      const optionNode = document.createElement("option");
      optionNode.value = option.id;
      optionNode.textContent = option.label || codexProviderLabel(option.id, payload);
      optionNode.selected = option.id === strategyState.codexProvider;
      providerSelect.appendChild(optionNode);
    });
    providerSelect.addEventListener("change", () => {
      strategyState.codexProvider = providerSelect.value || null;
      syncCodexSelection(payload, { resetModel: true });
      if (strategyState.payload) renderStrategies(strategyState.payload);
    });
    providerBlock.append(providerLabel, providerSelect);
    providerRow.appendChild(providerBlock);

    const modelBlock = el("div", "strategy-codex-provider-block");
    const modelLabel = el("label", "muted-label", "Model");
    modelLabel.setAttribute("for", "strategies-codex-model");
    const suggestedModels = codexSuggestedModels(activeProvider);
    const modelOptions = (
      strategyState.codexModel && !suggestedModels.includes(strategyState.codexModel)
        ? [strategyState.codexModel, ...suggestedModels]
        : suggestedModels
    );
    const modelInput = setTestId(el("input", "strategy-codex-model-input"), "strategy-codex-model");
    modelInput.id = "strategies-codex-model";
    modelInput.type = "text";
    modelInput.autocomplete = "off";
    modelInput.spellcheck = false;
    modelInput.disabled = !lab.available || strategyState.codexSubmitting;
    modelInput.value = strategyState.codexModel || "";
    modelInput.placeholder = activeProvider && activeProvider.default_model ? activeProvider.default_model : "Enter model id";
    modelInput.addEventListener("input", () => {
      strategyState.codexModel = modelInput.value || "";
    });
    if (modelOptions.length) {
      modelInput.setAttribute("list", "strategies-codex-model-options");
      const modelSuggestions = document.createElement("datalist");
      modelSuggestions.id = "strategies-codex-model-options";
      modelOptions.forEach((modelName) => {
        const optionNode = document.createElement("option");
        optionNode.value = modelName;
        modelSuggestions.appendChild(optionNode);
      });
      modelBlock.append(modelLabel, modelInput, modelSuggestions);
    } else {
      modelBlock.append(modelLabel, modelInput);
    }
    providerRow.appendChild(modelBlock);
    controls.appendChild(providerRow);

    const promptBlock = el("div", "strategy-codex-prompt-block");
    const promptLabel = el("label", "muted-label", "Operator brief");
    promptLabel.setAttribute("for", "strategies-codex-prompt");
    const prompt = el("textarea", "strategy-codex-prompt");
    prompt.id = "strategies-codex-prompt";
    prompt.rows = 4;
    prompt.placeholder = strategyState.codexMode === "suggest"
      ? "Optional: explain what kind of new threshold preset you want the strategy lab to explore."
      : "Optional: tell the strategy lab what you want emphasized in the evaluation.";
    prompt.value = strategyState.codexPrompt || "";
    prompt.addEventListener("input", () => {
      strategyState.codexPrompt = prompt.value || "";
    });
    promptBlock.append(promptLabel, prompt);
    controls.appendChild(promptBlock);

    const actionRow = el("div", "strategy-codex-action-row");
    const runButton = setTestId(el(
      "button",
      "dash-button primary-button",
      strategyState.codexSubmitting
        ? "Working..."
        : strategyState.codexMode === "suggest"
        ? "Run Suggestion"
        : "Run Evaluation",
    ), "strategy-codex-run");
    runButton.type = "button";
    runButton.disabled = !lab.available || strategyState.codexSubmitting;
    runButton.addEventListener("click", () => {
      void startCodexRun();
    });
    actionRow.appendChild(runButton);
    if (strategyState.codexMessage && strategyState.codexMessage.text) {
      const message = el(
          "div",
          `strategy-codex-message is-${strategyState.codexMessage.tone || "neutral"}`,
          strategyState.codexMessage.text,
        );
      setTestId(message, "strategy-codex-message");
      actionRow.appendChild(message);
    }
    controls.appendChild(actionRow);
    children.push(controls);

    const lower = el("div", "strategy-codex-grid");
    const left = el("section", "strategy-codex-sidebar");
    left.append(el("h3", null, "Recent Runs"));
    if (!recentRuns.length) {
      left.append(el("p", "empty-state", "No strategy lab runs yet."));
    } else {
      const runList = el("div", "strategy-codex-run-list");
      recentRuns.forEach((run) => {
        const button = setTestId(
          el("button", `strategy-codex-run-item${run.id === strategyState.codexRunId ? " is-selected" : ""}`),
          "strategy-codex-run-item",
        );
        button.type = "button";
        button.dataset.runId = run.id;
        button.disabled = strategyState.codexSubmitting;
        button.addEventListener("click", () => {
          strategyState.codexRunId = run.id;
          void fetchCodexRunDetail(run.id);
        });
        const title = el("div", "strategy-codex-run-title");
        title.append(
          el("strong", null, run.mode === "suggest" ? "Suggestion" : "Evaluation"),
          pill(codexTriggerSourceLabel(run.trigger_source), run.trigger_source === "nightly" ? "warning" : "neutral"),
          pill(run.status || "queued", run.status === "completed" ? "good" : run.status === "failed" ? "bad" : "warning"),
        );
        button.appendChild(title);
        const context = [run.window_days ? `${run.window_days}d` : null, run.series_ticker, run.strategy_name].filter(Boolean).join(" · ");
        if (context) button.appendChild(el("div", "muted-label mono", context));
        if (run.summary) button.appendChild(el("div", "strategy-codex-run-summary", run.summary));
        if (run.created_at) {
          const ts = el("span", "muted-label", formatAge(run.created_at));
          ts.dataset.timestamp = run.created_at;
          button.appendChild(ts);
        }
        runList.appendChild(button);
      });
      left.appendChild(runList);
    }

    const draftsSection = el("div", "strategy-codex-drafts");
    draftsSection.append(el("h3", null, "Saved Inactive Presets"));
    if (!inactiveStrategies.length) {
      draftsSection.append(el("p", "empty-state", "No inactive strategy-lab presets yet."));
    } else {
      const draftList = el("div", "strategy-codex-draft-list");
      inactiveStrategies.forEach((item) => {
        const draft = setTestId(el("article", "strategy-codex-draft-item"), "strategy-codex-inactive-preset");
        draft.dataset.strategyName = item.name;
        draft.append(el("strong", null, item.name));
        if (item.description) draft.append(el("p", "muted-label", item.description));
        if (Array.isArray(item.labels) && item.labels.length) {
          const labels = el("div", "strategy-codex-chip-row");
          item.labels.forEach((label) => labels.appendChild(pill(label, "neutral")));
          draft.appendChild(labels);
        }
        const activate = setTestId(
          el("button", "dash-button secondary-button", "Activate"),
          "strategy-codex-activate-preset",
        );
        activate.type = "button";
        activate.dataset.strategyName = item.name;
        activate.disabled = strategyState.codexSubmitting;
        activate.addEventListener("click", () => {
          void activateCodexStrategy(item.name);
        });
        draft.appendChild(activate);
        draftList.appendChild(draft);
      });
      draftsSection.appendChild(draftList);
    }
    left.appendChild(draftsSection);
    lower.appendChild(left);

    const right = setTestId(el("section", "strategy-codex-result"), "strategy-codex-run-detail");
    right.append(el("h3", null, "Run Detail"));
    if (!selectedRun) {
      right.append(el("p", "empty-state", "Select a run to inspect the result."));
    } else {
      const statusRow = el("div", "strategy-codex-status-row");
      statusRow.append(
        pill(selectedRun.mode === "suggest" ? "Suggestion" : "Evaluation", "neutral"),
        pill(codexTriggerSourceLabel(selectedRun.trigger_source), selectedRun.trigger_source === "nightly" ? "warning" : "neutral"),
        pill(selectedRun.status || "queued", selectedRun.status === "completed" ? "good" : selectedRun.status === "failed" ? "bad" : "warning"),
      );
      if (selectedRun.provider) {
        statusRow.appendChild(pill(codexProviderLabel(selectedRun.provider, payload), "neutral"));
      }
      if (selectedRun.model) {
        statusRow.appendChild(el("span", "muted-label mono", selectedRun.model));
      }
      right.appendChild(statusRow);
      if (selectedRun.error_text) {
        right.append(setTestId(el("p", "strategy-codex-message is-bad", selectedRun.error_text), "strategy-codex-message"));
      }
      if (selectedRun.result && selectedRun.result.kind === "evaluate") {
        const evaluation = selectedRun.result.evaluation || {};
        right.append(el("p", null, evaluation.summary || "No evaluation summary returned."));
        [["Strengths", evaluation.strengths || []], ["Risks", evaluation.risks || []], ["Opportunities", evaluation.opportunities || []], ["Recommended Actions", evaluation.recommended_actions || []]].forEach(([title, items]) => {
          const section = el("section", "strategy-detail-section");
          section.append(el("h4", null, title));
          if (items.length) {
            const list = el("ul", "strategy-methodology-list");
            items.forEach((item) => list.appendChild(el("li", null, item)));
            section.appendChild(list);
          } else {
            section.append(el("p", "empty-state", `No ${String(title).toLowerCase()} noted.`));
          }
          right.appendChild(section);
        });
      } else if (selectedRun.result && selectedRun.result.kind === "suggest") {
        const candidate = selectedRun.result.candidate || {};
        const backtest = selectedRun.result.backtest || {};
        const header = el("section", "strategy-detail-section");
        header.append(el("h4", null, candidate.name || "Suggested preset"));
        if (candidate.description) header.append(el("p", "muted-label", candidate.description));
        if (candidate.rationale) header.append(el("p", null, candidate.rationale));
        if (Array.isArray(candidate.labels) && candidate.labels.length) {
          const chipRow = el("div", "strategy-codex-chip-row");
          candidate.labels.forEach((label) => chipRow.appendChild(pill(label, "neutral")));
          header.appendChild(chipRow);
        }
        right.appendChild(header);

        const metrics = el("div", "strategy-detail-metrics");
        const candidateMetrics = backtest.candidate_metrics || {};
        metrics.append(
          metricListItem("Rank", backtest.candidate_rank ? `#${backtest.candidate_rank} / ${backtest.strategy_count || "—"}` : "—"),
          metricListItem("Win rate", candidateMetrics.overall_win_rate_display || "—"),
          metricListItem("Trade rate", candidateMetrics.overall_trade_rate_display || "—"),
          metricListItem("Coverage", candidateMetrics.outcome_coverage_rate_display || "—"),
          metricListItem("P/L", candidateMetrics.total_pnl_display || "—", candidateMetrics.total_pnl_dollars > 0 ? "value-positive" : candidateMetrics.total_pnl_dollars < 0 ? "value-negative" : ""),
          metricListItem("Avg edge", candidateMetrics.avg_edge_bps_display || "—"),
          metricListItem("Cities led", String(candidateMetrics.cities_led || 0)),
          metricListItem("Scored trades", String(candidateMetrics.total_resolved_trade_count || 0)),
        );
        right.appendChild(metrics);

        const thresholdSection = el("section", "strategy-detail-section");
        thresholdSection.append(el("h4", null, "Threshold Snapshot"));
        thresholdSection.append(renderThresholdGroups(thresholdGroupsFromObject(candidate.thresholds || {})));
        right.appendChild(thresholdSection);

        const backtestSection = el("section", "strategy-detail-section");
        backtestSection.append(el("h4", null, "Deterministic Backtest"));
        backtestSection.append(el("p", null, backtest.summary || "No deterministic backtest summary available."));
        if (backtest.selected_city) {
          backtestSection.append(
            el(
              "p",
              "muted-label",
              `${backtest.selected_city.series_ticker}: rank ${backtest.selected_city.candidate_rank || "—"}, leader ${backtest.selected_city.leader || "—"}, ${backtest.selected_city.candidate_win_rate_display || "—"} win rate.`,
            ),
          );
        }
        right.appendChild(backtestSection);

        right.appendChild(renderListSection("Strongest Cities", backtest.strongest_cities || [], "No winning city splits yet."));
        right.appendChild(renderListSection("Weakest Cities", backtest.weakest_cities || [], "No weak city splits yet."));

        const actions = el("div", "strategy-codex-result-actions");
        if (selectedRun.can_accept) {
          const accept = setTestId(
            el("button", "dash-button primary-button", "Accept As Inactive Preset"),
            "strategy-codex-accept-suggestion",
          );
          accept.type = "button";
          accept.disabled = strategyState.codexSubmitting;
          accept.addEventListener("click", () => {
            void acceptCodexSuggestion(selectedRun.id);
          });
          actions.appendChild(accept);
        } else if (selectedRun.accept_disabled_reason) {
          actions.appendChild(el("span", "muted-label", selectedRun.accept_disabled_reason));
        }
        if (selectedRun.can_activate && selectedRun.saved_strategy_name) {
          const activate = setTestId(
            el("button", "dash-button secondary-button", `Activate ${selectedRun.saved_strategy_name}`),
            "strategy-codex-activate-preset",
          );
          activate.type = "button";
          activate.dataset.strategyName = selectedRun.saved_strategy_name;
          activate.disabled = strategyState.codexSubmitting;
          activate.addEventListener("click", () => {
            void activateCodexStrategy(selectedRun.saved_strategy_name);
          });
          actions.appendChild(activate);
        }
        if (selectedRun.saved_strategy_name) {
          actions.appendChild(el("span", "muted-label", selectedRun.saved_strategy_active ? `${selectedRun.saved_strategy_name} is active.` : `${selectedRun.saved_strategy_name} is saved as inactive.`));
        }
        if (actions.childNodes.length) right.appendChild(actions);
      }
    }
    lower.appendChild(right);
    children.push(lower);
    clearNode(container, children);
  }

  function formatMetricCell(metric) {
    const wrapper = el(
      "div",
      `strategy-matrix-cell${metric.is_best ? " is-best" : ""}${metric.is_assigned ? " is-assigned" : ""}${metric.selected ? " is-selected" : ""}${!metric.has_data ? " is-empty" : ""}`,
    );
    if (!metric.has_data) {
      wrapper.append(el("span", "muted-label", "No data"));
      return wrapper;
    }
      const lines = [
        `Win ${metric.win_rate_display}`,
        `Wilson ${metric.win_rate_interval_display || "—"}`,
        `Trade ${metric.trade_rate_display}`,
        `Scored ${metric.outcome_coverage_display}`,
        `P/L ${metric.total_pnl_display}`,
        `Edge ${metric.avg_edge_bps_display}`,
        `Rooms ${metric.rooms_evaluated}`,
    ];
    lines.forEach((line) => wrapper.append(el("span", null, line)));
    return wrapper;
  }

  function sortCityRows(rows, sortKey) {
    const clone = rows.slice();
    clone.sort((a, b) => {
      if (sortKey === "series") {
        return String(a.series_ticker).localeCompare(String(b.series_ticker));
      }
      if (sortKey === "gap") {
        return (b.gap_to_runner_up ?? -1) - (a.gap_to_runner_up ?? -1);
      }
      if (sortKey === "assignment_gap") {
        return (b.gap_to_assignment ?? -1) - (a.gap_to_assignment ?? -1);
      }
      if (sortKey === "resolved") {
        return ((b.recommendation || {}).resolved_trade_count ?? b.best_resolved_trade_count ?? -1)
          - ((a.recommendation || {}).resolved_trade_count ?? a.best_resolved_trade_count ?? -1);
      }
      if (sortKey === "coverage") {
        return ((b.recommendation || {}).outcome_coverage_rate ?? -1)
          - ((a.recommendation || {}).outcome_coverage_rate ?? -1);
      }
      if (sortKey === "best_win_rate") {
        return (b.best_strategy_win_rate ?? -1) - (a.best_strategy_win_rate ?? -1);
      }
      if ((a.sort_priority || 0) !== (b.sort_priority || 0)) {
        return (a.sort_priority || 0) - (b.sort_priority || 0);
      }
      const assignmentGapDiff = (b.gap_to_assignment ?? -1) - (a.gap_to_assignment ?? -1);
      if (assignmentGapDiff !== 0) return assignmentGapDiff;
      return String(a.series_ticker).localeCompare(String(b.series_ticker));
    });
    return clone;
  }

  function filteredCityRows(payload) {
    const reviewAvailable = Boolean(payload && payload.summary && payload.summary.review_available);
    const query = strategyState.searchQuery.trim().toLowerCase();
    const rows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix : [];
    return sortCityRows(rows, strategyState.sortKey)
      .filter((row) => cityMatchesFilter(row, strategyState.cityFilter, reviewAvailable))
      .filter((row) => cityMatchesSearch(row, query));
  }

  function renderCityFilters(payload) {
    const container = document.getElementById("strategies-city-filters");
    if (!container) return;
    const summary = payload && payload.summary ? payload.summary : {};
    const reviewAvailable = Boolean(summary.review_available);
    const rows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix : [];
    const counts = countCityFilters(rows, reviewAvailable);
    const filters = reviewAvailable
      ? STRATEGY_CITY_FILTERS
      : STRATEGY_CITY_FILTERS.filter((filterKey) => filterKey !== "needs_review");
    const buttons = filters.map((filterKey) => {
      const button = el(
        "button",
        `strategy-city-filter${filterKey === strategyState.cityFilter ? " is-active" : ""}`,
      );
      button.type = "button";
      button.dataset.cityFilter = filterKey;
      button.setAttribute("aria-selected", filterKey === strategyState.cityFilter ? "true" : "false");
      button.append(
        el("span", null, strategyCityFilterLabel(filterKey)),
        el("strong", "mono", String(counts[filterKey] || 0)),
      );
      button.addEventListener("click", () => {
        if (strategyState.fetching || filterKey === strategyState.cityFilter) return;
        strategyState.cityFilter = filterKey;
        renderStrategies(strategyState.payload);
      });
      return button;
    });
    clearNode(container, buttons);
  }

  function renderStrategyMatrix(payload, rows) {
    const container = document.getElementById("strategies-city-matrix");
    const meta = document.getElementById("strategies-matrix-meta");
    if (!container) return;
    const query = strategyState.searchQuery.trim().toLowerCase();
    const allRows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix : [];
    const visibleRows = Array.isArray(rows) ? rows : filteredCityRows(payload);
    const filterActive = strategyState.cityFilter !== "all";
    if (!visibleRows.length) {
      clearNode(container, [el("p", "empty-state", (query || filterActive) ? "No cities match the current search or filter." : "No city comparison data yet.")]);
      if (meta) {
        meta.textContent = (query || filterActive)
          ? `0 of ${allRows.length} cities match the active analysis filters`
          : "No city-level comparison data";
      }
      return;
    }
    if (meta) {
      meta.textContent = (query || filterActive)
        ? `${visibleRows.length} of ${allRows.length} cities match the active analysis filters`
        : `${visibleRows.length} city rows in the selected window`;
    }

    const table = el("table", "positions-table strategy-matrix-table");
    const thead = el("thead");
    const headRow = el("tr");
    ["City", "Assignment", "Recommendation", "Review", "Gap", "Resolved", "Coverage", "Best Strategy"].forEach((label) => headRow.append(el("th", null, label)));
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = el("tbody");
    visibleRows.forEach((row) => {
      const recommendation = row.recommendation || {};
      const recommendationStatus = recommendation.status || "no_outcomes";
      const review = row.review || {};
      const tr = el(
        "tr",
        `strategy-matrix-row${row.series_ticker === strategyState.selectedSeriesTicker ? " is-selected" : ""} is-${recommendationStatus.replace(/_/g, "-")}`,
      );
      const cityTd = el("td");
      const cityButton = el("button", "strategy-matrix-link");
      cityButton.type = "button";
      cityButton.dataset.seriesTicker = row.series_ticker;
      cityButton.append(el("strong", "mono", row.series_ticker), el("span", "muted-label", row.city_label || row.location_name || row.series_ticker));
      cityButton.addEventListener("click", () => {
        if (strategyState.fetching) return;
        loadStrategies({
          windowDays: strategyState.windowDays,
          seriesTicker: row.series_ticker,
          strategyName: null,
          focusMode: "cities",
          explicitSelection: true,
        });
      });
      cityTd.appendChild(cityButton);
      tr.appendChild(cityTd);

      const assignmentCell = el("td");
      const assignmentWrap = el("div", "strategy-matrix-cell");
      const assignmentStatus = row.assignment_context_status || "unassigned";
      const assignmentTone = assignmentStatus === "matches_recommendation" ? "good" : "neutral";
      assignmentWrap.appendChild(
        row.assignment && row.assignment.strategy_name ? pill(row.assignment.strategy_name, assignmentTone) : pill("Unassigned", "neutral"),
      );
      assignmentWrap.appendChild(
        el(
          "span",
          "muted-label",
          assignmentStatus === "matches_recommendation"
            ? "Matches recommendation"
            : assignmentStatus === "differs_from_recommendation"
            ? "Differs from recommendation"
            : "Unassigned",
        ),
      );
      assignmentCell.appendChild(assignmentWrap);
      tr.appendChild(assignmentCell);

      const recommendationCell = el("td");
      const recommendationWrap = el("div", "strategy-matrix-cell is-best");
      recommendationWrap.appendChild(
        recommendation.strategy_name ? pill(recommendation.strategy_name, recommendationTone(recommendationStatus)) : pill("—", "neutral"),
      );
      recommendationWrap.appendChild(el("span", "muted-label", recommendation.label || "No recommendation"));
      recommendationWrap.appendChild(el("span", "mono", recommendation.gap_to_runner_up_display ? `Gap ${recommendation.gap_to_runner_up_display}` : "Gap —"));
      recommendationCell.appendChild(recommendationWrap);
      tr.appendChild(recommendationCell);

      const reviewCell = el("td");
      const reviewWrap = el("div", "strategy-matrix-cell");
      if (payload && payload.summary && payload.summary.review_available && review.status && review.label) {
        reviewWrap.appendChild(pill(review.label, reviewTone(review.status)));
        reviewWrap.appendChild(
          el(
            "span",
            "muted-label",
            review.needs_review
              ? "Needs review"
              : review.status === "ready_for_approval"
              ? "Ready"
              : review.status === "aligned"
              ? "Aligned"
              : "Waiting",
          ),
        );
      } else {
        reviewWrap.appendChild(el("span", "muted-label", "—"));
      }
      reviewCell.appendChild(reviewWrap);
      tr.appendChild(reviewCell);

      tr.appendChild(el("td", "mono", row.gap_to_runner_up_display || "—"));
      tr.appendChild(el("td", "mono", row.best_resolved_trade_count_display || "0"));
      tr.appendChild(el("td", "mono", row.best_outcome_coverage_display || "—"));

      const bestCell = el("td");
      const bestWrap = el("div", "strategy-matrix-cell");
      bestWrap.appendChild(
        row.best_strategy ? pill(row.best_strategy, recommendationTone(recommendationStatus)) : pill("—", "neutral"),
      );
      bestWrap.appendChild(el("span", "muted-label", row.best_strategy_win_rate_display || "—"));
      if (row.runner_up_strategy) {
        bestWrap.appendChild(el("span", "mono", `Runner-up ${row.runner_up_strategy}`));
      }
      bestCell.appendChild(bestWrap);
      tr.appendChild(bestCell);
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    clearNode(container, [table]);
  }

  function chartSeriesValue(valueKey, point) {
    const value = point[valueKey];
    return typeof value === "number" ? value : null;
  }

  function buildLineChart(series, options) {
    const width = 360;
    const height = 150;
    const padding = 18;
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("class", "strategy-chart-svg");

    const entries = [];
    series.forEach((item, seriesIndex) => {
      (item.points || []).forEach((point) => {
        const value = chartSeriesValue(options.valueKey, point);
        if (value == null) return;
        entries.push({
          x: new Date(point.run_at).getTime(),
          y: value,
          seriesIndex,
        });
      });
    });
    if (!entries.length) {
      return null;
    }

    const minX = Math.min(...entries.map((entry) => entry.x));
    const maxX = Math.max(...entries.map((entry) => entry.x));
    let minY = Math.min(...entries.map((entry) => entry.y));
    let maxY = Math.max(...entries.map((entry) => entry.y));
    if (minY === maxY) {
      minY -= 1;
      maxY += 1;
    }

    function scaleX(value) {
      if (maxX === minX) return width / 2;
      return padding + ((value - minX) / (maxX - minX)) * (width - padding * 2);
    }

    function scaleY(value) {
      return height - padding - ((value - minY) / (maxY - minY)) * (height - padding * 2);
    }

    for (let idx = 0; idx < 3; idx += 1) {
      const y = padding + (idx / 2) * (height - padding * 2);
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", String(padding));
      line.setAttribute("x2", String(width - padding));
      line.setAttribute("y1", String(y));
      line.setAttribute("y2", String(y));
      line.setAttribute("class", "strategy-chart-grid");
      svg.appendChild(line);
    }

    series.forEach((item, index) => {
      const points = (item.points || [])
        .map((point) => {
          const value = chartSeriesValue(options.valueKey, point);
          if (value == null) return null;
          return `${scaleX(new Date(point.run_at).getTime())},${scaleY(value)}`;
        })
        .filter(Boolean);
      if (!points.length) return;
      const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
      polyline.setAttribute("points", points.join(" "));
      polyline.setAttribute("fill", "none");
      polyline.setAttribute("stroke", STRATEGY_COLORS[index % STRATEGY_COLORS.length]);
      polyline.setAttribute("stroke-width", "2.5");
      polyline.setAttribute("stroke-linecap", "round");
      polyline.setAttribute("stroke-linejoin", "round");
      svg.appendChild(polyline);
    });

    return svg;
  }

  function chartCard(title, subtitle, series, valueKey) {
    const card = el("section", "strategy-chart-card");
    card.append(el("h4", null, title));
    if (subtitle) card.append(el("p", "muted-label", subtitle));
    const svg = buildLineChart(series, { valueKey });
    if (!svg) {
      card.append(el("p", "empty-state", "Not enough stored history yet."));
      return card;
    }
    card.appendChild(svg);

    const legend = el("div", "strategy-chart-legend");
    series.forEach((item, index) => {
      const label = el("span", "strategy-chart-legend-item");
      const dot = el("span", "strategy-chart-dot");
      dot.style.background = STRATEGY_COLORS[index % STRATEGY_COLORS.length];
      label.append(dot, document.createTextNode(item.label || item.strategy_name || item.name || `Series ${index + 1}`));
      legend.appendChild(label);
    });
    card.appendChild(legend);
    return card;
  }

  function renderEventList(container, events, emptyText) {
    if (!container) return;
    if (!Array.isArray(events) || !events.length) {
      clearNode(container, [el("p", "empty-state", emptyText)]);
      return;
    }
    const list = el("div", "strategy-event-list");
    events.forEach((event) => {
      const row = el("article", "strategy-event-row");
      const header = el("div", "strategy-event-header");
      const eventLabel =
        event.kind === "promotion"
          ? "promotion"
          : event.kind === "threshold_adjustment"
          ? "tuning"
          : event.kind === "assignment_approval"
          ? "approval"
          : "event";
      const eventTone =
        event.kind === "promotion"
          ? "good"
          : event.kind === "assignment_approval"
          ? "neutral"
          : "warning";
      header.append(
        pill(eventLabel, eventTone),
        el("strong", null, event.summary || "Strategy event"),
      );
      row.appendChild(header);
      const meta = el("div", "muted-label strategy-event-meta");
      meta.append(document.createTextNode(event.source || "strategy"));
      if (event.created_at) {
        meta.append(document.createTextNode(" · "));
        const ts = el("span", null, formatAge(event.created_at));
        ts.dataset.timestamp = event.created_at;
        meta.appendChild(ts);
      }
      if (event.series_ticker) {
        meta.append(document.createTextNode(` · ${event.series_ticker}`));
      }
      row.appendChild(meta);
      const extras = [];
      if (event.win_rate_display && event.win_rate_display !== "—") extras.push(`win ${event.win_rate_display}`);
      if (event.trade_count) extras.push(`trades ${event.trade_count}`);
      if (event.outcome_coverage_display && event.outcome_coverage_display !== "—") extras.push(event.outcome_coverage_display);
      if (event.gap_to_runner_up_display && event.gap_to_runner_up_display !== "—") extras.push(`gap ${event.gap_to_runner_up_display}`);
      if (event.change_display) extras.push(event.change_display);
      if (extras.length) row.appendChild(el("div", "strategy-event-extra mono", extras.join(" · ")));
      if (event.note) row.appendChild(el("div", "strategy-event-note", `Note: ${event.note}`));
      list.appendChild(row);
    });
    clearNode(container, [list]);
  }

  async function submitStrategyApproval(detail) {
    const city = detail && detail.city ? detail.city : null;
    const approval = detail && detail.approval ? detail.approval : null;
    const recommendation = city && city.recommendation ? city.recommendation : null;
    if (!city || !approval || !approval.eligible || !recommendation || strategyState.approvalSubmitting) return;

    const note = (strategyState.approvalNotes[city.series_ticker] || "").trim();
    if (!note) {
      setApprovalMessage(city.series_ticker, "bad", "Approval note is required.");
      renderStrategiesDetail(strategyState.payload && strategyState.payload.detail_context ? strategyState.payload.detail_context : {});
      return;
    }

    strategyState.approvalSubmitting = true;
    setApprovalMessage(city.series_ticker, "neutral", "Submitting approval...");
    if (strategyState.payload) renderStrategies(strategyState.payload);

    try {
      const response = await fetch(`/api/strategies/assignments/${encodeURIComponent(city.series_ticker)}/approve`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          expected_strategy_name: recommendation.strategy_name,
          expected_recommendation_status: recommendation.status,
          note,
        }),
      });
      const body = await response.json().catch(() => ({}));
      if (redirectToLoginIfRequired(response, body)) return;
      if (response.ok) {
        strategyState.approvalNotes[city.series_ticker] = "";
        setApprovalMessage(
          city.series_ticker,
          "good",
          `Assignment approved for ${city.series_ticker}: ${body.strategy_name || recommendation.strategy_name}.`,
        );
        await loadStrategies({
          windowDays: strategyState.windowDays,
          seriesTicker: city.series_ticker,
          strategyName: null,
          preserveApprovalMessage: true,
          focusMode: strategyState.focusMode,
          explicitSelection: true,
        });
        return;
      }
      if (response.status === 409) {
        setApprovalMessage(
          city.series_ticker,
          "warning",
          responseErrorMessage(body, "Recommendation changed. Reloaded the latest 180d snapshot."),
        );
        await loadStrategies({
          windowDays: strategyState.windowDays,
          seriesTicker: city.series_ticker,
          strategyName: null,
          preserveApprovalMessage: true,
          focusMode: strategyState.focusMode,
          explicitSelection: true,
        });
        return;
      }
      setApprovalMessage(
        city.series_ticker,
        "bad",
        responseErrorMessage(body, "Approval failed. Try refreshing the strategy snapshot."),
      );
    } catch (_) {
      setApprovalMessage(city.series_ticker, "bad", "Approval request failed. Check the dashboard connection and try again.");
    } finally {
      strategyState.approvalSubmitting = false;
      if (strategyState.payload) renderStrategies(strategyState.payload);
    }
  }

  function cityEvidenceInterpretation(detail) {
    const city = detail && detail.city ? detail.city : {};
    const rationale = detail && (detail.recommendation_rationale || detail.promotion_rationale) ? (detail.recommendation_rationale || detail.promotion_rationale) : {};
    const recommendation = city.recommendation || {};
    const status = rationale.recommendation_status || recommendation.status || "";
    if (status === "strong_recommendation") {
      return "Evidence is actionable: the current winner clears the resolved-trade, coverage, and strong-gap thresholds against the runner-up.";
    }
    if (status === "lean_recommendation") {
      return "Evidence is actionable but narrow: the current winner clears the resolved-trade and coverage gates, but only by the lean-gap threshold.";
    }
    if (status === "too_close") {
      return "Evidence is still too close: the current winner has support, but the gap over the runner-up is not wide enough to act on confidently.";
    }
    if (status === "low_sample") {
      return "Evidence is incomplete: this city still needs more resolved trades before the recommendation is strong enough to trust.";
    }
    return "Evidence is incomplete: this city does not yet have enough scored outcome history to produce a reliable winner.";
  }

  function renderCityResearchBrief(detail, title) {
    const city = detail.city || {};
    const approval = detail.approval || {};
    const review = detail.review || {};
    const recommendation = city.recommendation || {};
    const currentAssignment = review.current_assignment || city.assignment || {};
    const latestRecommendation = review.latest_recommendation || recommendation;
    const card = el("section", "strategy-detail-section strategy-decision-brief strategy-research-brief");
    card.append(el("h4", null, title || "City Research Brief"));

    const statusRow = el("div", "strategy-review-status-row");
    statusRow.append(
      currentAssignment.strategy_name ? pill(`Assigned ${currentAssignment.strategy_name}`, "neutral") : pill("Unassigned", "neutral"),
      recommendation.strategy_name ? pill(`Recommended ${recommendation.strategy_name}`, recommendationTone(recommendation.status)) : pill("No recommendation", "neutral"),
    );
    if (review.available) {
      statusRow.append(pill(review.label || "Review", reviewTone(review.status)));
    }
    if (approval.eligible) {
      statusRow.append(pill(approval.label || "Ready to approve", "good"));
    }
    card.appendChild(statusRow);

    const metrics = el("div", "strategy-detail-metrics strategy-brief-metrics");
    metrics.append(
      metricListItem("Current assignment", currentAssignment.strategy_name || "Unassigned"),
      metricListItem("Recommended strategy", latestRecommendation.strategy_name || recommendation.strategy_name || city.best_strategy || "—"),
      metricListItem("Runner-up strategy", city.runner_up_strategy || "—"),
      metricListItem("Gap to runner-up", latestRecommendation.gap_to_runner_up_display || city.gap_to_runner_up_display || "—"),
      metricListItem("Gap to assignment", city.gap_to_assignment_display || "—"),
      metricListItem("Resolved trades", latestRecommendation.resolved_trade_count_display || city.best_resolved_trade_count_display || "0"),
      metricListItem("Outcome coverage", latestRecommendation.outcome_coverage_display || city.best_outcome_coverage_display || "—"),
    );
    if (review.available) {
      metrics.append(metricListItem("Review state", review.label || "—"));
    }
    card.appendChild(metrics);

    const interpretation = el("div", "strategy-brief-interpretation");
    interpretation.append(
      el("strong", null, "Evidence interpretation"),
      el("p", null, cityEvidenceInterpretation(detail)),
    );
    card.appendChild(interpretation);

    if (review.available && review.reason) {
      card.append(el("p", "muted-label", review.reason));
    }
    if (review.available && review.next_action_copy) {
      const action = el("div", "strategy-review-next-action");
      action.append(
        el("strong", null, review.next_action_label || "Next action"),
        el("p", "muted-label", review.next_action_copy),
      );
      card.appendChild(action);
    }
    if (review.available && review.last_approval_event && review.last_approval_event.note) {
      const note = el("div", "strategy-review-audit-note");
      note.append(
        el("strong", null, "Latest approval note"),
        el("p", null, review.last_approval_event.note),
      );
      if (review.last_approval_event.created_at) {
        const stamp = el("span", "muted-label", formatAge(review.last_approval_event.created_at));
        stamp.dataset.timestamp = review.last_approval_event.created_at;
        note.appendChild(stamp);
      }
      card.appendChild(note);
    }

    return card;
  }

  function renderCityApprovalSection(detail) {
    const city = detail.city || {};
    const approval = detail.approval || {};
    const review = detail.review || {};
    const recommendation = city.recommendation || {};
    const messageNode = approvalMessageNode(city.series_ticker);
    if (!approval.eligible && !messageNode) return null;

    const section = el("section", "strategy-detail-section strategy-approval-card");
    section.append(el("h4", null, "Approval"));

    if (approval.eligible) {
      const noteValue = strategyState.approvalNotes[city.series_ticker] || "";
      const form = el("div", "strategy-approval-form");
      form.append(
        el(
          "p",
          "muted-label",
          `This writes the latest ${approval.window_days || 180}d recommendation into canonical city assignments and records your note for audit.`,
        ),
      );
      if (recommendation.status === "lean_recommendation") {
        form.append(
          el(
            "p",
            "strategy-approval-hint",
            "Lean recommendations are actionable, but the edge over the runner-up is still narrow. Add note context that explains why you are comfortable approving it now.",
          ),
        );
      }
      if (approval.reason) {
        form.append(el("p", "muted-label", approval.reason));
      }
      if (review.available && review.next_action_label) {
        form.append(el("p", "muted-label", `${review.next_action_label}.`));
      }
      const label = el("label", "muted-label", "Operator note");
      label.setAttribute("for", `strategy-approval-note-${city.series_ticker}`);
      const textarea = setTestId(el("textarea", "strategy-approval-textarea"), "strategy-approval-note");
      textarea.id = `strategy-approval-note-${city.series_ticker}`;
      textarea.dataset.seriesTicker = city.series_ticker;
      textarea.rows = 4;
      textarea.placeholder = "Why does this recommendation make sense to approve right now?";
      textarea.value = noteValue;
      textarea.addEventListener("input", () => {
        strategyState.approvalNotes[city.series_ticker] = textarea.value;
        if (strategyState.approvalMessage && strategyState.approvalMessage.seriesTicker === city.series_ticker) {
          setApprovalMessage(null, null, null);
        }
      });
      form.append(label, textarea);

      const actions = el("div", "strategy-approval-actions");
      const button = setTestId(el(
        "button",
        "dash-button primary-button",
        strategyState.approvalSubmitting ? "Approving..." : `Approve ${approval.strategy_name || recommendation.strategy_name || "recommendation"}`,
      ), "strategy-approval-submit");
      button.type = "button";
      button.dataset.seriesTicker = city.series_ticker;
      button.disabled = strategyState.approvalSubmitting;
      button.addEventListener("click", () => {
        submitStrategyApproval(detail);
      });
      actions.appendChild(button);
      form.appendChild(actions);

      if (messageNode) form.appendChild(messageNode);
      section.appendChild(form);
    } else {
      section.appendChild(messageNode);
    }

    return section;
  }

  function renderCityDetail(detail, targetIds, options) {
    const container = document.getElementById(targetIds.containerId);
    const meta = document.getElementById(targetIds.metaId);
    if (!container) return;
    const briefTitle = options && options.briefTitle ? options.briefTitle : "City Research Brief";
    if (meta) {
      meta.textContent = detail.city
        ? `${detail.city.series_ticker} · ${briefTitle.toLowerCase()} and recommendation evidence`
        : "City evidence";
    }

    const children = [];
    const city = detail.city || {};
    const header = el("div", "strategy-detail-header");
    header.append(el("h3", null, city.series_ticker || "City detail"));
    header.append(el("p", "muted-label", city.city_label || city.location_name || "City-level strategy comparison"));
    const codexAction = setTestId(el("button", "strategy-inline-action", "Open in Evaluation Lab"), "strategy-open-evaluation-lab");
    codexAction.type = "button";
    codexAction.dataset.seriesTicker = city.series_ticker || "";
    codexAction.addEventListener("click", () => {
      openCodexLabContext({
        seriesTicker: city.series_ticker || null,
        strategyName: null,
      });
    });
    header.appendChild(codexAction);
    children.push(header);
    children.push(renderCityResearchBrief(detail, briefTitle));
    const approvalSection = renderCityApprovalSection(detail);
    if (approvalSection) children.push(approvalSection);

    const rankingSection = el("section", "strategy-detail-section");
    rankingSection.append(el("h4", null, "Strategy Ranking"));
    const rankingWrap = el("div", "table-wrap");
    const rankingTable = el("table", "positions-table strategy-detail-table");
    const rankingHead = el("thead");
    const rankingHeadRow = el("tr");
    ["Strategy", "Win Rate", "Wilson", "Trade Rate", "Trades", "Scored", "Coverage", "P/L", "Edge", "Status"].forEach((label) => rankingHeadRow.append(el("th", null, label)));
    rankingHead.appendChild(rankingHeadRow);
    rankingTable.appendChild(rankingHead);
    const rankingBody = el("tbody");
    (detail.ranking || []).forEach((item) => {
      const tr = el("tr");
      tr.append(
        el("td", "mono", item.strategy_name),
        el("td", `mono ${item.is_best ? "value-positive" : ""}`.trim(), item.win_rate_display || "—"),
        el("td", "mono", item.win_rate_interval_display || "—"),
        el("td", "mono", item.trade_rate_display || "—"),
        el("td", "mono", String(item.trade_count || 0)),
        el("td", "mono", item.resolved_trade_count_display || "0"),
        el("td", "mono", item.outcome_coverage_display || "—"),
        el("td", `mono ${item.total_pnl_dollars > 0 ? "value-positive" : item.total_pnl_dollars < 0 ? "value-negative" : ""}`.trim(), item.total_pnl_display || "—"),
        el("td", "mono", item.avg_edge_bps_display || "—"),
        el("td", null, ""),
      );
      tr.lastChild.appendChild(
        item.is_best ? pill("winner", "good") : item.is_runner_up ? pill("runner-up", "neutral") : item.is_assigned ? pill("assigned", "warning") : pill("—", "neutral"),
      );
      rankingBody.appendChild(tr);
    });
    rankingTable.appendChild(rankingBody);
    rankingWrap.appendChild(rankingTable);
    rankingSection.appendChild(rankingWrap);
    children.push(rankingSection);

    const comparisonSection = el("section", "strategy-detail-section");
    comparisonSection.append(el("h4", null, "Threshold Comparison"));
    const comparisonGrid = el("div", "strategy-comparison-grid");
    (detail.threshold_comparison || []).forEach((entry) => {
      const block = el("article", "strategy-comparison-card");
      block.append(el("strong", null, `${entry.strategy_name} (${entry.role})`));
      block.append(renderThresholdGroups(entry.threshold_groups || []));
      comparisonGrid.appendChild(block);
    });
    if (!comparisonGrid.childNodes.length) comparisonGrid.append(el("p", "empty-state", "No threshold comparison available."));
    comparisonSection.appendChild(comparisonGrid);
    children.push(comparisonSection);

    const trendSection = el("section", "strategy-detail-section");
    trendSection.append(el("h4", null, "Trend History"));
    if (detail.trend && detail.trend.note) trendSection.append(el("p", "muted-label", detail.trend.note));
    const trendSeries = (detail.trend && detail.trend.series ? detail.trend.series : []).map((item) => ({
      strategy_name: item.strategy_name,
      label: item.strategy_name,
      points: item.points || [],
    }));
    trendSection.appendChild(chartCard("Win Rate Over Time", "Stored regression snapshots", trendSeries, "win_rate"));
    children.push(trendSection);

    const eventSection = el("section", "strategy-detail-section");
    eventSection.append(el("h4", null, "Recent Events"));
    const eventContainer = el("div");
    renderEventList(eventContainer, detail.recent_events || [], "No recent city-specific strategy events.");
    eventSection.appendChild(eventContainer);
    children.push(eventSection);

    clearNode(container, children);
  }

  function renderListSection(title, items, emptyText) {
    const section = el("section", "strategy-detail-section");
    section.append(el("h4", null, title));
    if (!items.length) {
      section.append(el("p", "empty-state", emptyText));
      return section;
    }
    const list = el("div", "strategy-city-distribution");
    items.forEach((item) => {
      const row = el("div", "strategy-city-row");
      const text = el("div", "strategy-city-row-text");
      text.append(el("strong", "mono", item.series_ticker), el("span", "muted-label", item.city_label));
      const metrics = el("span", "mono", `${item.win_rate_display} · ${item.resolved_trade_count_display || "0"} scored · ${item.total_pnl_display}`);
      row.append(text, metrics);
      list.appendChild(row);
    });
    section.appendChild(list);
    return section;
  }

  function renderStrategyDetail(detail, targetIds) {
    const container = document.getElementById(targetIds.containerId);
    const meta = document.getElementById(targetIds.metaId);
    if (!container) return;
    if (meta) meta.textContent = detail.strategy ? `${detail.strategy.name} · preset performance and stability` : "Strategy detail";

    const children = [];
    const strategy = detail.strategy || {};
    const header = el("div", "strategy-detail-header");
    header.append(el("h3", null, strategy.name || "Strategy detail"));
    if (strategy.description) header.append(el("p", "muted-label", strategy.description));
    const codexAction = setTestId(el("button", "strategy-inline-action", "Open in Evaluation Lab"), "strategy-open-evaluation-lab");
    codexAction.type = "button";
    codexAction.dataset.strategyName = strategy.name || "";
    codexAction.addEventListener("click", () => {
      openCodexLabContext({
        seriesTicker: null,
        strategyName: strategy.name || null,
      });
    });
    header.appendChild(codexAction);
    children.push(header);

    const metricGrid = el("div", "strategy-detail-metrics");
    metricGrid.append(
      metricListItem("Win rate", strategy.overall_win_rate_display || "—", strategy.overall_win_rate >= 0.6 ? "value-positive" : strategy.overall_win_rate <= 0.35 ? "value-negative" : ""),
      metricListItem("Cities led", String(strategy.cities_led || 0)),
      metricListItem("Assigned cities", String(strategy.assigned_city_count || 0)),
      metricListItem("Outcome coverage", strategy.outcome_coverage_display || "—"),
      metricListItem("Total P/L", strategy.total_pnl_display || "—", strategy.total_pnl_dollars > 0 ? "value-positive" : strategy.total_pnl_dollars < 0 ? "value-negative" : ""),
      metricListItem("Trade rate", strategy.overall_trade_rate_display || "—"),
      metricListItem("Avg edge", strategy.avg_edge_bps_display || "—"),
      metricListItem("Scored trades", strategy.total_resolved_trade_count_display || "0"),
      metricListItem("Sim trades", strategy.total_trade_count_display || "0"),
    );
    children.push(metricGrid);

    const thresholdSection = el("section", "strategy-detail-section");
    thresholdSection.append(el("h4", null, "Threshold Set"));
    thresholdSection.append(renderThresholdGroups(strategy.threshold_groups || []));
    children.push(thresholdSection);

    const trendPoints = detail.trend && Array.isArray(detail.trend.points) ? detail.trend.points : [];
    const trendSeries = [{ name: strategy.name, label: strategy.name, points: trendPoints }];
    const trendGrid = el("div", "strategy-detail-chart-grid");
    trendGrid.append(
      chartCard("Win Rate", detail.trend && detail.trend.note ? detail.trend.note : "Stored regression snapshots", trendSeries, "win_rate"),
      chartCard("Trade Rate", "Stored regression snapshots", trendSeries, "trade_rate"),
      chartCard("Total P/L", "Stored regression snapshots", trendSeries, "total_pnl_dollars"),
    );
    const trendSection = el("section", "strategy-detail-section");
    trendSection.append(el("h4", null, detail.trend && detail.trend.title ? detail.trend.title : "Trend"));
    trendSection.appendChild(trendGrid);
    children.push(trendSection);

    children.push(renderListSection("Strongest Cities", detail.strongest_cities || [], "No winning city splits yet."));
    children.push(renderListSection("Weakest Cities", detail.weakest_cities || [], "No weak-city splits yet."));

    const distributionSection = el("section", "strategy-detail-section");
    distributionSection.append(el("h4", null, "City Distribution"));
    if (Array.isArray(detail.city_distribution) && detail.city_distribution.length) {
      const distTableWrap = el("div", "table-wrap");
      const distTable = el("table", "positions-table strategy-detail-table");
      const distHead = el("thead");
      const distHeadRow = el("tr");
      ["City", "Win Rate", "Trade Rate", "Trades", "Scored", "Coverage", "P/L", "Flags"].forEach((label) => distHeadRow.append(el("th", null, label)));
      distHead.appendChild(distHeadRow);
      distTable.appendChild(distHead);
      const distBody = el("tbody");
      detail.city_distribution.forEach((item) => {
        const tr = el("tr");
        tr.append(
          el("td", "mono", item.series_ticker),
          el("td", "mono", item.win_rate_display || "—"),
          el("td", "mono", item.trade_rate_display || "—"),
          el("td", "mono", item.trade_count_display || "0"),
          el("td", "mono", item.resolved_trade_count_display || "0"),
          el("td", "mono", item.outcome_coverage_display || "—"),
          el("td", `mono ${item.total_pnl_dollars > 0 ? "value-positive" : item.total_pnl_dollars < 0 ? "value-negative" : ""}`.trim(), item.total_pnl_display || "—"),
          el("td", null, ""),
        );
        if (item.is_best) tr.lastChild.appendChild(pill("lead", "good"));
        else if (item.is_assigned) tr.lastChild.appendChild(pill("assigned", "warning"));
        else tr.lastChild.appendChild(pill("—", "neutral"));
        distBody.appendChild(tr);
      });
      distTable.appendChild(distBody);
      distTableWrap.appendChild(distTable);
      distributionSection.appendChild(distTableWrap);
    } else {
      distributionSection.append(el("p", "empty-state", "No city distribution data yet."));
    }
    children.push(distributionSection);

    const eventSection = el("section", "strategy-detail-section");
    eventSection.append(el("h4", null, "Recent Strategy Events"));
    const eventContainer = el("div");
    renderEventList(eventContainer, detail.recent_events || [], "No recent events mention this strategy yet.");
    eventSection.appendChild(eventContainer);
    children.push(eventSection);

    clearNode(container, children);
  }

  function renderReviewDetail(detail) {
    const container = document.getElementById("strategies-review-detail");
    const meta = document.getElementById("strategies-review-detail-meta");
    if (!container) return;
    if (!detail || detail.type !== "city" || detail.selected_series_ticker !== strategyState.selectedSeriesTicker) {
      if (meta) meta.textContent = strategyState.fetching ? "Loading decision brief..." : "Select a city from the review queue to inspect the current decision brief.";
      clearNode(container, [el("p", "empty-state", strategyState.fetching ? "Loading decision brief..." : "No city decision brief selected yet.")]);
      return;
    }
    renderCityDetail(detail, {
      containerId: "strategies-review-detail",
      metaId: "strategies-review-detail-meta",
    }, {
      briefTitle: "Decision Brief",
    });
  }

  function renderStrategiesDetail(detail) {
    const focusMode = strategyState.focusMode === "review" ? "cities" : strategyState.focusMode;
    const targetIds = focusMode === "cities"
      ? { containerId: "strategies-cities-detail", metaId: "strategies-cities-detail-meta" }
      : { containerId: "strategies-detail", metaId: "strategies-detail-meta" };
    const container = document.getElementById(targetIds.containerId);
    const meta = document.getElementById(targetIds.metaId);
    if (!container) return;
    if (!detail) {
      if (meta) meta.textContent = "No detail available yet";
      clearNode(container, [el("p", "empty-state", "No strategy data available yet.")]);
      return;
    }
    if (focusMode === "cities") {
      if (!strategyState.selectedSeriesTicker) {
        if (meta) meta.textContent = "No city matches the current analysis filters";
        clearNode(container, [el("p", "empty-state", "No city research brief is available for the current search or filter.")]);
        return;
      }
      if (detail.type !== "city" || detail.selected_series_ticker !== strategyState.selectedSeriesTicker) {
        if (meta) meta.textContent = strategyState.fetching ? "Loading city research brief..." : `Loading ${strategyState.selectedSeriesTicker} city research brief...`;
        clearNode(container, [el("p", "empty-state", strategyState.fetching ? "Loading city research brief..." : "Loading city research brief...")]);
        return;
      }
      renderCityDetail(detail, targetIds, {
        briefTitle: "City Research Brief",
      });
      return;
    }
    if (focusMode === "strategies" && detail.type === "strategy" && detail.selected_strategy_name === strategyState.selectedStrategyName) {
      renderStrategyDetail(detail, targetIds);
      return;
    }
    if (focusMode === "strategies") {
      if (meta) meta.textContent = strategyState.fetching ? "Loading strategy drilldown..." : "Select a strategy card to inspect preset-level evidence.";
      clearNode(container, [el("p", "empty-state", strategyState.fetching ? "Loading strategy drilldown..." : "No strategy drilldown selected yet.")]);
      return;
    }
    if (meta) meta.textContent = "No detail available yet";
    clearNode(container, [el("p", "empty-state", detail.message || "No strategy data available yet.")]);
  }

  function renderRecentStrategyChanges(recentPromotions) {
    const container = document.getElementById("strategies-recent");
    renderEventList(container, recentPromotions || [], "No recent approvals, promotions, or threshold tuning events.");
  }

  function renderMethodology(methodology) {
    const container = document.getElementById("strategies-methodology");
    if (!container) return;
    const children = [];
    if (methodology && methodology.title) {
      children.push(el("p", "muted-label", methodology.title));
    }
    const list = el("ul", "strategy-methodology-list");
    (methodology && methodology.points ? methodology.points : []).forEach((point) => {
      const item = el("li", null, point);
      list.appendChild(item);
    });
    children.push(list);
    const note = el(
      "p",
      "muted-label",
      `Recommendation tiers: at least ${methodology && methodology.recommendation_trade_threshold != null ? methodology.recommendation_trade_threshold : "?"} resolved trades, ${(methodology && methodology.recommendation_outcome_coverage_threshold != null ? methodology.recommendation_outcome_coverage_threshold * 100 : "?")}% outcome coverage, and ${(methodology && methodology.recommendation_lean_gap_threshold != null ? methodology.recommendation_lean_gap_threshold * 100 : "?")}%–${(methodology && methodology.recommendation_strong_gap_threshold != null ? methodology.recommendation_strong_gap_threshold * 100 : "?")}% win-rate separation. Auto-assignment stays paused, but the latest 180d winner can be manually approved with a required note.`,
    );
    children.push(note);
    clearNode(container, children);
  }

  function renderStrategies(payload) {
    if (!payload) return;
    strategyState.payload = payload;
    strategyState.windowDays = payload.summary && payload.summary.window_days ? payload.summary.window_days : strategyState.windowDays;
    strategyState.focusMode = normalizeStrategyFocusMode(strategyState.focusMode, payload.summary || {});
    if (!STRATEGY_CITY_FILTERS.includes(strategyState.cityFilter)) {
      strategyState.cityFilter = "all";
    }
    if (!(payload.summary && payload.summary.review_available) && strategyState.cityFilter === "needs_review") {
      strategyState.cityFilter = "all";
    }
    syncStrategySelections(payload);
    strategyState.dirty = false;
    const visibleCityRows = filteredCityRows(payload);
    ensureFocusDetailSelection(payload, visibleCityRows);
    renderFocusPanels(payload.summary || {});
    renderFocusSwitch(payload);
    renderWindowFilter(payload.summary || {});
    renderStrategiesSummary(payload);
    renderCodexLab(payload);
    renderReviewQueue(payload);
    renderStrategyLeaderboard(payload.leaderboard || []);
    renderCityFilters(payload);
    renderStrategyMatrix(payload, visibleCityRows);
    renderStrategiesDetail(payload.detail_context || {});
    renderReviewDetail(payload.detail_context || {});
    renderRecentStrategyChanges(payload.recent_promotions || []);
    renderMethodology(payload.methodology || {});
    if (strategyState.codexRunDetail && (strategyState.codexRunDetail.status === "queued" || strategyState.codexRunDetail.status === "running") && !strategyState.codexPollTimer) {
      scheduleCodexRunPolling(strategyState.codexRunDetail.id);
    }
    refreshTimestamps();
  }

  async function loadStrategies(options) {
    if (strategyState.fetching) return;
    if (!options || !options.preserveApprovalMessage) {
      setApprovalMessage(null, null, null);
    }
    if (options && Object.prototype.hasOwnProperty.call(options, "focusMode")) {
      strategyState.focusMode = options.focusMode || strategyState.focusMode;
    }
    if (options && Object.prototype.hasOwnProperty.call(options, "explicitSelection")) {
      strategyState.explicitSelection = Boolean(options.explicitSelection);
    }
    const focusMode = normalizeStrategyFocusMode(strategyState.focusMode, (strategyState.payload && strategyState.payload.summary) || {});
    strategyState.focusMode = focusMode;
    const next = {
      windowDays: options && options.windowDays ? options.windowDays : strategyState.windowDays,
      seriesTicker: null,
      strategyName: null,
    };
    if (focusMode === "cities" || focusMode === "review") {
      next.seriesTicker = options && Object.prototype.hasOwnProperty.call(options, "seriesTicker")
        ? options.seriesTicker
        : strategyState.selectedSeriesTicker;
    } else if (focusMode === "strategies") {
      next.strategyName = options && Object.prototype.hasOwnProperty.call(options, "strategyName")
        ? options.strategyName
        : strategyState.selectedStrategyName;
    }
    strategyState.fetching = true;
    const strategyMeta = document.getElementById("strategies-detail-meta");
    const cityMeta = document.getElementById("strategies-cities-detail-meta");
    const reviewMeta = document.getElementById("strategies-review-detail-meta");
    if (strategyMeta) strategyMeta.textContent = "Loading research snapshot...";
    if (cityMeta) cityMeta.textContent = "Loading research snapshot...";
    if (reviewMeta) reviewMeta.textContent = "Loading decision brief...";
    try {
      const response = await fetch(`/api/dashboard/strategies?${strategyQueryParams(next)}`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        if (redirectToLoginIfRequired(response, body)) return;
        return;
      }
      const payload = await response.json();
      renderStrategies(payload);
    } catch (_) {
      // skip on network error
    } finally {
      strategyState.fetching = false;
    }
  }

  function initStrategySort() {
    const select = document.getElementById("strategies-city-sort");
    if (!select) return;
    select.value = strategyState.sortKey;
    select.addEventListener("change", () => {
      strategyState.sortKey = select.value || "priority";
      if (strategyState.payload) renderStrategies(strategyState.payload);
    });
  }

  function initStrategySearch() {
    const input = document.getElementById("strategies-city-search");
    if (!input) return;
    input.value = strategyState.searchQuery;
    input.addEventListener("input", () => {
      strategyState.searchQuery = input.value || "";
      if (strategyState.payload) renderStrategies(strategyState.payload);
    });
  }

  async function refreshAll() {
    const activeEnv = currentDashboardEnv();
    if (dashboardMode === "single_site") {
      if (activeEnv === "strategies") {
        await loadStrategies({
          windowDays: strategyState.windowDays,
          focusMode: strategyState.focusMode,
          explicitSelection: strategyState.explicitSelection,
        });
      } else {
        await refreshEnv(activeEnv);
      }
      setLastRefreshed();
      refreshTimestamps();
      return;
    }

    await Promise.all([refreshEnv("demo"), refreshEnv("production")]);
    if (activeEnv === "strategies" && strategyState.payload) {
      await loadStrategies({
        windowDays: strategyState.windowDays,
        focusMode: strategyState.focusMode,
        explicitSelection: strategyState.explicitSelection,
      });
    }
    setLastRefreshed();
    refreshTimestamps();
  }

  initStrategySort();
  initStrategySearch();
  if (strategyState.payload) renderStrategies(strategyState.payload);
  refreshTimestamps();
  setLastRefreshed();
  setInterval(refreshAll, REFRESH_MS);
  setInterval(refreshTimestamps, 15_000);

  const refreshBtn = document.getElementById("dash-refresh-btn");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => {
      refreshBtn.disabled = true;
      refreshAll().finally(() => { refreshBtn.disabled = false; });
    });
  }
})();
