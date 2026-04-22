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

  function parseBootstrap() {
    const node = document.getElementById("strategies-bootstrap");
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_) {
      return null;
    }
  }

  const STRATEGY_FOCUS_MODES = ["review", "cities", "strategies"];
  const REVIEW_QUEUE_ORDER = [
    "drifted_assignment",
    "ready_for_approval",
    "evidence_weakened",
    "aligned",
    "waiting_for_evidence",
  ];

  function reviewPriority(status) {
    const index = REVIEW_QUEUE_ORDER.indexOf(status || "");
    return index === -1 ? REVIEW_QUEUE_ORDER.length : index;
  }

  function strategyFocusLabel(mode) {
    if (mode === "review") return "Review Queue";
    if (mode === "cities") return "Cities";
    return "Strategies";
  }

  function normalizeStrategyFocusMode(mode, summary) {
    if (mode === "review" && !(summary && summary.review_available)) return "cities";
    if (!STRATEGY_FOCUS_MODES.includes(mode || "")) return (summary && summary.review_available) ? "review" : "cities";
    return mode;
  }

  function cityMatchesSearch(row, query) {
    if (!query) return true;
    const haystack = [
      row.series_ticker,
      row.city_label,
      row.location_name,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(query);
  }

  function selectDefaultReviewCity(payload) {
    const rows = Array.isArray(payload && payload.city_matrix) ? payload.city_matrix.slice() : [];
    if (!rows.length) return null;
    rows.sort((a, b) => {
      const reviewDiff = reviewPriority((a.review || {}).status) - reviewPriority((b.review || {}).status);
      if (reviewDiff !== 0) return reviewDiff;
      const sortDiff = (a.sort_priority || 0) - (b.sort_priority || 0);
      if (sortDiff !== 0) return sortDiff;
      const assignmentGapDiff = (b.gap_to_assignment ?? -1) - (a.gap_to_assignment ?? -1);
      if (assignmentGapDiff !== 0) return assignmentGapDiff;
      return String(a.series_ticker || "").localeCompare(String(b.series_ticker || ""));
    });
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
    sortKey: "priority",
    focusMode: "review",
    searchQuery: "",
    fetching: false,
    dirty: false,
    approvalSubmitting: false,
    approvalMessage: null,
    approvalNotes: {},
    explicitSelection: false,
  };

  if (strategyState.payload && strategyState.payload.summary) {
    strategyState.windowDays = strategyState.payload.summary.window_days || 180;
    strategyState.focusMode = strategyState.payload.summary.review_available ? "review" : "cities";
    if (strategyState.payload.detail_context) {
      strategyState.selectedSeriesTicker = strategyState.payload.detail_context.selected_series_ticker || null;
      strategyState.selectedStrategyName = strategyState.payload.detail_context.selected_strategy_name || null;
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
      if (!resp.ok) return;
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
    return el(
      "div",
      `strategy-approval-message is-${message.tone || "neutral"}`,
      message.text,
    );
  }

  function responseErrorMessage(body, fallbackText) {
    if (body && typeof body.message === "string" && body.message) return body.message;
    if (body && Array.isArray(body.detail) && body.detail.length) {
      const first = body.detail[0];
      if (first && typeof first.msg === "string") return first.msg;
    }
    return fallbackText;
  }

  function renderStrategiesSummary(summary) {
    const container = document.getElementById("strategies-summary");
    if (!container) return;
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

    const strongValue = strategyStatValue(String(summary.strong_recommendations_count || 0), "value-positive");
    const strongDetail = strategyStatDetail("Strong recommendations");

    const needsReviewValue = strategyStatValue(
      summary.review_available ? String(summary.needs_review_count || 0) : "—",
      summary.review_available && (summary.needs_review_count || 0) > 0 ? "value-negative" : "",
    );
    const needsReviewDetail = strategyStatDetail(
      summary.review_available ? "Assignments that drifted or weakened" : `${summary.approval_window_days || 180}d only`,
    );

    const readyValue = strategyStatValue(
      summary.review_available ? String(summary.ready_for_approval_count || 0) : "—",
      summary.review_available && (summary.ready_for_approval_count || 0) > 0 ? "value-positive" : "",
    );
    const readyDetail = strategyStatDetail(
      summary.review_available ? "Unassigned cities with an eligible 180d winner" : `${summary.approval_window_days || 180}d only`,
    );

    const bestValue = strategyStatValue(summary.best_strategy_name || "—");
    const bestDetail = strategyStatDetail(summary.best_strategy_win_rate_display || "—");

    const approvalsValue = strategyStatValue(String(summary.recent_approvals_count || 0));
    const approvalsDetail = strategyStatDetail("Manual approvals in recent history");

    const assignValue = strategyStatValue(summary.assignments_covered_display || "—");
    const assignDetail = strategyStatDetail("Canonical assignments covered");

    const stats = [
      createSummaryStat("Window", windowValue, windowDetail),
      createSummaryStat("Last Regression Run", lastValue, lastDetail),
      createSummaryStat("Needs Review", needsReviewValue, needsReviewDetail),
      createSummaryStat("Ready for Approval", readyValue, readyDetail),
      createSummaryStat("Strong Recs", strongValue, strongDetail),
      createSummaryStat("Best Overall Strategy", bestValue, bestDetail),
      createSummaryStat("Recent Approvals", approvalsValue, approvalsDetail),
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
          seriesTicker: strategyState.selectedSeriesTicker,
          strategyName: strategyState.selectedSeriesTicker ? null : strategyState.selectedStrategyName,
          focusMode: strategyState.focusMode,
          explicitSelection: strategyState.explicitSelection,
        });
      });
      return button;
    });
    clearNode(container, buttons);
  }

  function maybeSelectDefaultReviewCity(payload) {
    if (!payload || !payload.summary || !payload.summary.review_available) return false;
    if (strategyState.focusMode !== "review") return false;
    if (strategyState.selectedSeriesTicker) return false;
    const nextCity = selectDefaultReviewCity(payload);
    if (!nextCity || !nextCity.series_ticker) return false;
    const reviewMeta = document.getElementById("strategies-review-detail-meta");
    const reviewContainer = document.getElementById("strategies-review-detail");
    if (reviewMeta) reviewMeta.textContent = `Loading ${nextCity.series_ticker} decision brief...`;
    if (reviewContainer) clearNode(reviewContainer, [el("p", "empty-state", "Loading decision brief...")]);
    loadStrategies({
      windowDays: strategyState.windowDays,
      seriesTicker: nextCity.series_ticker,
      strategyName: null,
      focusMode: "review",
      explicitSelection: false,
    });
    return true;
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

  function renderFocusSwitch(summary) {
    const container = document.getElementById("strategies-focus-switch");
    if (!container) return;
    const allowedModes = (summary && summary.review_available)
      ? STRATEGY_FOCUS_MODES
      : STRATEGY_FOCUS_MODES.filter((mode) => mode !== "review");
    strategyState.focusMode = normalizeStrategyFocusMode(strategyState.focusMode, summary || {});
    const buttons = allowedModes.map((mode) => {
      const button = el("button", `strategy-focus-pill${mode === strategyState.focusMode ? " is-active" : ""}`, strategyFocusLabel(mode));
      button.type = "button";
      button.dataset.focusMode = mode;
      button.setAttribute("aria-selected", mode === strategyState.focusMode ? "true" : "false");
      button.addEventListener("click", () => {
        if (strategyState.fetching) return;
        strategyState.focusMode = mode;
        renderFocusSwitch(summary || {});
        renderFocusPanels(summary || {});
        if (mode === "review" && !strategyState.selectedSeriesTicker) {
          if (maybeSelectDefaultReviewCity(strategyState.payload)) return;
        }
        renderStrategies(strategyState.payload);
      });
      return button;
    });
    clearNode(container, buttons);
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
      const card = el("article", `strategy-card${item.selected ? " is-selected" : ""}`);
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

      const counts = el("div", "strategy-card-counts");
      counts.append(
        pill(`${item.cities_led} lead${item.cities_led === 1 ? "" : "s"}`, "good"),
        pill(`${item.assigned_city_count} assigned`, item.assigned_city_count ? "neutral" : "warning"),
      );
      header.appendChild(counts);

      const metrics = el("div", "strategy-card-metrics");
      metrics.append(
        metricListItem("Win rate", item.overall_win_rate_display || "—", item.overall_win_rate >= 0.6 ? "value-positive" : item.overall_win_rate <= 0.35 ? "value-negative" : ""),
        metricListItem("Trade rate", item.overall_trade_rate_display || "—"),
        metricListItem("Total P/L", item.total_pnl_display || "—", item.total_pnl_dollars > 0 ? "value-positive" : item.total_pnl_dollars < 0 ? "value-negative" : ""),
        metricListItem("Avg edge", item.avg_edge_bps_display || "—"),
        metricListItem("Rooms", item.total_rooms_evaluated_display || "0"),
        metricListItem("Sim trades", item.total_trade_count_display || "0"),
        metricListItem("Scored trades", item.total_resolved_trade_count_display || "0"),
        metricListItem("Outcome coverage", item.outcome_coverage_display || "—"),
      );

      const details = el("details", "strategy-threshold-details");
      const summary = el("summary", "muted-label", "Threshold snapshot");
      details.append(summary, renderThresholdGroups(item.threshold_groups || []));

      card.append(header, metrics, details);
      return card;
    });
    clearNode(container, [el("div", "strategy-card-grid")]);
    container.firstChild.replaceChildren(...cards);
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

  function renderStrategyMatrix(payload) {
    const container = document.getElementById("strategies-city-matrix");
    const meta = document.getElementById("strategies-matrix-meta");
    if (!container) return;
    const query = strategyState.searchQuery.trim().toLowerCase();
    const allRows = Array.isArray(payload.city_matrix) ? sortCityRows(payload.city_matrix, strategyState.sortKey) : [];
    const rows = allRows.filter((row) => cityMatchesSearch(row, query));
    if (!rows.length) {
      clearNode(container, [el("p", "empty-state", query ? "No cities match the current search." : "No city comparison data yet.")]);
      if (meta) meta.textContent = query ? `0 of ${allRows.length} cities match the current search` : "No city-level comparison data";
      return;
    }
    if (meta) {
      meta.textContent = query
        ? `${rows.length} of ${allRows.length} cities match the current search`
        : `${rows.length} city rows in the selected window`;
    }

    const table = el("table", "positions-table strategy-matrix-table");
    const thead = el("thead");
    const headRow = el("tr");
    ["City", "Assignment", "Recommendation", "Review", "Gap", "Resolved Trades", "Coverage", "Best Strategy"].forEach((label) => headRow.append(el("th", null, label)));
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = el("tbody");
    rows.forEach((row) => {
      const recommendation = row.recommendation || {};
      const recommendationStatus = recommendation.status || "no_outcomes";
      const review = row.review || {};
      const tr = el(
        "tr",
        `strategy-matrix-row${row.selected ? " is-selected" : ""} is-${recommendationStatus.replace(/_/g, "-")}`,
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
      if (row.approval_eligible) {
        recommendationWrap.appendChild(pill(row.approval_label || "Ready to approve", recommendationTone(recommendationStatus)));
      }
      recommendationCell.appendChild(recommendationWrap);
      tr.appendChild(recommendationCell);

      const reviewCell = el("td");
      const reviewWrap = el("div", "strategy-matrix-cell");
      if (review.status && review.label) {
        reviewWrap.appendChild(pill(review.label, reviewTone(review.status)));
      } else {
        reviewWrap.appendChild(pill("180d only", "neutral"));
      }
      if (review.status) {
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

  function renderCityDecisionBrief(detail) {
    const city = detail.city || {};
    const approval = detail.approval || {};
    const review = detail.review || {};
    const recommendation = city.recommendation || {};
    const currentAssignment = review.current_assignment || city.assignment || {};
    const latestRecommendation = review.latest_recommendation || recommendation;
    const card = el("section", "strategy-detail-section strategy-decision-brief");
    card.append(el("h4", null, "Decision Brief"));

    const metrics = el("div", "strategy-detail-metrics");
    metrics.append(
      metricListItem("Current assignment", currentAssignment.strategy_name || "Unassigned"),
      metricListItem("Latest recommendation", latestRecommendation.strategy_name || recommendation.strategy_name || city.best_strategy || "—"),
      metricListItem("Gap to runner-up", latestRecommendation.gap_to_runner_up_display || city.gap_to_runner_up_display || "—"),
      metricListItem("Resolved trades", latestRecommendation.resolved_trade_count_display || city.best_resolved_trade_count_display || "0"),
      metricListItem("Outcome coverage", latestRecommendation.outcome_coverage_display || city.best_outcome_coverage_display || "—"),
      metricListItem("Status", recommendation.label || city.evidence_label || "—"),
    );
    if (review.available) {
      metrics.append(
        metricListItem("Review status", review.label || "—"),
        metricListItem("Next action", review.next_action_label || "Wait for evidence"),
      );
    }
    card.appendChild(metrics);

    const statusRow = el("div", "strategy-review-status-row");
    statusRow.append(
      currentAssignment.strategy_name ? pill(`Assigned ${currentAssignment.strategy_name}`, "neutral") : pill("Unassigned", "neutral"),
      recommendation.strategy_name ? pill(`Latest ${recommendation.strategy_name}`, recommendationTone(recommendation.status)) : pill("No recommendation", "neutral"),
    );
    if (review.available) {
      statusRow.append(pill(review.label || "Review", reviewTone(review.status)));
    }
    if (approval.eligible) {
      statusRow.append(pill(approval.label || "Ready to approve", "good"));
    }
    card.appendChild(statusRow);

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
      const label = el("label", "muted-label", "Operator note");
      label.setAttribute("for", `strategy-approval-note-${city.series_ticker}`);
      const textarea = el("textarea", "strategy-approval-textarea");
      textarea.id = `strategy-approval-note-${city.series_ticker}`;
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
      const button = el(
        "button",
        "dash-button primary-button",
        strategyState.approvalSubmitting ? "Approving..." : `Approve ${approval.strategy_name || recommendation.strategy_name || "recommendation"}`,
      );
      button.type = "button";
      button.disabled = strategyState.approvalSubmitting;
      button.addEventListener("click", () => {
        submitStrategyApproval(detail);
      });
      actions.appendChild(button);
      form.appendChild(actions);

      const messageNode = approvalMessageNode(city.series_ticker);
      if (messageNode) form.appendChild(messageNode);
      card.appendChild(form);
    } else {
      const standaloneMessage = approvalMessageNode(city.series_ticker);
      if (standaloneMessage) card.appendChild(standaloneMessage);
    }

    return card;
  }

  function renderCityDetail(detail, targetIds) {
    const container = document.getElementById(targetIds.containerId);
    const meta = document.getElementById(targetIds.metaId);
    if (!container) return;
    if (meta) {
      meta.textContent = detail.city
        ? `${detail.city.series_ticker} · ${detail.review && detail.review.available ? "decision brief and recommendation evidence" : "recommendation evidence"}`
        : "City evidence";
    }

    const children = [];
    const city = detail.city || {};
    const header = el("div", "strategy-detail-header");
    header.append(el("h3", null, city.series_ticker || "City detail"));
    header.append(el("p", "muted-label", city.city_label || city.location_name || "City-level strategy comparison"));
    children.push(header);
    children.push(renderCityDecisionBrief(detail));

    const rationale = detail.recommendation_rationale || detail.promotion_rationale || {};
    const rationaleCard = el("section", "strategy-detail-section");
    rationaleCard.append(el("h4", null, "Recommendation Rationale"));
    const rationaleGrid = el("div", "strategy-detail-metrics");
    rationaleGrid.append(
      metricListItem("Recommendation", rationale.recommendation_label || "—"),
      metricListItem("Best trade count", rationale.best_trade_count_display || "0"),
      metricListItem("Resolved trades", rationale.best_resolved_trade_count_display || "0"),
      metricListItem("Unscored trades", rationale.best_unscored_trade_count_display || "0"),
      metricListItem("Outcome coverage", rationale.best_outcome_coverage_display || "—"),
      metricListItem("Gap to runner-up", rationale.gap_to_runner_up_display || "—"),
      metricListItem("Gap to assignment", rationale.gap_to_current_assignment_display || "—"),
      metricListItem("Winner Wilson", rationale.winner_wilson_display || "—"),
      metricListItem("Runner-up Wilson", rationale.runner_up_wilson_display || "—"),
      metricListItem("Resolved rule", rationale.meets_trade_threshold ? "Pass" : "Below threshold", rationale.meets_trade_threshold ? "value-positive" : "value-negative"),
      metricListItem("Coverage rule", rationale.meets_coverage_threshold ? "Pass" : "Below threshold", rationale.meets_coverage_threshold ? "value-positive" : "value-negative"),
      metricListItem("Strong gap", rationale.meets_gap_threshold ? "Pass" : "Below threshold", rationale.meets_gap_threshold ? "value-positive" : "value-negative"),
      metricListItem("Lean gap", rationale.meets_lean_gap_threshold ? "Pass" : "Below threshold", rationale.meets_lean_gap_threshold ? "value-positive" : "value-neutral"),
      metricListItem("Writes assignment", rationale.writes_assignment ? "Yes" : "No", rationale.writes_assignment ? "value-positive" : "value-neutral"),
    );
    rationaleCard.appendChild(rationaleGrid);
    children.push(rationaleCard);

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
    trendSection.append(el("h4", null, detail.trend && detail.trend.title ? detail.trend.title : "Trend"));
    if (detail.trend && detail.trend.note) trendSection.append(el("p", "muted-label", detail.trend.note));
    const trendSeries = (detail.trend && detail.trend.series ? detail.trend.series : []).map((item) => ({
      strategy_name: item.strategy_name,
      label: item.strategy_name,
      points: item.points || [],
    }));
    trendSection.appendChild(chartCard("Win Rate Over Time", "Stored regression snapshots", trendSeries, "win_rate"));
    children.push(trendSection);

    const eventSection = el("section", "strategy-detail-section");
    eventSection.append(el("h4", null, "Recent Strategy Changes"));
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
    children.push(header);

    const metricGrid = el("div", "strategy-detail-metrics");
    metricGrid.append(
      metricListItem("Win rate", strategy.overall_win_rate_display || "—", strategy.overall_win_rate >= 0.6 ? "value-positive" : strategy.overall_win_rate <= 0.35 ? "value-negative" : ""),
      metricListItem("Trade rate", strategy.overall_trade_rate_display || "—"),
      metricListItem("Outcome coverage", strategy.outcome_coverage_display || "—"),
      metricListItem("Total P/L", strategy.total_pnl_display || "—", strategy.total_pnl_dollars > 0 ? "value-positive" : strategy.total_pnl_dollars < 0 ? "value-negative" : ""),
      metricListItem("Avg edge", strategy.avg_edge_bps_display || "—"),
      metricListItem("Scored trades", strategy.total_resolved_trade_count_display || "0"),
      metricListItem("Cities led", String(strategy.cities_led || 0)),
      metricListItem("Assigned cities", String(strategy.assigned_city_count || 0)),
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
    if (!detail || detail.type !== "city") {
      if (meta) meta.textContent = strategyState.fetching ? "Loading decision brief..." : "Select a city from the review queue to inspect the current decision brief.";
      clearNode(container, [el("p", "empty-state", strategyState.fetching ? "Loading decision brief..." : "No city decision brief selected yet.")]);
      return;
    }
    renderCityDetail(detail, {
      containerId: "strategies-review-detail",
      metaId: "strategies-review-detail-meta",
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
    if (detail.type === "city") {
      renderCityDetail(detail, targetIds);
      return;
    }
    if (detail.type === "strategy") {
      renderStrategyDetail(detail, targetIds);
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
    strategyState.selectedSeriesTicker = payload.detail_context ? payload.detail_context.selected_series_ticker || null : null;
    strategyState.selectedStrategyName = payload.detail_context ? payload.detail_context.selected_strategy_name || null : null;
    strategyState.focusMode = normalizeStrategyFocusMode(strategyState.focusMode, payload.summary || {});
    strategyState.dirty = false;
    renderFocusPanels(payload.summary || {});
    renderFocusSwitch(payload.summary || {});
    renderWindowFilter(payload.summary || {});
    renderStrategiesSummary(payload.summary || {});
    renderReviewQueue(payload);
    renderStrategyLeaderboard(payload.leaderboard || []);
    renderStrategyMatrix(payload);
    renderStrategiesDetail(payload.detail_context || {});
    renderReviewDetail(payload.detail_context || {});
    renderRecentStrategyChanges(payload.recent_promotions || []);
    renderMethodology(payload.methodology || {});
    if (!strategyState.explicitSelection && maybeSelectDefaultReviewCity(payload)) return;
    refreshTimestamps();
  }

  async function loadStrategies(options) {
    if (strategyState.fetching) return;
    if (!options || !options.preserveApprovalMessage) {
      setApprovalMessage(null, null, null);
    }
    const next = {
      windowDays: options && options.windowDays ? options.windowDays : strategyState.windowDays,
      seriesTicker: options && Object.prototype.hasOwnProperty.call(options, "seriesTicker") ? options.seriesTicker : strategyState.selectedSeriesTicker,
      strategyName: options && Object.prototype.hasOwnProperty.call(options, "strategyName") ? options.strategyName : strategyState.selectedStrategyName,
    };
    if (options && Object.prototype.hasOwnProperty.call(options, "focusMode")) {
      strategyState.focusMode = options.focusMode || strategyState.focusMode;
    }
    if (options && Object.prototype.hasOwnProperty.call(options, "explicitSelection")) {
      strategyState.explicitSelection = Boolean(options.explicitSelection);
    }
    if (next.seriesTicker) next.strategyName = null;
    if (!next.seriesTicker && !next.strategyName && strategyState.payload && strategyState.payload.detail_context) {
      next.strategyName = strategyState.payload.detail_context.selected_strategy_name || null;
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
      if (!response.ok) return;
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
      if (strategyState.payload) renderStrategyMatrix(strategyState.payload);
    });
  }

  function initStrategySearch() {
    const input = document.getElementById("strategies-city-search");
    if (!input) return;
    input.value = strategyState.searchQuery;
    input.addEventListener("input", () => {
      strategyState.searchQuery = input.value || "";
      if (strategyState.payload) renderStrategyMatrix(strategyState.payload);
    });
  }

  async function refreshAll() {
    const activeEnv = currentDashboardEnv();
    if (dashboardMode === "single_site") {
      if (activeEnv === "strategies") {
        await loadStrategies({
          windowDays: strategyState.windowDays,
          seriesTicker: strategyState.selectedSeriesTicker,
          strategyName: strategyState.selectedSeriesTicker ? null : strategyState.selectedStrategyName,
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
        seriesTicker: strategyState.selectedSeriesTicker,
        strategyName: strategyState.selectedSeriesTicker ? null : strategyState.selectedStrategyName,
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
})();
