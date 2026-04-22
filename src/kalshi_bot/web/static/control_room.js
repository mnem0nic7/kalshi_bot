(function () {
  const bootstrapNode = document.getElementById("control-room-bootstrap");
  if (!bootstrapNode) {
    return;
  }

  const bootstrap = JSON.parse(bootstrapNode.textContent || "{}");
  const summaryContainer = document.getElementById("control-room-summary");
  const page = document.getElementById("control-room-page");
  const refreshStateNode = document.getElementById("control-room-refresh-state");
  const lastRefreshedNode = document.getElementById("control-room-last-refreshed");
  const modal = document.getElementById("control-room-modal");
  const modalBody = document.getElementById("control-room-modal-body");
  const modalConfirm = document.getElementById("control-room-modal-confirm");
  const TABS = Array.isArray(bootstrap.tabs) ? bootstrap.tabs : [];

  if (!page || !summaryContainer || !refreshStateNode || !lastRefreshedNode || !modal || !modalBody || !modalConfirm) {
    return;
  }

  const state = {
    summary: bootstrap.summary || {},
    tabs: { overview: bootstrap.initial_tab_payload || null },
    activeTab: "overview",
    refreshIntervalMs: Math.max(Number(bootstrap.refresh_interval_seconds || 15) * 1000, 5000),
    refreshTimer: null,
    refreshEnabled: true,
    loadingTabs: new Set(),
    pendingConfirmation: null,
    trainingSubtab: "quality",
    researchFilters: {
      status: "active",
      series: "all",
      query: "",
    },
    roomsFilters: {
      status: "all",
      origin: "all",
      query: "",
    },
    operationsFilters: {
      severity: "all",
    },
  };

  function element(tagName, classNames, text) {
    const node = document.createElement(tagName);
    if (classNames) {
      const classes = Array.isArray(classNames) ? classNames : [classNames];
      node.classList.add(...classes.filter(Boolean));
    }
    if (text !== undefined) {
      node.textContent = text;
    }
    return node;
  }

  function button(label, classNames, options) {
    const node = element("button", ["button"].concat(Array.isArray(classNames) ? classNames : [classNames]), label);
    node.type = "button";
    Object.entries(options || {}).forEach(([key, value]) => {
      if (key === "dataset" && value) {
        Object.entries(value).forEach(([datasetKey, datasetValue]) => {
          node.dataset[datasetKey] = String(datasetValue);
        });
        return;
      }
      if (key === "disabled") {
        node.disabled = Boolean(value);
        return;
      }
      node.setAttribute(key, String(value));
    });
    return node;
  }

  function linkButton(label, href, classNames) {
    const node = element("a", ["button"].concat(Array.isArray(classNames) ? classNames : [classNames]), label);
    node.href = href;
    return node;
  }

  function statusPill(label, tone) {
    const classes = ["status-pill"];
    if (tone === "good" || tone === "success" || tone === "healthy") {
      classes.push("status-good", "status-success");
    } else if (tone === "warning" || tone === "stand_down") {
      classes.push("status-warning");
    } else if (tone === "bad" || tone === "danger" || tone === "critical" || tone === "blocked" || tone === "failed") {
      classes.push("status-bad", "status-danger");
    } else {
      classes.push("status-neutral");
    }
    return element("span", classes, label);
  }

  function infoCard(label, value, detail) {
    const node = element("div", "info-card");
    node.append(element("span", "muted-label", label), element("strong", null, value));
    if (detail) {
      node.append(element("span", "muted", detail));
    }
    return node;
  }

  function compactStatGrid(entries) {
    const grid = element("div", "info-grid");
    entries.forEach((entry) => {
      grid.append(infoCard(entry.label, entry.value, entry.detail));
    });
    return grid;
  }

  function parseDate(value) {
    if (!value) {
      return null;
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return null;
    }
    return date;
  }

  function formatDateTime(value) {
    const date = parseDate(value);
    if (!date) {
      return value || "n/a";
    }
    return date.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function formatNumber(value, digits) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
      return String(value);
    }
    return numeric.toFixed(digits === undefined ? 2 : digits);
  }

  function formatInteger(value) {
    if (value === null || value === undefined || value === "") {
      return "0";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
      return String(value);
    }
    return String(Math.round(numeric));
  }

  function formatRelativeTime(value, options) {
    const date = parseDate(value);
    if (!date) {
      return value || "n/a";
    }
    const now = new Date();
    const deltaMs = date.getTime() - now.getTime();
    const absoluteSeconds = Math.round(Math.abs(deltaMs) / 1000);
    const future = deltaMs > 0;
    let amount = absoluteSeconds;
    let unit = "s";
    if (absoluteSeconds >= 86400) {
      amount = Math.round(absoluteSeconds / 86400);
      unit = "d";
    } else if (absoluteSeconds >= 3600) {
      amount = Math.round(absoluteSeconds / 3600);
      unit = "h";
    } else if (absoluteSeconds >= 60) {
      amount = Math.round(absoluteSeconds / 60);
      unit = "m";
    }
    if (absoluteSeconds < 5 && !(options && options.futurePreferred)) {
      return "just now";
    }
    if (future || (options && options.futurePreferred)) {
      return `in ${amount}${unit}`;
    }
    return `${amount}${unit} ago`;
  }

  function formatDurationSeconds(value) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    const seconds = Math.round(Number(value));
    if (Number.isNaN(seconds)) {
      return String(value);
    }
    if (seconds >= 86400) {
      return `${Math.round(seconds / 86400)}d`;
    }
    if (seconds >= 3600) {
      return `${Math.round(seconds / 3600)}h`;
    }
    if (seconds >= 60) {
      return `${Math.round(seconds / 60)}m`;
    }
    return `${seconds}s`;
  }

  function exactTimestamp(value) {
    const date = parseDate(value);
    if (!date) {
      return value || "";
    }
    return date.toISOString();
  }

  function applyRelativeTime(node, value, options) {
    node.textContent = formatRelativeTime(value, options);
    const exact = exactTimestamp(value);
    if (exact) {
      node.title = exact;
    }
  }

  function formatKey(key) {
    return String(key || "")
      .replaceAll("_", " ")
      .replace(/\b\w/g, (character) => character.toUpperCase());
  }

  function clearNode(node) {
    node.replaceChildren();
  }

  function setRefreshState(text, tone) {
    refreshStateNode.textContent = text;
    refreshStateNode.className = "status-pill";
    if (tone === "good") {
      refreshStateNode.classList.add("status-good", "status-success");
    } else if (tone === "warning") {
      refreshStateNode.classList.add("status-warning");
    } else if (tone === "bad") {
      refreshStateNode.classList.add("status-bad", "status-danger");
    } else {
      refreshStateNode.classList.add("status-neutral");
    }
  }

  function currentHistoricalWindow(days) {
    const end = new Date();
    end.setUTCDate(end.getUTCDate() - 1);
    const start = new Date(end.getTime());
    start.setUTCDate(start.getUTCDate() - Math.max(days - 1, 0));
    return {
      date_from: start.toISOString().slice(0, 10),
      date_to: end.toISOString().slice(0, 10),
    };
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, {
      headers: { "Content-Type": "application/json" },
      ...options,
    });
    if (!response.ok) {
      let message = `${response.status} ${response.statusText}`;
      try {
        const payload = await response.json();
        message = payload.detail || payload.error || message;
      } catch (_error) {
        // keep default message
      }
      throw new Error(message);
    }
    return response.json();
  }

  async function postJson(url, payload) {
    return fetchJson(url, {
      method: "POST",
      body: JSON.stringify(payload || {}),
    });
  }

  function buildSparkline(values) {
    const container = element("div", "summary-sparkline");
    const list = Array.isArray(values) ? values.filter((value) => typeof value === "number") : [];
    if (!list.length) {
      container.append(element("span", "muted", "No trend yet"));
      return container;
    }
    const maxValue = Math.max(...list, 1);
    list.forEach((value) => {
      const bar = element("span", "summary-sparkline-bar");
      const height = Math.max(12, Math.round((value / maxValue) * 100));
      bar.style.height = `${height}%`;
      bar.title = formatNumber(value, 2);
      container.append(bar);
    });
    return container;
  }

  function summaryCard(config) {
    const extraClass = config.critical ? "summary-card-critical" : config.warn ? "summary-card-warn" : null;
    const card = element("article", ["summary-card"].concat(extraClass ? [extraClass] : []));
    const heading = element("div", "summary-card-heading");
    heading.append(element("span", "summary-card-label", config.label));
    if (config.pill) {
      heading.append(statusPill(config.pill.label, config.pill.tone));
    }
    card.append(heading, element("strong", "summary-card-value", config.value));
    if (config.detail) {
      card.append(element("span", "summary-card-detail", config.detail));
    }
    if (config.subdetail) {
      card.append(element("span", "summary-card-subdetail", config.subdetail));
    }
    if (config.sparkline) {
      card.append(buildSparkline(config.sparkline));
    }
    return card;
  }

  function renderSummary(summary) {
    clearNode(summaryContainer);
    const systemStatus = summary.system_status || {};
    const activeDeployment = summary.active_deployment || {};
    const openPositions = summary.open_positions || {};
    const researchConfidence = summary.research_confidence || {};
    const roomOutcomes = summary.room_outcomes || {};
    const errorAlert = summary.error_alert || {};
    const resolvedRoomCount = Number(roomOutcomes.resolved_total || 0);

    const summaryCards = [
      summaryCard({
        label: "System Status",
        value: String(systemStatus.label || "Unknown").toUpperCase(),
        detail: systemStatus.detail || "No runtime story available.",
        subdetail: `Active ${String(systemStatus.active_color || "unknown").toUpperCase()}`,
        pill: {
          label: String(systemStatus.level || "unknown").toUpperCase(),
          tone: systemStatus.level || "neutral",
        },
        critical: systemStatus.level === "critical",
      }),
      summaryCard({
        label: "Active Deployment",
        value: String(activeDeployment.active_color || "unknown").toUpperCase(),
        detail: `Watchdog ${formatRelativeTime(activeDeployment.watchdog_updated_at)}`,
        subdetail: `Last action: ${((activeDeployment.last_action || {}).action) || "n/a"}`,
        pill: {
          label: activeDeployment.kill_switch_enabled ? "KILL SWITCH ON" : "WATCHDOG",
          tone: activeDeployment.kill_switch_enabled ? "bad" : "neutral",
        },
      }),
      summaryCard({
        label: "Open Positions",
        value: formatInteger(openPositions.count || 0),
        detail: `Tracked contracts: ${openPositions.total_contracts || "0.00"}`,
      }),
      summaryCard({
        label: "Research Confidence",
        value: researchConfidence.average === null || researchConfidence.average === undefined ? "n/a" : formatNumber(researchConfidence.average, 2),
        detail: `${formatInteger(researchConfidence.count || 0)} active dossiers`,
        sparkline: researchConfidence.sparkline || [],
      }),
      summaryCard({
        label: "Active Rooms",
        value: formatInteger(roomOutcomes.running || 0),
        detail: roomOutcomes.running > 0 ? "rooms in progress" : "no rooms running",
        pill: {
          label: roomOutcomes.running > 0 ? "LIVE" : "IDLE",
          tone: roomOutcomes.running > 0 ? "good" : "neutral",
        },
      }),
      summaryCard({
        label: "Room Outcomes",
        value: `${formatInteger(roomOutcomes.succeeded || 0)}/${formatInteger(resolvedRoomCount)}`,
        detail: `Resolved last ${formatInteger(roomOutcomes.window_hours || 24)}h`,
        subdetail: `blocked ${formatInteger(roomOutcomes.blocked || 0)} · stand down ${formatInteger(roomOutcomes.stand_down || 0)} · failed ${formatInteger(roomOutcomes.failed || 0)}`,
        critical: resolvedRoomCount > 0 && Number(roomOutcomes.succeeded || 0) === 0,
      }),
      summaryCard({
        label: "Error Events",
        value: formatInteger(errorAlert.error_count || 0),
        detail: `${formatInteger(errorAlert.warning_count || 0)} warnings (24h)`,
        subdetail: errorAlert.most_recent || "No recent errors.",
        pill: {
          label: (errorAlert.error_count || 0) > 0 ? "ERRORS" : (errorAlert.warning_count || 0) > 0 ? "WARNINGS" : "CLEAR",
          tone: (errorAlert.error_count || 0) > 0 ? "bad" : (errorAlert.warning_count || 0) > 0 ? "warning" : "good",
        },
        critical: (errorAlert.error_count || 0) > 0,
        warn: (errorAlert.error_count || 0) === 0 && (errorAlert.warning_count || 0) > 0,
      }),
    ];
    summaryContainer.replaceChildren(...summaryCards);

    renderIntelBoard(summary);

    state.summary = summary;
    lastRefreshedNode.dataset.timestamp = summary.as_of || "";
    applyRelativeTime(lastRefreshedNode, summary.as_of);
  }

  function renderIntelBoard(summary) {
    const boardEl = document.getElementById("control-room-intel-board");
    if (!boardEl) return;
    const rows = Array.isArray(summary.current_intel_board) ? summary.current_intel_board : [];
    if (!rows.length) {
      boardEl.replaceChildren();
      return;
    }
    const card = element("section", "control-card");
    const header = element("div", "control-card-header");
    header.append(
      element("h2", null, "Current Market Intel"),
      element("p", null, "Gate-passing markets listed first, sorted by confidence. Blocked rows show the first blocking reason.")
    );
    const table = buildTable(
      [
        { label: "Market", render: (r) => r.ticker },
        { label: "Gate", render: (r) => r.ticker }, // overwritten below with pills
        { label: "Fair YES", render: (r) => r.fair_yes_dollars ? "$".concat(r.fair_yes_dollars) : "—" },
        {
          label: "Confidence",
          render: (r) => {
            if (r.confidence == null) return element("span", "muted", "—");
            const wrap = element("div", "intel-conf");
            const bar = element("div", "intel-conf-bar");
            bar.style.width = `${Math.round(r.confidence * 100)}%`;
            bar.classList.add(r.confidence >= 0.9 ? "confidence-high" : r.confidence >= 0.75 ? "confidence-medium" : "confidence-low");
            wrap.append(element("span", null, formatNumber(r.confidence, 2)), bar);
            return wrap;
          },
        },
        { label: "Age", render: (r) => r.age_seconds != null ? formatDurationSeconds(r.age_seconds) : "—" },
        { label: "Why Blocked", render: (r) => (r.gate_reasons || [])[0] || (r.gate_passed ? "" : "unknown") },
      ],
      rows
    );
    // Replace plain-text gate cells with colour-coded pills (safe DOM only)
    const gateCells = table.querySelectorAll("td:nth-child(2)");
    rows.forEach((r, i) => {
      if (!gateCells[i]) return;
      const pill = statusPill(r.gate_passed ? "PASS" : "BLOCKED", r.gate_passed ? "good" : "bad");
      if (r.stale && r.gate_passed) {
        gateCells[i].replaceChildren(pill, statusPill("STALE", "warning"));
      } else {
        gateCells[i].replaceChildren(pill);
      }
    });
    card.append(header, table);
    boardEl.replaceChildren(card);
  }

  function toneFromBoolean(value) {
    return value ? "good" : "bad";
  }

  function keyValueRows(payload, options) {
    const entries = Object.entries(payload || {});
    const rows = entries.map(([key, value]) => ({
      key: formatKey(key),
      value: typeof value === "object" && value !== null ? JSON.stringify(value) : String(value),
    }));
    return rows.slice(0, options && options.limit ? options.limit : rows.length);
  }

  function buildTable(columns, rows, emptyText) {
    if (!rows.length) {
      return element("p", "empty-state", emptyText || "No rows to show.");
    }
    const wrap = element("div", "table-wrap");
    const table = element("table", "data-table");
    const thead = element("thead");
    const headRow = element("tr");
    columns.forEach((column) => {
      headRow.append(element("th", null, column.label));
    });
    thead.append(headRow);

    const tbody = element("tbody");
    rows.forEach((row) => {
      const tr = element("tr");
      columns.forEach((column) => {
        const td = element("td");
        const content = column.render(row);
        if (content instanceof Node) {
          td.append(content);
        } else {
          td.textContent = content;
        }
        if (column.className) {
          td.classList.add(column.className);
        }
        tr.append(td);
      });
      tbody.append(tr);
    });
    table.append(thead, tbody);
    wrap.append(table);
    return wrap;
  }

  function buildLimitedList(rows, limit, renderer) {
    const wrapper = element("div", "stack");
    const items = rows.map(renderer);
    if (items.length <= limit) {
      wrapper.replaceChildren(...items);
      return wrapper;
    }

    items.forEach((item, index) => {
      if (index >= limit) {
        item.hidden = true;
      }
      wrapper.append(item);
    });

    const showMoreButton = button(`Show ${items.length - limit} more`, "button-ghost show-more");
    let expanded = false;
    showMoreButton.addEventListener("click", () => {
      expanded = !expanded;
      items.forEach((item, index) => {
        item.hidden = !expanded && index >= limit;
      });
      showMoreButton.textContent = expanded ? "Show less" : `Show ${items.length - limit} more`;
    });
    wrapper.append(showMoreButton);
    return wrapper;
  }

  function accordionCard(title, summaryText, content, open) {
    const details = document.createElement("details");
    details.classList.add("accordion-card");
    if (open) {
      details.open = true;
    }
    const summary = element("summary");
    summary.append(
      element("span", "accordion-title", title),
      element("span", "accordion-summary", summaryText)
    );
    const body = element("div", "accordion-body");
    if (content instanceof Node) {
      body.append(content);
    } else {
      body.append(element("p", "muted", String(content)));
    }
    details.append(summary, body);
    return details;
  }

  function renderOverviewPanel(payload) {
    const panel = document.getElementById("tab-panel-overview");
    if (!panel) {
      return;
    }
    clearNode(panel);
    const grid = element("div", "control-room-grid");
    const control = payload.control || {};
    const runtimeHealth = payload.runtime_health || {};
    const selfImprove = payload.self_improve || {};
    const heuristics = payload.heuristics || {};

    const actionsCard = element("article", ["control-card", "control-card-actions"]);
    const actionsHeader = element("div", "control-card-header");
    actionsHeader.append(element("h2", null, "Deployment Controls"));
    const groups = element("div", "control-action-groups");
    const safetyGroup = element("div", "action-group");
    safetyGroup.append(
      element("span", "action-group-label", "Safety"),
      (() => {
        const row = element("div", "actions");
        row.append(
          button("Enable Kill Switch", ["button-danger"], { dataset: { action: "toggleKill", enabled: "true" } }),
          button("Disable Kill Switch", ["button-secondary"], { dataset: { action: "toggleKill", enabled: "false" } })
        );
        return row;
      })()
    );
    const deploymentGroup = element("div", "action-group");
    deploymentGroup.append(
      element("span", "action-group-label", "Deployment"),
      (() => {
        const row = element("div", "actions");
        row.append(
          button("Promote Blue", ["button-primary"], { dataset: { action: "promoteColor", color: "blue" } }),
          button("Promote Green", ["button-primary"], { dataset: { action: "promoteColor", color: "green" } })
        );
        return row;
      })()
    );
    groups.append(safetyGroup, deploymentGroup);
    actionsCard.append(actionsHeader, groups);

    const runtimeCard = element("article", ["control-card", "control-card-wide"]);
    const runtimeHeader = element("div", "control-card-header");
    runtimeHeader.append(element("h2", null, "Runtime Health"));
    const runtimeGrid = element("div", "runtime-health-grid");
    Object.entries(runtimeHealth.colors || {}).forEach(([color, health]) => {
      const card = element("div", "runtime-card");
      const top = element("div", "runtime-card-top");
      top.append(
        element("strong", null, color.toUpperCase()),
        statusPill(health.combined_healthy ? "healthy" : "degraded", health.combined_healthy ? "good" : "bad")
      );
      card.append(
        top,
        element("span", null, `app ${((health.app || {}).status) || "unknown"}`),
        element("span", null, `daemon ${((health.daemon || {}).healthy) ? "healthy" : "unhealthy"}`),
        element("span", "muted", `heartbeat ${formatDurationSeconds((health.daemon || {}).heartbeat_age_seconds)}`),
        element("span", "muted", `reconcile ${(health.daemon || {}).last_reconcile_at ? formatDateTime((health.daemon || {}).last_reconcile_at) : "never"}`)
      );
      runtimeGrid.append(card);
    });
    runtimeCard.append(runtimeHeader, runtimeGrid);

    const blockerCard = element("article", ["control-card", "control-card-narrow"]);
    blockerCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Top Blockers"));
        return header;
      })(),
      (() => {
        const list = element("div", "token-list");
        const blockers = Array.isArray(payload.top_blockers) ? payload.top_blockers : [];
        if (!blockers.length) {
          list.append(element("span", "token-chip", "No blockers reported."));
        } else {
          blockers.forEach((item) => list.append(element("span", ["token-chip", "token-chip-bad"], item)));
        }
        return list;
      })()
    );

    const nextActionCard = element("article", ["control-card", "control-card-narrow"]);
    nextActionCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Next Actions"));
        return header;
      })(),
      (() => {
        const list = element("div", "token-list");
        const actions = Array.isArray(payload.next_actions) ? payload.next_actions : [];
        if (!actions.length) {
          list.append(element("span", "token-chip", "No actions needed."));
        } else {
          actions.forEach((item) => list.append(element("span", ["token-chip", "token-chip-good"], item)));
        }
        return list;
      })()
    );

    const recentOpsCard = element("article", ["control-card", "control-card-medium"]);
    recentOpsCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Recent Ops"));
        return header;
      })()
    );
    const opsList = element("div", "compact-list");
    const severityRank = { error: 0, warning: 1, info: 2 };
    const opsEvents = (Array.isArray(payload.ops_events) ? payload.ops_events : [])
      .slice()
      .sort((a, b) => (severityRank[a.severity] ?? 3) - (severityRank[b.severity] ?? 3));
    if (!opsEvents.length) {
      opsList.append(element("p", "empty-state", "No ops events recorded yet."));
    } else {
      opsEvents.forEach((eventItem) => {
        const row = element("div", "compact-row");
        const body = element("div", "compact-row-body");
        const createdAt = element("span", "muted");
        applyRelativeTime(createdAt, eventItem.created_at);
        body.append(element("strong", null, eventItem.summary || "Event"), element("span", null, eventItem.source || "unknown"), createdAt);
        row.append(statusPill(eventItem.severity || "info", eventItem.severity === "error" ? "bad" : eventItem.severity === "warning" ? "warning" : "neutral"), body);
        opsList.append(row);
      });
    }
    recentOpsCard.append(opsList);

    const positionsCard = element("article", ["control-card", "control-card-wide"]);
    const positionsHeader = element("div", "control-card-header");
    positionsHeader.append(element("h2", null, "Open Positions"));
    positionsCard.append(positionsHeader);
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    if (!positions.length) {
      positionsCard.append(element("p", "empty-state", "No open positions."));
    } else {
      const tbl = element("table", "positions-table");
      const thead = element("thead");
      const hrow = element("tr");
      ["Market", "Side", "Contracts", "Avg Price", "Notional"].forEach((h) => hrow.append(element("th", null, h)));
      thead.append(hrow);
      const tbody = element("tbody");
      positions.forEach((pos) => {
        const row = element("tr");
        row.append(
          element("td", "mono", pos.market_ticker),
          (() => { const td = element("td"); td.append(statusPill(pos.side, pos.side === "yes" ? "good" : "warning")); return td; })(),
          element("td", "mono", pos.count_fp),
          element("td", "mono", `$${pos.average_price_dollars}`),
          element("td", "mono", `$${pos.notional_dollars}`)
        );
        tbody.append(row);
      });
      tbl.append(thead, tbody);
      positionsCard.append(tbl);
    }

    grid.append(actionsCard, positionsCard, runtimeCard, blockerCard, nextActionCard, recentOpsCard);
    panel.append(grid);
  }

  function renderObjectTable(payload, emptyText) {
    const rows = Object.entries(payload || {}).map(([key, value]) => ({
      key: formatKey(key),
      value: typeof value === "object" && value !== null ? JSON.stringify(value) : String(value),
    }));
    return buildTable(
      [
        { label: "Metric", render: (row) => row.key },
        { label: "Value", render: (row) => row.value },
      ],
      rows,
      emptyText
    );
  }

  function renderTrainingSubtab(tabKey) {
    const panel = document.getElementById("tab-panel-training");
    const payload = state.tabs.training || {};
    if (!panel) {
      return;
    }
    clearNode(panel);

    const shell = element("div", "stack");
    const subtabs = element("div", "training-subtabs");
    [
      { id: "quality", label: "Quality" },
      { id: "historical", label: "Historical" },
      { id: "pipeline", label: "Pipeline" },
      { id: "backlog", label: "Backlog" },
    ].forEach((subtab) => {
      const pill = button(subtab.label, ["button-secondary", "pill-button"].concat(state.trainingSubtab === subtab.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.trainingSubtab = subtab.id;
        renderTrainingSubtab(subtab.id);
      });
      subtabs.append(pill);
    });
    shell.append(subtabs);

    if (tabKey === "quality") {
      const quality = payload.quality || {};
      shell.append(
        accordionCard(
          "Build Actions",
          "Grouped historical and room dataset actions",
          (() => {
            const wrap = element("div", "action-stack");
            const rows = element("div", "actions");
            rows.append(
              button("Build Room Bundles", ["button-primary"], { dataset: { action: "buildRoomBundles" } }),
              button("Build Historical Bundles", ["button-secondary"], { dataset: { action: "buildHistoricalBundles" } }),
              button("Build Outcome Eval", ["button-secondary"], { dataset: { action: "buildOutcomeEval" } }),
              button("Build Decision Eval", ["button-secondary"], { dataset: { action: "buildDecisionEval" } }),
              button("Draft Gemini Export", ["button-secondary"], { dataset: { action: "buildGeminiDraft" } })
            );
            wrap.append(rows);
            return wrap;
          })(),
          true
        ),
        accordionCard(
          "Quality Debt",
          `stale ${formatInteger((quality.summary || {}).stale_mismatch_count || 0)} · missed ${formatInteger((quality.summary || {}).missed_stand_down_count || 0)}`,
          compactStatGrid(
            Object.entries(quality.summary || {}).map(([key, value]) => ({
              label: formatKey(key),
              value: String(value),
            }))
          ),
          true
        ),
        accordionCard(
          "Exclusion Reasons",
          `${Object.keys(quality.exclusion_reasons || {}).length} tracked reasons`,
          renderObjectTable(quality.exclusion_reasons || {}, "No exclusion reasons yet."),
          false
        ),
        accordionCard(
          "Recent Exclusion Memory",
          `${(quality.recent_exclusion_memory || []).length} recent markets`,
          buildTable(
            [
              { label: "Market", render: (row) => row.market_ticker || "n/a" },
              { label: "Reason", render: (row) => row.exclude_reason || row.reason || "n/a" },
              { label: "Count", render: (row) => formatInteger(row.count || 0) },
            ],
            (quality.recent_exclusion_memory || []).slice(0, 12),
            "No recent exclusion memory."
          ),
          false
        ),
        accordionCard(
          "Recent Builds",
          `${(quality.recent_builds || []).length} dataset runs`,
          buildTable(
            [
              { label: "Mode", render: (row) => row.mode || "n/a" },
              { label: "Status", render: (row) => row.status || "n/a" },
              { label: "Rooms", render: (row) => formatInteger(row.room_count || 0) },
              { label: "Created", render: (row) => formatRelativeTime(row.created_at) },
            ],
            (quality.recent_builds || []).slice(0, 8),
            "No dataset builds yet."
          ),
          false
        )
      );

      const blockers = element("div", "token-list");
      (quality.top_blockers || []).forEach((item) => blockers.append(element("span", ["token-chip", "token-chip-bad"], item)));
      (quality.next_actions || []).forEach((item) => blockers.append(element("span", ["token-chip", "token-chip-good"], item)));
      if (blockers.childNodes.length) {
        shell.append(accordionCard("Blockers And Next Actions", "Current readiness signals", blockers, false));
      }
    } else if (tabKey === "historical") {
      const historical = payload.historical || {};
      const corpus = historical.corpus || {};
      const readiness = historical.readiness || {};
      const confidence = historical.confidence_progress || {};
      const heuristics = historical.heuristics || {};
      shell.append(
        accordionCard(
          "Corpus Summary",
          `${formatInteger(corpus.imported_market_days || 0)} market-days · ${formatInteger(corpus.replayed_checkpoint_count || 0)} replayed checkpoints`,
          compactStatGrid([
            { label: "Imported Market Days", value: formatInteger(corpus.imported_market_days || 0) },
            { label: "Imported Markets", value: formatInteger(corpus.imported_market_count || 0) },
            { label: "Replayed Checkpoints", value: formatInteger(corpus.replayed_checkpoint_count || 0) },
            { label: "Trainable Rows", value: formatInteger(corpus.clean_historical_trainable_count || 0) },
            { label: "Mismatch Count", value: formatInteger(corpus.settlement_mismatch_count || 0) },
            { label: "Archive Promotions", value: formatInteger(corpus.checkpoint_archive_promotion_count || 0) },
            { label: "External Recoveries", value: formatInteger(((corpus.external_archive_recovery || {}).recovered_via_external_archive_market_day_count) || 0) },
          ]),
          true
        ),
        accordionCard(
          "Readiness And Confidence",
          `${readiness.training_ready ? "training ready" : "draft only"} · ${heuristics.confidence_state || "unknown"}`,
          (() => {
            const stack = element("div", "stack");
            stack.append(
              compactStatGrid(
                Object.entries(confidence || {}).map(([key, value]) => ({
                  label: formatKey(key),
                  value: typeof value === "object" && value !== null ? JSON.stringify(value) : String(value),
                }))
              ),
              renderObjectTable(readiness, "No historical readiness data.")
            );
            return stack;
          })(),
          true
        ),
        accordionCard(
          "Source Replay Coverage Samples",
          `${((historical.samples || {}).source_replay_coverage || []).length} samples`,
          buildTable(
            [
              { label: "Market Day", render: (row) => row.market_day || row.local_market_day || "n/a" },
              { label: "Ticker", render: (row) => row.market_ticker || "n/a" },
              { label: "Coverage", render: (row) => row.coverage_class || row.coverage || "n/a" },
              { label: "Reason", render: (row) => row.reason || "n/a" },
            ],
            ((historical.samples || {}).source_replay_coverage || []).slice(0, 8),
            "No source coverage samples."
          ),
          false
        ),
        accordionCard(
          "Checkpoint Archive Samples",
          `${((historical.samples || {}).checkpoint_archive_coverage || []).length} samples`,
          buildTable(
            [
              { label: "Market Day", render: (row) => row.market_day || row.local_market_day || "n/a" },
              { label: "Ticker", render: (row) => row.market_ticker || "n/a" },
              { label: "Coverage", render: (row) => row.coverage_class || row.coverage || "n/a" },
              { label: "Reason", render: (row) => row.reason || "n/a" },
            ],
            ((historical.samples || {}).checkpoint_archive_coverage || []).slice(0, 8),
            "No checkpoint archive coverage samples."
          ),
          false
        ),
        accordionCard(
          "External Archive Recovery",
          `${Object.keys(corpus.external_archive_recovery || {}).length} recovery metrics`,
          (() => {
            const stack = element("div", "stack");
            stack.append(
              renderObjectTable(corpus.external_archive_coverage || {}, "No external archive coverage data."),
              buildTable(
                [
                  { label: "Market Day", render: (row) => row.local_market_day || "n/a" },
                  { label: "Ticker", render: (row) => row.market_ticker || "n/a" },
                  { label: "Recovery", render: (row) => row.recovery_status || "n/a" },
                ],
                ((historical.samples || {}).external_archive_coverage || []).slice(0, 8),
                "No external archive recovery samples."
              )
            );
            return stack;
          })(),
          false
        ),
        accordionCard(
          "Coverage Repair",
          `${Object.keys(corpus.coverage_repair_summary || {}).length} repair metrics`,
          renderObjectTable(corpus.coverage_repair_summary || {}, "No coverage repair data."),
          false
        )
      );
    } else if (tabKey === "pipeline") {
      const pipeline = payload.pipeline || {};
      shell.append(
        accordionCard(
          "Pipeline Status",
          `${(pipeline.latest_pipeline_run || {}).status || "unknown"} · stale builds ${formatInteger(pipeline.stale_build_count || 0)}`,
          compactStatGrid([
            { label: "Latest Run", value: ((pipeline.latest_pipeline_run || {}).status) || "n/a", detail: (pipeline.latest_pipeline_run || {}).run_type || "" },
            { label: "Bootstrap Progress", value: pipeline.bootstrap_progress ? JSON.stringify(pipeline.bootstrap_progress) : "n/a" },
            { label: "Stale Builds", value: formatInteger(pipeline.stale_build_count || 0) },
          ]),
          true
        ),
        accordionCard(
          "Recent Import Runs",
          `${(pipeline.recent_import_runs || []).length} runs`,
          buildTable(
            [
              { label: "Status", render: (row) => row.status || "n/a" },
              { label: "Source", render: (row) => row.source_family || row.source_kind || "n/a" },
              { label: "Date Range", render: (row) => `${row.date_from || "?"} to ${row.date_to || "?"}` },
              { label: "Created", render: (row) => formatRelativeTime(row.created_at) },
            ],
            (pipeline.recent_import_runs || []).slice(0, 8),
            "No import runs yet."
          ),
          false
        ),
        accordionCard(
          "Recent Pipeline Runs",
          `${(pipeline.recent_pipeline_runs || []).length} runs`,
          buildTable(
            [
              { label: "Run Type", render: (row) => row.run_type || "n/a" },
              { label: "Status", render: (row) => row.status || "n/a" },
              { label: "Window", render: (row) => `${row.date_from || "?"} to ${row.date_to || "?"}` },
              { label: "Updated", render: (row) => formatRelativeTime(row.updated_at || row.completed_at || row.created_at) },
            ],
            (pipeline.recent_pipeline_runs || []).slice(0, 8),
            "No pipeline runs yet."
          ),
          false
        ),
        accordionCard(
          "Replay Refresh Causes",
          `${Object.keys(pipeline.replay_refresh_counts_by_cause || {}).length} causes`,
          renderObjectTable(pipeline.replay_refresh_counts_by_cause || {}, "No replay refresh activity yet."),
          false
        )
      );
    } else {
      const backlog = payload.backlog || {};
      shell.append(
        accordionCard(
          "Promotable Market-Day Counts",
          `${Object.keys(backlog.promotable_market_day_counts || {}).length} tracked buckets`,
          renderObjectTable(backlog.promotable_market_day_counts || {}, "No promotable market-day counts."),
          true
        ),
        accordionCard(
          "Coverage Backlog",
          `${Object.keys((backlog.coverage_backlog || {}).reason_counts || {}).length} missing-reason buckets`,
          renderObjectTable((backlog.coverage_backlog || {}).reason_counts || {}, "No coverage backlog data."),
          true
        ),
        accordionCard(
          "Settlement Maturity",
          `${Object.keys(backlog.settlement_maturity || {}).length} signals`,
          renderObjectTable(backlog.settlement_maturity || {}, "No settlement maturity data."),
          false
        ),
        accordionCard(
          "Recent Exclusion Memory",
          `${Object.keys(backlog.recent_exclusion_memory || {}).length} tracked sections`,
          renderObjectTable(backlog.recent_exclusion_memory || {}, "No recent exclusion memory."),
          false
        )
      );
    }

    panel.append(shell);
  }

  function confidenceMeter(value, band) {
    const wrap = element("div", "stack");
    const label = element("strong", null, value === null || value === undefined ? "n/a" : formatNumber(value, 2));
    const meter = element("div", "confidence-meter");
    const fill = element("div", ["confidence-meter-fill", `confidence-${band || "low"}`]);
    fill.style.width = `${Math.max(0, Math.min(100, Math.round(Number(value || 0) * 100)))}%`;
    meter.append(fill);
    wrap.append(label, meter);
    return wrap;
  }

  function renderResearchPanel(payload) {
    const panel = document.getElementById("tab-panel-research");
    if (!panel) {
      return;
    }
    clearNode(panel);
    const shell = element("div", "stack");

    const filters = element("div", "control-card");
    const filtersHeader = element("div", "control-card-header");
    filtersHeader.append(element("h2", null, "Research Filters"), element("p", null, "Prioritize markets by settlement timing, confidence, and series."));
    const statusPills = element("div", "filter-pills");
    [
      { id: "active", label: "Active" },
      { id: "closed", label: "Closed" },
      { id: "all", label: "All" },
    ].forEach((item) => {
      const pill = button(item.label, ["button-secondary", "pill-button"].concat(state.researchFilters.status === item.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.researchFilters.status = item.id;
        renderResearchPanel(payload);
      });
      statusPills.append(pill);
    });
    const seriesPills = element("div", "filter-pills");
    const seriesOptions = Array.isArray(payload.series_filters) && payload.series_filters.length
      ? payload.series_filters
      : [{ id: "all", label: "All Series" }];
    const allowedSeries = new Set(seriesOptions.map((item) => item.id));
    if (!allowedSeries.has(state.researchFilters.series)) {
      state.researchFilters.series = "all";
    }
    seriesOptions.forEach((item) => {
      const pill = button(item.label, ["button-secondary", "pill-button"].concat(state.researchFilters.series === item.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.researchFilters.series = item.id;
        renderResearchPanel(payload);
      });
      seriesPills.append(pill);
    });
    const searchField = element("div", "form-field form-field-wide");
    const searchInput = document.createElement("input");
    searchInput.type = "search";
    searchInput.placeholder = "Search market ticker or label";
    searchInput.value = state.researchFilters.query;
    searchInput.addEventListener("change", () => {
      state.researchFilters.query = searchInput.value.trim().toLowerCase();
      renderResearchPanel(payload);
    });
    searchField.append(element("label", null, "Search"), searchInput);
    filters.append(filtersHeader, statusPills, seriesPills, searchField);

    const markets = Array.isArray(payload.markets) ? payload.markets.slice() : [];
    const filtered = markets.filter((item) => {
      if (state.researchFilters.status !== "all" && item.status_group !== state.researchFilters.status) {
        return false;
      }
      if (state.researchFilters.series !== "all" && item.series_ticker !== state.researchFilters.series) {
        return false;
      }
      if (state.researchFilters.query) {
        const haystack = `${item.market_ticker} ${item.label}`.toLowerCase();
        return haystack.includes(state.researchFilters.query);
      }
      return true;
    });

    const active = filtered.filter((item) => item.status_group === "active");
    const closed = filtered.filter((item) => item.status_group !== "active");

    function researchSection(title, subtitle, items) {
      const card = element("section", "control-card");
      const header = element("div", "control-card-header");
      header.append(element("h2", null, title), element("p", null, subtitle));
      card.append(header);
      const list = element("div", "research-list");
      if (!items.length) {
        list.append(element("p", "empty-state", "No markets match the current filters."));
      } else {
        items.forEach((item) => {
          const marketCard = element("article", "research-card");
          const cardHeader = element("div", "research-card-header");
          const titleBlock = element("div", "research-card-title");
          titleBlock.append(
            element("strong", null, item.market_ticker),
            element("span", "muted", item.label || "Configured market")
          );
          const side = element("div", "stack");
          side.append(
            statusPill(item.status_group, item.status_group === "active" ? "good" : "neutral"),
            statusPill(item.confidence_band || "none", item.confidence_band === "high" ? "good" : item.confidence_band === "medium" ? "warning" : "bad")
          );
          cardHeader.append(titleBlock, side);

          const meta = element("div", "meta-row");
          meta.append(
            element("span", null, `series ${item.series_ticker || "n/a"}`),
            element("span", null, `close ${formatRelativeTime(item.close_at, { futurePreferred: true })}`),
            element("span", null, `refreshed ${formatRelativeTime(item.refreshed_at)}`),
            element("span", null, `expires ${formatRelativeTime(item.expires_at, { futurePreferred: true })}`)
          );

          const actions = element("div", "actions");
          actions.append(
            linkButton("Dossier JSON", item.json_url, ["button-ghost"]),
            button("Refresh Dossier", ["button-secondary"], { dataset: { action: "refreshResearch", marketTicker: item.market_ticker } })
          );

          const notes = element("div", "token-list");
          (item.notes || []).forEach((note) => {
            notes.append(element("span", "token-chip", note));
          });
          if (!notes.childNodes.length) {
            notes.append(element("span", "token-chip", item.has_dossier ? "Dossier present" : "No dossier yet"));
          }

          const gateReasons = element("div", "token-list");
          if (!item.gate_passed && (item.gate_reasons || []).length) {
            item.gate_reasons.forEach((reason) => {
              gateReasons.append(element("span", ["token-chip", "token-chip-bad"], reason));
            });
          }

          marketCard.append(
            cardHeader,
            confidenceMeter(item.confidence, item.confidence_band),
            meta,
            notes,
            ...(gateReasons.childNodes.length ? [gateReasons] : []),
            actions
          );
          list.append(marketCard);
        });
      }
      card.append(list);
      return card;
    }

    shell.append(filters, researchSection("Active Markets", "Sorted by settlement and confidence so the needy markets surface first.", active), researchSection("Closed Or Inactive", "Separated from the active queue so they stop stealing attention.", closed));
    panel.append(shell);
  }

  function renderRoomsPanel(payload) {
    const panel = document.getElementById("tab-panel-rooms");
    if (!panel) {
      return;
    }
    clearNode(panel);
    const shell = element("div", "stack");
    const quickCard = element("section", "control-card");
    const quickHeader = element("div", "control-card-header");
    quickHeader.append(element("h2", null, "Create Or Trigger Rooms"), element("p", null, "Compact controls for creating a room, running a shadow market, or kicking off a shadow campaign."));
    const form = element("form", "form-grid");
    form.addEventListener("submit", (event) => {
      event.preventDefault();
    });
    const marketField = element("div", "form-field");
    const marketLabel = element("label", null, "Market Ticker");
    const marketInput = document.createElement("input");
    marketInput.name = "market_ticker";
    marketInput.setAttribute("list", "quick-market-list");
    marketInput.placeholder = "KXHIGHCHI-26APR13-T76";
    marketField.append(marketLabel, marketInput);
    const marketList = document.createElement("datalist");
    marketList.id = "quick-market-list";
    (payload.quick_create_markets || []).forEach((item) => {
      const option = document.createElement("option");
      option.value = item.market_ticker;
      option.label = item.label || item.market_ticker;
      marketList.append(option);
    });
    marketField.append(marketList);

    const nameField = element("div", "form-field");
    const nameInput = document.createElement("input");
    nameInput.name = "name";
    nameInput.placeholder = "Chicago shadow room";
    nameField.append(element("label", null, "Room Name"), nameInput);

    const promptField = element("div", "form-field form-field-wide");
    const promptInput = document.createElement("textarea");
    promptInput.name = "prompt";
    promptInput.rows = 3;
    promptInput.placeholder = "Optional operator prompt";
    promptField.append(element("label", null, "Prompt"), promptInput);

    form.append(marketField, nameField, promptField);
    const actionRow = element("div", "actions");
    const createRoomButton = button("Create Room", ["button-primary"]);
    createRoomButton.addEventListener("click", async () => {
      if (!marketInput.value.trim() || !nameInput.value.trim()) {
        window.alert("Market ticker and room name are required.");
        return;
      }
      await runAction(async () => {
        const result = await postJson("/api/rooms", {
          market_ticker: marketInput.value.trim(),
          name: nameInput.value.trim(),
          prompt: promptInput.value.trim() || null,
        });
        window.location.assign(result.redirect);
      }, "Room created.", "Creating room...");
    });
    const shadowRunButton = button("Run Shadow Market", ["button-secondary"]);
    shadowRunButton.addEventListener("click", async () => {
      if (!marketInput.value.trim()) {
        window.alert("Enter a market ticker first.");
        return;
      }
      await runAction(async () => {
        const result = await postJson(`/api/markets/${encodeURIComponent(marketInput.value.trim())}/shadow-run`, {
          name: nameInput.value.trim() || null,
          prompt: promptInput.value.trim() || null,
          reason: "control_room_shadow_run",
        });
        window.location.assign(result.redirect);
      }, "Shadow room scheduled.", "Scheduling shadow room...");
    });
    const shadowCampaignButton = button("Run Shadow Campaign", ["button-secondary"]);
    shadowCampaignButton.addEventListener("click", async () => {
      await runAction(async () => {
        await postJson("/api/shadow-campaign/run", { reason: "control_room_shadow_campaign" });
      }, "Shadow campaign completed.", "Running shadow campaign...");
    });
    actionRow.append(createRoomButton, shadowRunButton, shadowCampaignButton);
    quickCard.append(quickHeader, form, actionRow);

    const filtersCard = element("section", "control-card");
    filtersCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Recent Rooms"), element("p", null, "Grouped by market ticker and filtered by outcome or origin so the repeated failures are easy to spot."));
        return header;
      })()
    );
    const statusPills = element("div", "filter-pills");
    [
      { id: "all", label: "All" },
      { id: "blocked", label: "Blocked" },
      { id: "stand_down", label: "Stand Down" },
      { id: "succeeded", label: "Succeeded" },
      { id: "failed", label: "Failed" },
    ].forEach((item) => {
      const pill = button(item.label, ["button-secondary", "pill-button"].concat(state.roomsFilters.status === item.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.roomsFilters.status = item.id;
        renderRoomsPanel(payload);
      });
      statusPills.append(pill);
    });
    const originPills = element("div", "filter-pills");
    [
      { id: "all", label: "All Origins" },
      { id: "shadow", label: "Shadow" },
      { id: "live", label: "Live" },
    ].forEach((item) => {
      const pill = button(item.label, ["button-secondary", "pill-button"].concat(state.roomsFilters.origin === item.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.roomsFilters.origin = item.id;
        renderRoomsPanel(payload);
      });
      originPills.append(pill);
    });
    const roomSearchField = element("div", "form-field form-field-wide");
    const roomSearchInput = document.createElement("input");
    roomSearchInput.type = "search";
    roomSearchInput.placeholder = "Search ticker or room name";
    roomSearchInput.value = state.roomsFilters.query;
    roomSearchInput.addEventListener("change", () => {
      state.roomsFilters.query = roomSearchInput.value.trim().toLowerCase();
      renderRoomsPanel(payload);
    });
    roomSearchField.append(element("label", null, "Search"), roomSearchInput);
    filtersCard.append(statusPills, originPills, roomSearchField);

    const rooms = Array.isArray(payload.rooms) ? payload.rooms : [];
    const filteredRooms = rooms.filter((room) => {
      if (state.roomsFilters.status !== "all" && room.status !== state.roomsFilters.status) {
        return false;
      }
      if (state.roomsFilters.origin !== "all" && String(room.room_origin || "").toLowerCase() !== state.roomsFilters.origin) {
        return false;
      }
      if (state.roomsFilters.query) {
        const haystack = `${room.market_ticker} ${room.name} ${room.id}`.toLowerCase();
        return haystack.includes(state.roomsFilters.query);
      }
      return true;
    });

    const grouped = new Map();
    filteredRooms.forEach((room) => {
      const key = room.market_ticker || "unknown";
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push(room);
    });

    const groupsContainer = element("div", "rooms-groups");
    if (!filteredRooms.length) {
      groupsContainer.append(element("p", "empty-state", "No rooms match the current filters."));
    } else {
      Array.from(grouped.entries()).forEach(([ticker, entries], index) => {
        const details = document.createElement("details");
        details.classList.add("rooms-group-card");
        if (index < 3) {
          details.open = true;
        }
        const summary = element("summary", "rooms-group-header");
        const title = element("div", "rooms-group-title");
        const counts = entries.reduce((accumulator, room) => {
          accumulator[room.status] = (accumulator[room.status] || 0) + 1;
          return accumulator;
        }, {});
        title.append(
          element("strong", null, ticker),
          element("span", "muted", `${entries.length} rooms · last ${formatRelativeTime(entries[0].updated_at)}`)
        );
        const pills = element("div", "token-list");
        Object.entries(counts).forEach(([key, value]) => {
          pills.append(statusPill(`${key.replaceAll("_", " ")} ${value}`, key));
        });
        summary.append(title, pills);
        const body = element("div", "room-list-inline");
        entries.forEach((room) => {
          const row = element("div", "room-row");
          const main = element("div", "room-row-main");
          const anchor = document.createElement("a");
          anchor.href = room.url;
          anchor.textContent = room.name || room.id;
          main.append(anchor, element("span", "muted", room.reason || room.stage || "n/a"));
          const side = element("div", "room-row-side");
          const ticketChip = (() => {
            const t = room.ticket;
            if (!t) return null;
            const label = []
              .concat(t.action ? [String(t.action).toUpperCase()] : [])
              .concat(t.side ? [String(t.side).toUpperCase()] : [])
              .concat(t.yes_price_dollars ? ["$".concat(t.yes_price_dollars)] : [])
              .concat(t.count_fp ? ["x".concat(t.count_fp)] : [])
              .join(" ");
            return label ? element("span", ["token-chip", "token-chip-good"], label) : null;
          })();
          side.append(
            statusPill(room.status_label || room.status, room.status_tone || room.status),
            ...(ticketChip ? [ticketChip] : []),
            element("span", "muted", formatRelativeTime(room.updated_at)),
            element("span", "muted", String(room.room_origin || "unknown"))
          );
          row.append(main, side);
          body.append(row);
        });
        details.append(summary, body);
        groupsContainer.append(details);
      });
    }

    shell.append(quickCard, filtersCard, groupsContainer);
    panel.append(shell);
  }

  function renderOperationsPanel(payload) {
    const panel = document.getElementById("tab-panel-operations");
    if (!panel) {
      return;
    }
    clearNode(panel);
    const shell = element("div", "control-room-grid");
    const control = payload.control || {};
    const runtimeHealth = payload.runtime_health || {};
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    const opsEvents = Array.isArray(payload.ops_events) ? payload.ops_events : [];
    const selfImprove = payload.self_improve || {};
    const heuristics = payload.heuristics || {};

    const deploymentCard = element("article", ["control-card", "control-card-medium"]);
    deploymentCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Deployment And Safety"), element("p", null, "The dangerous controls live together with clear visual hierarchy."));
        return header;
      })(),
      compactStatGrid([
        { label: "Active Color", value: String(control.active_color || "unknown").toUpperCase() },
        { label: "Kill Switch", value: control.kill_switch_enabled ? "Enabled" : "Disabled" },
        { label: "Execution Lock", value: control.execution_lock_holder || "unheld" },
      ]),
      (() => {
        const actions = element("div", "actions");
        actions.append(
          button("Enable Kill Switch", ["button-danger"], { dataset: { action: "toggleKill", enabled: "true" } }),
          button("Disable Kill Switch", ["button-secondary"], { dataset: { action: "toggleKill", enabled: "false" } }),
          button("Promote Blue", ["button-primary"], { dataset: { action: "promoteColor", color: "blue" } }),
          button("Promote Green", ["button-primary"], { dataset: { action: "promoteColor", color: "green" } })
        );
        return actions;
      })()
    );

    const runtimeCard = element("article", ["control-card", "control-card-medium"]);
    runtimeCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Runtime Freshness"), element("p", null, "Daemon heartbeat ages are shown as relative durations instead of raw floats."));
        return header;
      })()
    );
    const runtimeGrid = element("div", "runtime-health-grid");
    Object.entries(runtimeHealth.colors || {}).forEach(([color, health]) => {
      runtimeGrid.append(
        infoCard(
          `${color.toUpperCase()} daemon`,
          ((health.daemon || {}).healthy) ? "healthy" : "unhealthy",
          `heartbeat ${formatDurationSeconds((health.daemon || {}).heartbeat_age_seconds)}`
        )
      );
    });
    runtimeCard.append(runtimeGrid);

    const positionsCard = element("article", ["control-card", "control-card-wide"]);
    positionsCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Positions"), element("p", null, "Count-first summary with safe exposure details only."));
        return header;
      })()
    );
    positionsCard.append(
      buildTable(
        [
          { label: "Ticker", render: (row) => row.market_ticker || "n/a" },
          { label: "Side", render: (row) => row.side || "n/a" },
          { label: "Count", render: (row) => row.count_fp || "0" },
          { label: "Average Price", render: (row) => row.average_price_dollars || "n/a" },
          { label: "Updated", render: (row) => formatRelativeTime(row.updated_at) },
        ],
        positions.slice(0, 20),
        "No reconciled positions yet."
      )
    );

    const eventsCard = element("article", ["control-card", "control-card-wide"]);
    const eventsHeader = element("div", "control-card-header");
    eventsHeader.append(element("h2", null, "Ops Events"), element("p", null, "Timestamped and filterable so info spam doesn’t hide problems."));
    const severityFilters = element("div", "filter-pills");
    [
      { id: "all", label: "All" },
      { id: "info", label: "Info" },
      { id: "warning", label: "Warning" },
      { id: "error", label: "Error" },
    ].forEach((item) => {
      const pill = button(item.label, ["button-secondary", "pill-button"].concat(state.operationsFilters.severity === item.id ? ["is-active"] : []));
      pill.addEventListener("click", () => {
        state.operationsFilters.severity = item.id;
        renderOperationsPanel(payload);
      });
      severityFilters.append(pill);
    });
    eventsCard.append(eventsHeader, severityFilters);
    const filteredEvents = opsEvents.filter((eventItem) => state.operationsFilters.severity === "all" || eventItem.severity === state.operationsFilters.severity);
    const eventsList = element("div", "events-list");
    if (!filteredEvents.length) {
      eventsList.append(element("p", "empty-state", "No ops events match the current filter."));
    } else {
      filteredEvents.slice(0, 16).forEach((eventItem) => {
        const card = element("article", "event-card");
        const header = element("div", "event-card-header");
        header.append(
          element("strong", null, eventItem.summary || "Event"),
          statusPill(eventItem.severity || "info", eventItem.severity === "error" ? "bad" : eventItem.severity === "warning" ? "warning" : "neutral")
        );
        const meta = element("div", "meta-row");
        meta.append(
          element("span", null, eventItem.source || "unknown"),
          element("span", null, formatRelativeTime(eventItem.created_at)),
          element("span", "muted", formatDateTime(eventItem.created_at))
        );
        card.append(header, meta);
        eventsList.append(card);
      });
    }
    eventsCard.append(eventsList);

    const improveCard = element("article", ["control-card", "control-card-medium"]);
    improveCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Self Improve"), element("p", null, "Recent critique and evaluation activity stays visible without crowding training history."));
        return header;
      })(),
      compactStatGrid([
        { label: "Champion", value: (selfImprove.agent_packs || {}).champion_version || "n/a" },
        { label: "Candidate", value: (selfImprove.agent_packs || {}).candidate_version || "none" },
        { label: "Latest Evaluation", value: (((selfImprove.recent_evaluations || [])[0] || {}).candidate_version) || "n/a" },
      ]),
      (() => {
        const actions = element("div", "actions");
        actions.append(button("Run Critique", ["button-secondary"], { dataset: { action: "runCritique" } }));
        return actions;
      })()
    );

    const heuristicCard = element("article", ["control-card", "control-card-medium"]);
    heuristicCard.append(
      (() => {
        const header = element("div", "control-card-header");
        header.append(element("h2", null, "Historical Heuristics"), element("p", null, "The promoted runtime heuristic pack is controlled here, not buried in training scrollback."));
        return header;
      })(),
      compactStatGrid([
        { label: "Active Pack", value: heuristics.active_pack_version || "n/a" },
        { label: "Candidate Pack", value: heuristics.candidate_pack_version || "none" },
        { label: "Confidence", value: heuristics.confidence_state || "unknown" },
      ]),
      (() => {
        const actions = element("div", "actions");
        actions.append(
          button("Promote Pack", ["button-primary"], {
            dataset: { action: "promotePack", candidateVersion: heuristics.candidate_pack_version || "" },
            disabled: !heuristics.candidate_pack_version,
          }),
          button("Rollback Pack", ["button-danger"], { dataset: { action: "rollbackPack" } })
        );
        return actions;
      })()
    );

    shell.append(deploymentCard, runtimeCard, positionsCard, eventsCard, improveCard, heuristicCard);
    panel.append(shell);
  }

  function ensureTabPanel(tabId) {
    const panel = document.getElementById(`tab-panel-${tabId}`);
    if (!panel) {
      return null;
    }
    return panel;
  }

  function renderTab(tabId, payload) {
    if (tabId === "overview") {
      renderOverviewPanel(payload);
      return;
    }
    if (tabId === "training") {
      renderTrainingSubtab(state.trainingSubtab);
      return;
    }
    if (tabId === "research") {
      renderResearchPanel(payload);
      return;
    }
    if (tabId === "rooms") {
      renderRoomsPanel(payload);
      return;
    }
    renderOperationsPanel(payload);
  }

  async function loadTab(tabId, force) {
    if (!force && state.tabs[tabId]) {
      return state.tabs[tabId];
    }
    if (state.loadingTabs.has(tabId)) {
      return null;
    }
    state.loadingTabs.add(tabId);
    try {
      const payload = await fetchJson(`/api/control-room/tab/${encodeURIComponent(tabId)}`);
      state.tabs[tabId] = payload;
      return payload;
    } finally {
      state.loadingTabs.delete(tabId);
    }
  }

  function setActiveTabUi(tabId) {
    TABS.forEach((tab) => {
      const buttonNode = document.getElementById(`tab-button-${tab.id}`);
      const panel = ensureTabPanel(tab.id);
      const active = tab.id === tabId;
      if (buttonNode) {
        buttonNode.classList.toggle("is-active", active);
        buttonNode.setAttribute("aria-selected", active ? "true" : "false");
      }
      if (panel) {
        panel.classList.toggle("is-active", active);
        panel.hidden = !active;
      }
    });
  }

  async function activateTab(tabId, options) {
    const selectedTab = TABS.some((tab) => tab.id === tabId) ? tabId : "overview";
    state.activeTab = selectedTab;
    setActiveTabUi(selectedTab);
    if (!(options && options.skipHash)) {
      window.history.replaceState(null, "", `#${selectedTab}`);
    }
    const panel = ensureTabPanel(selectedTab);
    if (!panel) {
      return;
    }
    if (!state.tabs[selectedTab] || (options && options.force)) {
      clearNode(panel);
      panel.append(element("p", "tab-loading-state", `Loading ${selectedTab}…`));
      try {
        await loadTab(selectedTab, Boolean(options && options.force));
      } catch (error) {
        clearNode(panel);
        panel.append(element("p", "empty-state", `Could not load ${selectedTab}: ${error.message}`));
        return;
      }
    }
    renderTab(selectedTab, state.tabs[selectedTab]);
  }

  function openConfirmation(config) {
    state.pendingConfirmation = config;
    modalBody.textContent = config.body;
    modal.hidden = false;
    modalConfirm.textContent = config.confirmLabel || "Confirm";
  }

  function closeConfirmation() {
    state.pendingConfirmation = null;
    modal.hidden = true;
  }

  async function runAction(action, successMessage, progressMessage) {
    try {
      setRefreshState(progressMessage || "Working…", "warning");
      await action();
      setRefreshState(successMessage || "Completed.", "good");
      await refreshActiveView({ force: true });
    } catch (error) {
      setRefreshState(error.message || "Action failed.", "bad");
      window.alert(error.message || "Action failed.");
    }
  }

  async function runDangerousAction(kind, data) {
    if (kind === "toggleKill") {
      await runAction(
        async () => {
          await postJson(`/api/control/kill-switch/${data.enabled === "true"}`, {});
        },
        data.enabled === "true" ? "Kill switch enabled." : "Kill switch disabled.",
        data.enabled === "true" ? "Enabling kill switch…" : "Disabling kill switch…"
      );
      return;
    }
    if (kind === "promoteColor") {
      await runAction(
        async () => {
          await postJson(`/api/control/promote/${encodeURIComponent(data.color)}`, {});
        },
        `${String(data.color || "").toUpperCase()} promoted.`,
        `Promoting ${data.color}…`
      );
      return;
    }
    if (kind === "promotePack") {
      await runAction(
        async () => {
          await postJson("/api/heuristic-pack/promote", {
            candidate_version: data.candidateVersion || null,
            reason: "control_room_manual_promote",
          });
        },
        "Heuristic pack promoted.",
        "Promoting heuristic pack…"
      );
      return;
    }
    if (kind === "rollbackPack") {
      await runAction(
        async () => {
          await postJson("/api/heuristic-pack/rollback", { reason: "control_room_manual_rollback" });
        },
        "Heuristic pack rolled back.",
        "Rolling back heuristic pack…"
      );
    }
  }

  async function handleAction(actionName, dataset) {
    if (actionName === "refreshNow") {
      await refreshActiveView({ force: true });
      setRefreshState("Refreshed.", "good");
      return;
    }
    if (actionName === "toggleKill") {
      openConfirmation({
        body: dataset.enabled === "true" ? "Enable the kill switch and stop trading?" : "Disable the kill switch and allow trading again?",
        confirmLabel: dataset.enabled === "true" ? "Enable" : "Disable",
        kind: "toggleKill",
        data: dataset,
      });
      return;
    }
    if (actionName === "promoteColor") {
      openConfirmation({
        body: `Promote ${String(dataset.color || "").toUpperCase()} to active deployment?`,
        confirmLabel: "Promote",
        kind: "promoteColor",
        data: dataset,
      });
      return;
    }
    if (actionName === "promotePack") {
      openConfirmation({
        body: "Promote the current heuristic candidate pack into the active runtime slot?",
        confirmLabel: "Promote",
        kind: "promotePack",
        data: dataset,
      });
      return;
    }
    if (actionName === "rollbackPack") {
      openConfirmation({
        body: "Rollback the active heuristic pack to the previous version?",
        confirmLabel: "Rollback",
        kind: "rollbackPack",
        data: dataset,
      });
      return;
    }
    if (actionName === "refreshResearch") {
      await runAction(
        async () => {
          await postJson(`/api/research/${encodeURIComponent(dataset.marketTicker)}/refresh`, {});
        },
        `Refreshed ${dataset.marketTicker}.`,
        `Refreshing ${dataset.marketTicker}…`
      );
      return;
    }
    if (actionName === "runCritique") {
      await runAction(
        async () => {
          await postJson("/api/self-improve/critique", {});
        },
        "Critique run started.",
        "Running critique…"
      );
      return;
    }
    if (actionName === "buildRoomBundles") {
      await runAction(
        async () => {
          await postJson("/api/training/build", { mode: "room-bundles", quality_cleaned_only: true });
        },
        "Room bundles build started.",
        "Building room bundles…"
      );
      return;
    }
    const historicalWindow = currentHistoricalWindow(30);
    if (actionName === "buildHistoricalBundles") {
      await runAction(
        async () => {
          await postJson("/api/training/historical/build", {
            mode: "bundles",
            ...historicalWindow,
            quality_cleaned_only: true,
            require_full_checkpoints: false,
            late_only_ok: true,
          });
        },
        "Historical bundle build started.",
        "Building historical bundles…"
      );
      return;
    }
    if (actionName === "buildOutcomeEval") {
      await runAction(
        async () => {
          await postJson("/api/training/historical/build", {
            mode: "outcome-eval",
            ...historicalWindow,
            quality_cleaned_only: true,
            require_full_checkpoints: false,
            late_only_ok: true,
          });
        },
        "Outcome evaluation build started.",
        "Building outcome evaluation…"
      );
      return;
    }
    if (actionName === "buildDecisionEval") {
      await runAction(
        async () => {
          await postJson("/api/training/historical/build", {
            mode: "decision-eval",
            ...historicalWindow,
            quality_cleaned_only: true,
            require_full_checkpoints: true,
            late_only_ok: false,
          });
        },
        "Decision evaluation build started.",
        "Building decision evaluation…"
      );
      return;
    }
    if (actionName === "buildGeminiDraft") {
      await runAction(
        async () => {
          await postJson("/api/training/historical/build", {
            mode: "gemini-finetune",
            ...historicalWindow,
            quality_cleaned_only: true,
            require_full_checkpoints: true,
            late_only_ok: false,
          });
        },
        "Gemini draft export started.",
        "Building Gemini draft export…"
      );
    }
  }

  async function refreshActiveView(options) {
    const summaryPromise = fetchJson("/api/control-room/summary");
    const tabPromise = fetchJson(`/api/control-room/tab/${encodeURIComponent(state.activeTab)}`);
    try {
      const [summary, tabPayload] = await Promise.all([summaryPromise, tabPromise]);
      renderSummary(summary);
      state.tabs[state.activeTab] = tabPayload;
      renderTab(state.activeTab, tabPayload);
      setRefreshState(state.refreshEnabled ? "Auto refresh on" : "Auto refresh paused", state.refreshEnabled ? "good" : "warning");
    } catch (error) {
      if (!(options && options.silent)) {
        setRefreshState(error.message || "Refresh failed.", "bad");
      }
    }
  }

  function startPolling() {
    if (state.refreshTimer) {
      window.clearInterval(state.refreshTimer);
    }
    state.refreshTimer = window.setInterval(() => {
      if (!state.refreshEnabled || document.hidden) {
        return;
      }
      refreshActiveView({ silent: true });
    }, state.refreshIntervalMs);
  }

  page.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action], [data-tab]");
    if (!target) {
      return;
    }
    if (target.dataset.tab) {
      event.preventDefault();
      await activateTab(target.dataset.tab);
      return;
    }
    if (target.dataset.action === "closeModal") {
      closeConfirmation();
      return;
    }
    const actionMap = {
      "refresh-now": "refreshNow",
      "toggle-kill": "toggleKill",
      "promote-color": "promoteColor",
      "run-critique": "runCritique",
      "rollback-pack": "rollbackPack",
      refreshNow: "refreshNow",
    };
    const actionName = actionMap[target.dataset.action] || target.dataset.action;
    if (actionName) {
      event.preventDefault();
      await handleAction(actionName, target.dataset);
    }
  });

  modal.addEventListener("click", (event) => {
    if (event.target.dataset.action === "close-modal") {
      closeConfirmation();
    }
  });

  modalConfirm.addEventListener("click", async () => {
    const pending = state.pendingConfirmation;
    if (!pending) {
      return;
    }
    closeConfirmation();
    await runDangerousAction(pending.kind, pending.data || {});
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refreshActiveView({ silent: true });
    }
  });

  function determineInitialTab() {
    const hash = window.location.hash.replace("#", "");
    if (TABS.some((tab) => tab.id === hash)) {
      return hash;
    }
    return bootstrap.initial_tab || "overview";
  }

  renderSummary(state.summary);
  renderOverviewPanel(bootstrap.initial_tab_payload || {});
  setRefreshState("Auto refresh on", "good");
  startPolling();
  activateTab(determineInitialTab(), { skipHash: true });
})();
