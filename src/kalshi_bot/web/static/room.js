(function () {
  const SVG_NS = "http://www.w3.org/2000/svg";
  const bootstrapNode = document.getElementById("room-bootstrap");
  if (!bootstrapNode) {
    return;
  }

  const bootstrap = JSON.parse(bootstrapNode.textContent || "{}");
  const transcriptElement = document.getElementById("transcript");
  const followToggle = document.getElementById("follow-toggle");
  const unreadBadge = document.getElementById("transcript-unread");
  const runRoomButton = document.getElementById("run-room-button");
  const refreshResearchButton = document.getElementById("refresh-research-button");

  const state = {
    roomId: bootstrap.roomId,
    snapshotUrl: bootstrap.snapshotUrl,
    eventsUrl: bootstrap.eventsUrl,
    snapshot: bootstrap.initialSnapshot || {},
    messages: Array.isArray(bootstrap.initialMessages) ? bootstrap.initialMessages.slice() : [],
    lastSequence: 0,
    followEnabled: true,
    unreadCount: 0,
    eventSource: null,
    pollTimer: null,
  };

  state.lastSequence = state.messages.reduce((maxValue, message) => Math.max(maxValue, Number(message.sequence || 0)), 0);
  state.followEnabled = !["complete", "failed"].includes((state.snapshot.room || {}).stage || "");

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

  function svgNode(tagName, attrs) {
    const node = document.createElementNS(SVG_NS, tagName);
    Object.entries(attrs || {}).forEach(([key, value]) => {
      node.setAttribute(key, String(value));
    });
    return node;
  }

  function safeArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function formatStage(stage) {
    if (!stage) {
      return "n/a";
    }
    return String(stage)
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  function formatRole(role) {
    return formatStage(role);
  }

  function formatDecimal(value, digits = 4) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
      return String(value);
    }
    return numeric.toFixed(digits);
  }

  function formatInteger(value) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
      return String(value);
    }
    return String(Math.round(numeric));
  }

  function formatPercent(value) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
      return String(value);
    }
    return numeric.toFixed(2);
  }

  function formatTimestamp(value) {
    if (!value) {
      return "n/a";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    return date.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function updateFollowButton() {
    if (!followToggle) {
      return;
    }
    followToggle.textContent = `Follow: ${state.followEnabled ? "On" : "Off"}`;
  }

  function updateUnreadBadge() {
    if (!unreadBadge) {
      return;
    }
    if (state.unreadCount > 0 && !state.followEnabled) {
      unreadBadge.textContent = `${state.unreadCount} new`;
      unreadBadge.classList.remove("hidden");
    } else {
      unreadBadge.classList.add("hidden");
    }
  }

  function scrollTranscriptToBottom() {
    if (!transcriptElement) {
      return;
    }
    transcriptElement.scrollTop = transcriptElement.scrollHeight;
  }

  function setSummaryText(id, value) {
    const node = document.getElementById(id);
    if (node) {
      node.textContent = value;
    }
  }

  function buildBadge(className, text) {
    const badge = element("span", ["badge", className], text);
    return badge;
  }

  function buildPayloadDrawer(payload) {
    if (!payload || (typeof payload === "object" && Object.keys(payload).length === 0)) {
      return null;
    }
    const details = element("details", "payload-drawer");
    const summary = element("summary", null, "View details");
    const pre = element("pre", "payload-json");
    pre.textContent = JSON.stringify(payload, null, 2);
    details.append(summary, pre);
    return details;
  }

  function buildMessageCard(message) {
    const article = element("article", [
      "message-card",
      `role-${String(message.role || "supervisor").replaceAll("_", "-")}`,
    ]);

    const head = element("header", "message-head");
    const roleBlock = element("div", "message-role-block");
    roleBlock.append(
      element("strong", "message-role", formatRole(message.role)),
      buildBadge("kind-badge", String(message.kind || "Observation"))
    );
    if (message.stage) {
      roleBlock.append(buildBadge("stage-badge", formatStage(message.stage)));
    }

    const meta = element("div", "message-meta");
    meta.append(
      element("span", null, `#${message.sequence ?? "?"}`),
      element("span", null, formatTimestamp(message.created_at))
    );

    head.append(roleBlock, meta);

    const body = element("div", "message-body");
    body.textContent = message.content;

    article.append(head, body);

    const drawer = buildPayloadDrawer(message.payload);
    if (drawer) {
      article.append(drawer);
    }
    return article;
  }

  function renderTranscript() {
    if (!transcriptElement) {
      return;
    }
    if (!state.messages.length) {
      transcriptElement.replaceChildren(element("p", "empty-state", "No transcript messages yet."));
      return;
    }

    const nodes = [];
    let lastStage = "";
    state.messages.forEach((message) => {
      if (message.stage && message.stage !== lastStage) {
        nodes.push(element("div", ["stage-divider", `stage-${message.stage}`], formatStage(message.stage)));
        lastStage = message.stage;
      }
      nodes.push(buildMessageCard(message));
    });
    transcriptElement.replaceChildren(...nodes);
    if (state.followEnabled) {
      scrollTranscriptToBottom();
    }
  }

  function renderTimeline() {
    const timeline = document.getElementById("workflow-timeline");
    if (!timeline) {
      return;
    }
    const items = safeArray(state.snapshot.stage_timeline);
    if (!items.length) {
      timeline.replaceChildren(element("li", "empty-state", "No workflow stages recorded yet."));
      return;
    }
    const nodes = items.map((item) => {
      const li = element("li", ["timeline-step", `timeline-${item.status || "pending"}`]);
      const dot = element("span", "timeline-dot");
      const content = element("div", "timeline-content");
      content.append(
        element("strong", null, item.label || formatStage(item.stage)),
        element("span", "subtle-text", item.status || "pending")
      );
      if (item.at) {
        content.append(element("span", "subtle-text", formatTimestamp(item.at)));
      }
      li.append(dot, content);
      return li;
    });
    timeline.replaceChildren(...nodes);
  }

  function renderSummaryStrip() {
    const room = state.snapshot.room || {};
    const analytics = state.snapshot.analytics || {};
    const pricing = analytics.pricing || {};
    const decision = analytics.decision || {};

    setSummaryText("room-name", room.name || "Room");
    setSummaryText("room-market", room.market_ticker || "n/a");
    setSummaryText("room-stage", formatStage(room.stage));
    setSummaryText("room-shadow", room.shadow_mode ? "shadow" : "live");
    setSummaryText("room-pack", room.agent_pack_version || "n/a");

    const researchGatePassed = decision.research_gate_passed;
    setSummaryText(
      "summary-research-gate",
      researchGatePassed === null || researchGatePassed === undefined ? "n/a" : researchGatePassed ? "Passed" : "Blocked"
    );
    setSummaryText("summary-risk-status", formatStage(decision.risk_status || "n/a"));
    setSummaryText("summary-execution-status", formatStage(decision.execution_status || "n/a"));
    setSummaryText("summary-fair-yes", formatDecimal(pricing.fair_yes_dollars));
    setSummaryText("summary-edge-bps", formatInteger(pricing.edge_bps));
    setSummaryText("summary-confidence", formatPercent(pricing.confidence));
  }

  function buildMetricCard(label, value) {
    const card = element("div", "metric-card");
    card.append(element("span", "metric-label", label), element("strong", null, value));
    return card;
  }

  function renderDecisionPanel() {
    const decisionGrid = document.getElementById("decision-grid");
    if (!decisionGrid) {
      return;
    }
    const decision = ((state.snapshot || {}).analytics || {}).decision || {};
    const cards = [
      buildMetricCard("Trade Proposed", decision.trade_proposed ? "Yes" : "No"),
      buildMetricCard("Orders", formatInteger(decision.order_count || 0)),
      buildMetricCard("Fills", formatInteger(decision.fill_count || 0)),
      buildMetricCard("Latest Order", formatStage(decision.latest_order_status || "n/a")),
      buildMetricCard("Risk Status", formatStage(decision.risk_status || "n/a")),
      buildMetricCard("Execution", formatStage(decision.execution_status || "n/a")),
    ];
    decisionGrid.replaceChildren(...cards);

    const panel = decisionGrid.parentElement;
    if (!panel) {
      return;
    }
    let summaryNode = document.getElementById("latest-ops-summary");
    if (decision.latest_ops_summary) {
      if (!summaryNode) {
        summaryNode = element("p", "subtle-callout");
        summaryNode.id = "latest-ops-summary";
        panel.append(summaryNode);
      }
      summaryNode.textContent = decision.latest_ops_summary;
    } else if (summaryNode) {
      summaryNode.remove();
    }
  }

  function renderMetricGrid(containerId, items) {
    const container = document.getElementById(containerId);
    if (!container) {
      return;
    }
    const cards = items.map((item) => buildMetricCard(item.label, item.value));
    container.replaceChildren(...cards);
  }

  function renderSvgBars(targetId, bars, className, maxValue, options) {
    const svg = document.getElementById(targetId);
    if (!svg) {
      return;
    }
    svg.replaceChildren();
    const width = 360;
    const height = 220;
    const chartLeft = 48;
    const chartBottom = 180;
    const chartTop = 24;
    const chartWidth = 280;
    const chartHeight = chartBottom - chartTop;

    svg.append(
      svgNode("line", { x1: chartLeft, y1: chartBottom, x2: chartLeft + chartWidth, y2: chartBottom, class: "chart-axis" }),
      svgNode("line", { x1: chartLeft, y1: chartTop, x2: chartLeft, y2: chartBottom, class: "chart-axis" })
    );

    if (!bars.length || !maxValue || maxValue <= 0) {
      const label = svgNode("text", { x: 180, y: 110, "text-anchor": "middle", class: "chart-label" });
      label.textContent = options?.emptyLabel || "Awaiting data";
      svg.append(label);
      return;
    }

    const slotWidth = chartWidth / bars.length;
    bars.forEach((bar, index) => {
      const numeric = Number(bar.value);
      if (!Number.isFinite(numeric)) {
        return;
      }
      const barHeight = Math.max(6, (numeric / maxValue) * (chartHeight - 10));
      const x = chartLeft + index * slotWidth + 18;
      const y = chartBottom - barHeight;
      const rect = svgNode("rect", {
        x,
        y,
        width: Math.max(24, slotWidth - 36),
        height: barHeight,
        rx: 8,
        class: className,
      });
      const topLabel = svgNode("text", { x: x + (slotWidth - 36) / 2, y: y - 8, "text-anchor": "middle", class: "chart-value" });
      topLabel.textContent = bar.display;
      const bottomLabel = svgNode("text", {
        x: x + (slotWidth - 36) / 2,
        y: chartBottom + 18,
        "text-anchor": "middle",
        class: "chart-label",
      });
      bottomLabel.textContent = bar.label;
      svg.append(rect, topLabel, bottomLabel);
    });
  }

  function renderPricingPanel() {
    const pricing = ((state.snapshot || {}).analytics || {}).pricing || {};
    renderMetricGrid("pricing-metrics", [
      { label: "Yes Bid", value: formatDecimal(pricing.yes_bid_dollars) },
      { label: "Yes Ask", value: formatDecimal(pricing.yes_ask_dollars) },
      { label: "No Ask", value: formatDecimal(pricing.no_ask_dollars) },
      { label: "Last", value: formatDecimal(pricing.last_price_dollars) },
    ]);

    const bars = [
      { label: "Yes Bid", value: pricing.yes_bid_dollars, display: formatDecimal(pricing.yes_bid_dollars) },
      { label: "Yes Ask", value: pricing.yes_ask_dollars, display: formatDecimal(pricing.yes_ask_dollars) },
      { label: "Fair Yes", value: pricing.fair_yes_dollars, display: formatDecimal(pricing.fair_yes_dollars) },
      { label: "Last", value: pricing.last_price_dollars, display: formatDecimal(pricing.last_price_dollars) },
    ].filter((item) => item.value !== null && item.value !== undefined && item.value !== "");

    const maxValue = Math.max(1, ...bars.map((item) => Number(item.value) || 0));
    renderSvgBars("pricing-chart", bars, "chart-bar-pricing", maxValue, { emptyLabel: "No market snapshot yet" });
  }

  function renderWeatherPanel() {
    const weather = ((state.snapshot || {}).analytics || {}).weather || {};
    renderMetricGrid("weather-metrics", [
      { label: "Threshold", value: formatInteger(weather.threshold_f) },
      { label: "Forecast High", value: formatInteger(weather.forecast_high_f) },
      { label: "Current Temp", value: formatInteger(weather.current_temp_f) },
      { label: "Station", value: weather.station_id || "n/a" },
    ]);

    const numericValues = [weather.threshold_f, weather.forecast_high_f, weather.current_temp_f]
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
    const maxValue = numericValues.length ? Math.max(...numericValues) + 5 : 0;
    const bars = [
      { label: "Threshold", value: weather.threshold_f, display: formatInteger(weather.threshold_f) },
      { label: "Forecast", value: weather.forecast_high_f, display: formatInteger(weather.forecast_high_f) },
      { label: "Current", value: weather.current_temp_f, display: formatInteger(weather.current_temp_f) },
    ].filter((item) => item.value !== null && item.value !== undefined && item.value !== "");
    renderSvgBars("weather-chart", bars, "chart-bar-weather", maxValue, { emptyLabel: "No structured weather bundle yet" });
  }

  function renderQualityPanel() {
    const quality = ((state.snapshot || {}).analytics || {}).research_quality || {};
    renderMetricGrid("quality-metrics", [
      { label: "Overall", value: formatPercent(quality.overall_score) },
      { label: "Freshness", value: formatPercent(quality.freshness_score) },
      { label: "Settlement", value: formatPercent(quality.settlement_clarity_score) },
      { label: "Contradictions", value: formatInteger(quality.contradiction_count || 0) },
    ]);

    const bars = [
      { label: "Overall", value: quality.overall_score, display: formatPercent(quality.overall_score) },
      { label: "Citations", value: quality.citation_coverage_score, display: formatPercent(quality.citation_coverage_score) },
      { label: "Fresh", value: quality.freshness_score, display: formatPercent(quality.freshness_score) },
      { label: "Struct", value: quality.structured_completeness_score, display: formatPercent(quality.structured_completeness_score) },
      { label: "Fair", value: quality.fair_value_score, display: formatPercent(quality.fair_value_score) },
    ].filter((item) => item.value !== null && item.value !== undefined && item.value !== "");
    renderSvgBars("quality-chart", bars, "chart-bar-quality", 1, { emptyLabel: "No research quality data yet" });

    const summary = document.getElementById("research-summary");
    if (!summary) {
      return;
    }
    const dossier = state.snapshot.research_dossier || null;
    const delta = state.snapshot.research_delta || null;
    const nodes = [];
    if (dossier && dossier.summary && dossier.summary.narrative) {
      const narrative = element("p", "subtle-text", dossier.summary.narrative);
      narrative.id = "research-narrative";
      nodes.push(narrative);
      if (delta && delta.summary) {
        const deltaCard = element("div", "mini-card");
        deltaCard.id = "research-delta-card";
        deltaCard.append(element("strong", null, "Room Delta"), element("span", null, delta.summary));
        nodes.push(deltaCard);
      }
    } else {
      const empty = element("p", "empty-state", "No dossier available yet for this room.");
      empty.id = "research-narrative";
      nodes.push(empty);
    }
    summary.replaceChildren(...nodes);
  }

  function renderSourcePanel() {
    const summary = ((state.snapshot || {}).analytics || {}).source_summary || {};
    const sourceSummary = document.getElementById("source-summary");
    if (sourceSummary) {
      const chips = [buildBadge("", `sources ${formatInteger(summary.count || 0)}`)];
      Object.entries(summary.by_class || {}).forEach(([sourceClass, count]) => {
        chips.push(buildBadge("", `${sourceClass} · ${count}`));
      });
      Object.entries(summary.by_trust || {}).forEach(([trustTier, count]) => {
        chips.push(buildBadge("", `${trustTier} · ${count}`));
      });
      sourceSummary.replaceChildren(...chips);
    }

    const evidenceCards = document.getElementById("evidence-cards");
    if (evidenceCards) {
      const sources = safeArray(state.snapshot.research_sources);
      if (!sources.length) {
        evidenceCards.replaceChildren(element("p", "empty-state", "No source cards saved yet."));
      } else {
        const cards = sources.map((source) => {
          const card = element("div", "mini-card");
          card.append(
            element("strong", null, source.title || "Untitled source"),
            element(
              "span",
              null,
              `${source.publisher || "unknown"} · ${source.trust_tier || "n/a"} · ${source.source_class || "n/a"}`
            ),
            element("span", null, source.snippet || "No snippet saved.")
          );
          if (source.url) {
            const link = element("a", null, "Open source");
            link.href = source.url;
            link.target = "_blank";
            link.rel = "noreferrer";
            card.append(link);
          }
          return card;
        });
        evidenceCards.replaceChildren(...cards);
      }
    }

    const runStrip = document.getElementById("research-run-strip");
    if (runStrip) {
      const runs = safeArray(state.snapshot.research_runs);
      if (!runs.length) {
        runStrip.replaceChildren(element("p", "empty-state", "No research runs recorded yet."));
      } else {
        const cards = runs.map((run) => {
          const card = element("div", "mini-card");
          card.append(
            element("strong", null, formatStage(run.status || "unknown")),
            element("span", null, run.trigger_reason || "unspecified"),
            element("span", null, formatTimestamp(run.started_at))
          );
          return card;
        });
        runStrip.replaceChildren(...cards);
      }
    }
  }

  function renderRuntimeModels() {
    const container = document.getElementById("runtime-models");
    if (!container) {
      return;
    }
    const roleModels = ((state.snapshot || {}).room || {}).role_models || {};
    const entries = Object.entries(roleModels);
    if (!entries.length) {
      container.replaceChildren(element("p", "empty-state", "No runtime model metadata recorded yet."));
      return;
    }
    const cards = entries.map(([role, config]) => {
      const card = element("div", "mini-card");
      card.append(
        element("strong", null, formatRole(role)),
        element("span", null, `${config.provider || "n/a"} · ${config.model || "n/a"}`),
        element("span", null, `temperature ${config.temperature ?? "n/a"}`)
      );
      if (config.fallback_used) {
        card.append(element("span", null, "fallback used"));
      }
      return card;
    });
    container.replaceChildren(...cards);
  }

  function renderStrategyAudit() {
    const audit = state.snapshot.strategy_audit || null;
    setSummaryText("strategy-audit-source", audit ? audit.audit_source || "n/a" : "n/a");
    setSummaryText("strategy-audit-version", audit ? audit.audit_version || "n/a" : "n/a");
    setSummaryText("strategy-audit-trade-quality", audit ? formatStage(audit.trade_quality || "n/a") : "n/a");
    setSummaryText("strategy-audit-trainable", audit ? String(Boolean(audit.trainable_default)) : "n/a");
    setSummaryText("strategy-audit-exclusion", audit ? audit.exclude_reason || "none" : "none");

    const warnings = document.getElementById("strategy-audit-warnings");
    if (warnings) {
      const items = safeArray(audit && audit.quality_warnings);
      if (!items.length) {
        warnings.replaceChildren(buildBadge("", "no warnings"));
      } else {
        warnings.replaceChildren(...items.map((warning) => buildBadge("", String(warning))));
      }
    }

    const empty = document.getElementById("strategy-audit-empty");
    if (empty) {
      empty.textContent = audit ? "" : "No persisted strategy audit recorded yet.";
    }
  }

  function renderMemorySummary() {
    const container = document.getElementById("memory-summary");
    if (!container) {
      return;
    }
    const nodes = [];
    const memory = state.snapshot.memory_note;
    if (memory) {
      const card = element("div", "mini-card");
      card.append(element("strong", null, memory.title || "Memory Note"), element("span", null, memory.summary || ""));
      if (Array.isArray(memory.tags) && memory.tags.length) {
        card.append(element("span", null, memory.tags.join(", ")));
      }
      nodes.push(card);
    } else {
      nodes.push(element("p", "empty-state", "No memory note recorded yet."));
    }

    const decision = ((state.snapshot || {}).analytics || {}).decision || {};
    const rationaleCard = element("div", "mini-card");
    rationaleCard.append(element("strong", null, "Rationale Chain"));
    const rationaleIds = safeArray(decision.audit_rationale_ids);
    if (rationaleIds.length) {
      rationaleIds.forEach((item) => rationaleCard.append(element("span", null, String(item))));
    } else {
      rationaleCard.append(element("span", null, "No auditor rationale ids recorded yet."));
    }
    nodes.push(rationaleCard);

    const campaign = state.snapshot.campaign;
    if (campaign) {
      const campaignCard = element("div", "mini-card");
      campaignCard.append(
        element("strong", null, "Campaign Context"),
        element("span", null, campaign.trigger_source || "n/a"),
        element("span", null, `${campaign.city_bucket || "n/a"} · ${campaign.market_regime_bucket || "n/a"}`),
        element("span", null, `${campaign.difficulty_bucket || "n/a"} · ${campaign.outcome_bucket || "n/a"}`)
      );
      nodes.push(campaignCard);
    }

    container.replaceChildren(...nodes);
  }

  function renderSnapshot() {
    renderSummaryStrip();
    renderTimeline();
    renderDecisionPanel();
    renderPricingPanel();
    renderWeatherPanel();
    renderQualityPanel();
    renderSourcePanel();
    renderStrategyAudit();
    renderRuntimeModels();
    renderMemorySummary();
  }

  function mergeMessage(message) {
    const existingIndex = state.messages.findIndex((item) => item.id === message.id);
    if (existingIndex >= 0) {
      state.messages[existingIndex] = message;
    } else {
      state.messages.push(message);
      state.messages.sort((left, right) => Number(left.sequence || 0) - Number(right.sequence || 0));
    }
    state.lastSequence = Math.max(state.lastSequence, Number(message.sequence || 0));
  }

  function handleIncomingMessages(messages) {
    if (!Array.isArray(messages) || !messages.length) {
      return;
    }
    messages.forEach((message) => mergeMessage(message));
    renderTranscript();
    if (state.followEnabled) {
      state.unreadCount = 0;
    } else {
      state.unreadCount += messages.length;
    }
    updateUnreadBadge();
    void fetchSnapshot();
  }

  async function fetchSnapshot() {
    try {
      const response = await fetch(state.snapshotUrl, { headers: { Accept: "application/json" }, cache: "no-store" });
      if (!response.ok) {
        return;
      }
      const snapshot = await response.json();
      state.snapshot = snapshot;
      renderSnapshot();
    } catch (_error) {
      // Keep the current page state if polling fails.
    }
  }

  function connectEventStream() {
    if (!state.eventsUrl || !window.EventSource) {
      return;
    }
    state.eventSource = new window.EventSource(state.eventsUrl);
    state.eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleIncomingMessages(payload);
      } catch (_error) {
        // Ignore malformed event payloads and keep stream alive.
      }
    };
  }

  async function postJson(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(payload || {}),
    });
    if (!response.ok) {
      throw new Error(`Request failed with ${response.status}`);
    }
    return response.json();
  }

  async function handleRunRoom() {
    if (!state.roomId || !runRoomButton) {
      return;
    }
    runRoomButton.disabled = true;
    try {
      await postJson(`/api/rooms/${state.roomId}/run`, { reason: "manual_run" });
      await fetchSnapshot();
    } finally {
      runRoomButton.disabled = false;
    }
  }

  async function handleRefreshResearch() {
    const marketTicker = ((state.snapshot || {}).room || {}).market_ticker;
    if (!marketTicker || !refreshResearchButton) {
      return;
    }
    refreshResearchButton.disabled = true;
    try {
      await postJson(`/api/research/${marketTicker}/refresh`, {});
      window.setTimeout(() => {
        void fetchSnapshot();
      }, 1200);
    } finally {
      refreshResearchButton.disabled = false;
    }
  }

  function handleFollowToggle() {
    state.followEnabled = !state.followEnabled;
    if (state.followEnabled) {
      state.unreadCount = 0;
      scrollTranscriptToBottom();
    }
    updateFollowButton();
    updateUnreadBadge();
  }

  function hydrate() {
    updateFollowButton();
    updateUnreadBadge();
    renderTranscript();
    renderSnapshot();
    if (followToggle) {
      followToggle.addEventListener("click", handleFollowToggle);
    }
    if (runRoomButton) {
      runRoomButton.addEventListener("click", () => {
        void handleRunRoom();
      });
    }
    if (refreshResearchButton) {
      refreshResearchButton.addEventListener("click", () => {
        void handleRefreshResearch();
      });
    }
    connectEventStream();
    state.pollTimer = window.setInterval(() => {
      void fetchSnapshot();
    }, 5000);
  }

  hydrate();
})();
