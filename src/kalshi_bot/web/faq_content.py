from __future__ import annotations

FAQ_SECTIONS = [
    {
        "title": "Overview",
        "items": [
            {
                "question": "What is this platform doing?",
                "answer": [
                    "It runs a visible, room-based weather trading workflow for Kalshi. Every room captures research, agent messages, signal generation, risk checks, execution decisions, and audit context.",
                    "The current execution and training scope is structured weather markets only. Shadow/demo collection is the default path for improving the corpus before trusting self-improvement or live execution.",
                ],
                "read_more": ["docs/architecture.md", "docs/operations.md"],
            },
            {
                "question": "What does shadow mode mean?",
                "answer": [
                    "Shadow mode lets the bot run the full room workflow without placing live orders. It still creates dossiers, scores markets, proposes trade tickets when allowed, and records why risk or eligibility would have blocked or allowed an action.",
                    "That gives us training data and operational confidence without turning on live risk.",
                ],
                "read_more": ["docs/operations.md", "docs/training.md"],
            },
        ],
    },
    {
        "title": "Rooms and Agents",
        "items": [
            {
                "question": "What is a room?",
                "answer": [
                    "A room is one decision trace for one market. It has a transcript, research snapshot, signal, trade ticket, risk verdict, execution state, memory note, and strategy audit.",
                    "Rooms are the main unit for operator review and for training exports.",
                ],
                "read_more": ["docs/agent_protocol.md", "docs/training.md"],
            },
            {
                "question": "What are the agent roles?",
                "answer": [
                    "The researcher summarizes evidence, the president sets posture, the trader can propose a structured ticket, the risk officer explains deterministic gates, the execution clerk records exchange actions, the auditor links rationale, and the memory librarian distills the room.",
                    "Even with AI involved, deterministic code still owns risk limits, credential use, and final order submission.",
                ],
                "read_more": ["docs/agent_protocol.md", "docs/strategy/weather-temp-taker.md"],
            },
        ],
    },
    {
        "title": "Research and Strategy",
        "items": [
            {
                "question": "What is a research dossier?",
                "answer": [
                    "A dossier is the shared market research record: structured weather facts, settlement coverage, source cards, claims, freshness, confidence, and a trader-facing fair-value context.",
                    "Rooms take a dossier snapshot and add a room-local delta so every decision is reproducible even if the shared dossier changes later.",
                ],
                "read_more": ["docs/training.md", "docs/strategy/weather-temp-taker.md"],
            },
            {
                "question": "What is a strategy audit?",
                "answer": [
                    "A strategy audit is a post-hoc quality label for a room. It scores thesis correctness, trade quality, block correctness, stale-data mismatch, missed stand-downs, and whether the room should be trainable by default.",
                    "Historical rooms stay immutable; audits are supplemental labels used to clean the corpus.",
                ],
                "read_more": ["docs/training.md", "docs/strategy/weather-microstructure-roadmap.md"],
            },
            {
                "question": "Why do some rooms stand down early now?",
                "answer": [
                    "The base weather strategy now treats resolved contracts, stale research, stale market data, wide spreads, and tiny remaining payout as non-actionable. Those rooms should stand down at eligibility instead of proposing a low-quality ticket and relying on later risk blocks.",
                    "That keeps the live strategy tighter and the training corpus cleaner.",
                ],
                "read_more": ["docs/strategy/weather-temp-taker.md", "docs/strategy/weather-microstructure-roadmap.md"],
            },
        ],
    },
    {
        "title": "Training and Data Maturity",
        "items": [
            {
                "question": "What makes a room trainable?",
                "answer": [
                    "By default, rooms need to be complete, quality-cleaned, and free of stale-data mismatch or weak resolved-contract proposal labels. Good research helps, but the cleaned strategy audit is the final default filter.",
                    "You can still build raw legacy datasets when you want analysis instead of clean training slices.",
                ],
                "read_more": ["docs/training.md"],
            },
            {
                "question": "How does historical replay training work?",
                "answer": [
                    "Historical replay imports settled weather market-days, captured market snapshots, reconstructed checkpoint snapshots from Kalshi candlesticks, and point-in-time weather bundles from the bot archive plus captured raw events. The daemon now also writes dedicated checkpoint weather captures on schedule so future settled days can become fully replayable without depending on room traffic.",
                    "Those replayed rooms are marked historical_replay, kept out of live operator views by default, and exported into bundles, eval slices, or Gemini-first fine-tune files. If there are not enough distinct full-coverage market-days yet, the Gemini export is marked draft-only instead of pretending it is training-ready.",
                ],
                "read_more": ["docs/training.md", "docs/faq.md"],
            },
            {
                "question": "What is settlement backfill?",
                "answer": [
                    "Settlement backfill is the direct repair path for closed shadow or live room markets that still have no settlement label. Instead of waiting only for passive reconcile events, the bot can fetch the final market outcome from Kalshi and persist it as a labeled settlement.",
                    "That helps clear possible ingestion gaps from the maturity backlog and moves older rooms into outcome-aware training slices faster.",
                ],
                "read_more": ["docs/training.md", "docs/operations.md", "docs/faq.md"],
            },
            {
                "question": "What did we learn from the April 12, 2026 deploy?",
                "answer": [
                    "New schema changes require a rebuilt migrate image. Rebuilding only app or daemon containers can leave Alembic stuck on the previous head even though the new code is already live.",
                    "Checkpoint capture returning zero is normal when no checkpoint slot is due, and settlement backfill is now a normal maturity-repair tool for closed markets with missing labels.",
                    "Historical Gemini fine-tuning should still remain draft-only until real full-checkpoint coverage exists; the right fix is more checkpoint coverage, not weaker readiness rules.",
                ],
                "read_more": ["docs/operations.md", "docs/training.md", "docs/faq.md"],
            },
            {
                "question": "Why might self-improvement still be blocked?",
                "answer": [
                    "Because readiness is gated by corpus volume and label quality, not just by plumbing. We need enough complete rooms, enough diversity, enough trade-positive examples, and enough settled rooms before critique, evaluation, or promotion should be trusted.",
                    "If settled coverage is low, the right move is usually to keep shadow collection and reconciliation running rather than lowering the threshold.",
                ],
                "read_more": ["docs/self_improve.md", "docs/training.md"],
            },
            {
                "question": "What should I watch on the dashboard?",
                "answer": [
                    "The training panel shows room counts, cleaned-trainable share, recent exclusion memory, quality debt, unsettled backlog, settled-label velocity, and readiness blockers.",
                    "If the next bottleneck is not obvious from that panel, something is missing in the status surface and we should improve it.",
                ],
                "read_more": ["docs/training.md", "docs/operations.md"],
            },
        ],
    },
    {
        "title": "Operations and Safety",
        "items": [
            {
                "question": "How do blue/green deploys and the watchdog work?",
                "answer": [
                    "Both colors run on the same host, but only the active color can own the execution path. The watchdog monitors app and daemon health, restarts unhealthy colors, and can fail over the active color if needed.",
                    "Boot, restart, and recovery stay host-native through systemd plus Docker Compose.",
                ],
                "read_more": ["docs/operations.md", "docs/runbooks.md"],
            },
            {
                "question": "What do kill switch and active color mean?",
                "answer": [
                    "The kill switch blocks new live execution even if rooms continue to run. Active color is the stack currently allowed to own execution-related responsibilities.",
                    "Keeping the kill switch on is the safest default while we are still collecting and cleaning data.",
                ],
                "read_more": ["docs/operations.md", "docs/security.md"],
            },
            {
                "question": "Where do I inspect the system quickly?",
                "answer": [
                    "Use the Control Room for runtime health, room creation, training status, audits, and research status. Use room pages for the detailed decision trace. Use the CLI for exact JSON and operational scripts when you need machine-readable state.",
                    "Useful commands include training-status, research-audit, strategy-audit summary, shadow-campaign run, reconcile, and self-improve status.",
                ],
                "read_more": ["README.md", "docs/operations.md"],
            },
        ],
    },
]
