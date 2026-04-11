from __future__ import annotations

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import EvaluationMetrics
from kalshi_bot.services.agent_packs import AgentPackService
from kalshi_bot.services.self_improve import SelfImproveService


def test_self_improve_evaluation_summary_blocks_regressions_and_invalid_payloads() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = SelfImproveService(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        AgentPackService(settings),
        None,
    )

    summary = service._evaluation_summary(
        champion_version="champion",
        candidate_version="candidate",
        champion_metrics=EvaluationMetrics(
            composite_score=0.80,
            research_quality=0.85,
            directional_agreement=0.80,
            risk_compliance=0.95,
            memory_usefulness=0.70,
            sample_size=10,
        ),
        candidate_metrics=EvaluationMetrics(
            composite_score=0.79,
            research_quality=0.70,
            directional_agreement=0.79,
            risk_compliance=0.95,
            memory_usefulness=0.72,
            invalid_payload_rate=0.1,
            sample_size=10,
        ),
    )

    assert summary.passed is False
    assert any("invalid payloads" in reason.lower() for reason in summary.reasons)
    assert any("improve" in reason.lower() for reason in summary.reasons)


def test_self_improve_guardrail_failure_reason_detects_regression_spikes() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = SelfImproveService(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        AgentPackService(settings),
        None,
    )

    reason = service._guardrail_failure_reason(
        {
            "research_block_rate": 0.7,
            "blocked_rate": 0.2,
            "stale_rate": 0.0,
            "drawdown": 0.0,
        }
    )

    assert reason == "research gate regression spike"


def test_self_improve_evaluation_summary_passes_on_clean_improvement() -> None:
    settings = Settings(database_url="sqlite+aiosqlite:///./test.db")
    service = SelfImproveService(  # type: ignore[arg-type]
        settings,
        None,
        None,
        None,
        AgentPackService(settings),
        None,
    )

    summary = service._evaluation_summary(
        champion_version="champion",
        candidate_version="candidate",
        champion_metrics=EvaluationMetrics(
            composite_score=0.80,
            research_quality=0.80,
            directional_agreement=0.80,
            risk_compliance=0.95,
            memory_usefulness=0.70,
            sample_size=10,
        ),
        candidate_metrics=EvaluationMetrics(
            composite_score=0.84,
            research_quality=0.82,
            directional_agreement=0.82,
            risk_compliance=0.95,
            memory_usefulness=0.74,
            sample_size=10,
        ),
    )

    assert summary.passed is True
    assert summary.reasons == ["Evaluation passed."]
