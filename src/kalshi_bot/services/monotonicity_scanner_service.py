"""MonotonicityArbScannerService — orchestrates periodic scans and DB persistence."""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.models import MonotonicityArbProposal
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.monotonicity_scanner import ArbProposal, scan_for_violations

logger = logging.getLogger(__name__)


class MonotonicityArbScannerService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        kalshi: KalshiClient,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._kalshi = kalshi

    async def sweep(self) -> list[ArbProposal]:
        """Run one monotonicity arb scan tick across all open KXHIGH* markets.

        Fetches open markets from Kalshi, detects violations, persists proposals,
        and returns the full proposal list (suppressed and actionable).
        """
        if not self._settings.monotonicity_arb_enabled:
            logger.debug("monotonicity_arb: disabled — skipping sweep")
            return []

        markets = await self._fetch_open_kxhigh_markets()
        control = await self._load_control()

        proposals = scan_for_violations(
            markets,
            control=control,
            settings=self._settings,
        )

        for proposal in proposals:
            await self._persist(proposal)

        logger.info(
            "monotonicity_arb: sweep complete — %d markets, %d proposals (%d shadow)",
            len(markets),
            len(proposals),
            sum(1 for p in proposals if p.execution_outcome == "shadow"),
        )
        return proposals

    async def get_status(self) -> dict[str, Any]:
        """Return aggregate monotonicity arb metrics."""
        async with self._session_factory() as session:
            total = (await session.execute(
                select(func.count()).select_from(MonotonicityArbProposal)
            )).scalar_one()
            shadow = (await session.execute(
                select(func.count()).select_from(MonotonicityArbProposal).where(
                    MonotonicityArbProposal.execution_outcome == "shadow"
                )
            )).scalar_one()
            recent = (await session.execute(
                select(MonotonicityArbProposal)
                .order_by(MonotonicityArbProposal.detected_at.desc())
                .limit(10)
            )).scalars().all()

        return {
            "enabled": self._settings.monotonicity_arb_enabled,
            "shadow_only": self._settings.monotonicity_arb_shadow_only,
            "total_proposals": total,
            "shadow_proposals": shadow,
            "recent": [
                {
                    "ticker_low": r.ticker_low,
                    "ticker_high": r.ticker_high,
                    "net_edge_cents": r.net_edge_cents,
                    "execution_outcome": r.execution_outcome,
                    "detected_at": r.detected_at.isoformat(),
                }
                for r in recent
            ],
        }

    async def _fetch_open_kxhigh_markets(self) -> list[dict[str, Any]]:
        """Fetch all open KXHIGH* markets from Kalshi."""
        try:
            response = await self._kalshi.list_markets(
                status="open",
                series_ticker="KXHIGH",
                limit=200,
            )
            markets = response.get("markets", [])
            return [m for m in markets if m.get("ticker", "").startswith("KXHIGH")]
        except Exception:
            logger.warning("monotonicity_arb: failed to fetch markets", exc_info=True)
            return []

    async def _load_control(self):
        async with self._session_factory() as session:
            repo = PlatformRepository(session, kalshi_env=self._settings.kalshi_env)
            return await repo.get_deployment_control(kalshi_env=self._settings.kalshi_env)

    async def _persist(self, proposal: ArbProposal) -> None:
        record = MonotonicityArbProposal(
            station=proposal.station,
            event_date=proposal.event_date,
            ticker_low=proposal.ticker_low,
            ticker_high=proposal.ticker_high,
            threshold_low_f=proposal.threshold_low_f,
            threshold_high_f=proposal.threshold_high_f,
            ask_yes_low_cents=proposal.ask_yes_low_cents,
            ask_no_high_cents=proposal.ask_no_high_cents,
            total_cost_cents=proposal.total_cost_cents,
            gross_edge_cents=proposal.gross_edge_cents,
            fee_estimate_cents=proposal.fee_estimate_cents,
            net_edge_cents=proposal.net_edge_cents,
            contracts_proposed=proposal.contracts_proposed,
            execution_outcome=proposal.execution_outcome,
            suppression_reason=proposal.suppression_reason,
            detected_at=proposal.detected_at,
        )
        async with self._session_factory() as session:
            session.add(record)
            await session.commit()

        logger.info(
            "monotonicity_arb: %s/%s T%.0f<T%.0f net=%.2f¢ outcome=%s",
            proposal.station,
            proposal.event_date,
            proposal.threshold_low_f,
            proposal.threshold_high_f,
            proposal.net_edge_cents,
            proposal.execution_outcome,
        )
