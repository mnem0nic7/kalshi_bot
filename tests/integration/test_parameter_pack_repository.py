from __future__ import annotations

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.learning.parameter_pack import default_parameter_pack, parameter_pack_hash


@pytest.mark.asyncio
async def test_parameter_pack_repository_round_trip(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/parameter_pack.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    pack = default_parameter_pack(version="params-v1")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env="demo")
        record = await repo.update_parameter_pack(
            pack,
            holdout_report={"coverage": 0.97, "brier": 0.19},
        )
        fetched = await repo.get_parameter_pack("params-v1")
        champion = await repo.get_champion_parameter_pack()
        listed = await repo.list_parameter_packs(limit=1)
        await session.commit()

    assert record.pack_hash == parameter_pack_hash(pack)
    assert fetched is not None
    assert fetched.payload["parameters"]["pseudo_count"] == 8
    assert fetched.holdout_report["coverage"] == 0.97
    assert champion is not None
    assert champion.version == "params-v1"
    assert listed[0].version == "params-v1"

    await engine.dispose()
