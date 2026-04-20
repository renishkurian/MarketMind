import asyncio
from backend.data.db import SessionLocal, StockMaster, SignalsCache, FundamentalsCache
from sqlalchemy import select
from backend.engine.consensus.skill_loader import SkillLoader, StockMeta
from backend.engine.scoring.composite_score import CompositeScoreResult

async def test_loader(symbol):
    async with SessionLocal() as session:
        # Load tables
        st_res = await session.execute(select(StockMaster).where(StockMaster.symbol == symbol))
        stock = st_res.scalars().first()
        sig_res = await session.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
        sig = sig_res.scalars().first()
        fund_res = await session.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol))
        fund = fund_res.scalars().first()

        meta = StockMeta(
            symbol=stock.symbol if stock else symbol,
            isin=stock.isin if stock else "N/A",
            exchange=stock.exchange if stock else "NSE",
            sector=stock.sector if stock else "N/A",
            market_cap_cr=float(fund.market_cap)/10000000 if fund and fund.market_cap else 0.0,
            current_price=float(sig.current_price) if sig and sig.current_price else 0.0,
            pe_ratio=float(fund.pe_ratio) if fund and fund.pe_ratio else None,
            roe=float(fund.roe) if fund and fund.roe else None,
            debt_equity=float(fund.debt_equity) if fund and fund.debt_equity else None,
        )

        csr = CompositeScoreResult(
            symbol=symbol,
            isin=meta.isin,
            fundamental_score=float(sig.lt_score) if sig and sig.lt_score else 0.0,
            technical_score=float(sig.st_score) if sig and sig.st_score else 0.0,
            momentum_score=0.0,
            sector_rank_score=0.0,
            composite_score=0.0,
            data_confidence=float(sig.confidence_pct)/100.0 if sig and sig.confidence_pct else 0.5,
            fa_breakdown=sig.indicator_breakdown if sig and sig.indicator_breakdown else {}
        )

        loader = SkillLoader()
        skill_id = "warren_buffett_quality"
        prompt = loader.build_prompt(skill_id, meta, csr)
        print(prompt[:500])

asyncio.run(test_loader('APOLLO'))
