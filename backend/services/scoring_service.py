"""
MarketMind — Production Scoring Service
=========================================
Wires SignalBuilder → CompositeScorer → SignalsCache persistence.
Drop this in: backend/services/scoring_service.py

Replaces all stubs from integration_example.py with real DB queries
against your actual schema (StockMaster, PriceHistory, FundamentalsCache,
SignalsCache, AIInsights).

Usage (from FastAPI route or APScheduler job):
    service = ScoringService(db)
    result  = await service.score_symbol("RELIANCE", "NSE")
    # → CompositeScoreResult persisted to SignalsCache, returned to caller
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, and_, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.data.db import (
    StockMaster, SignalsCache, FundamentalsCache,
    PriceHistory, AIInsights
)
from backend.engine.scoring.composite_score import (
    CompositeScorer, ScoreConfig,
    CompositeScoreResult, result_to_cache_dict,
    SCORE_VERSION,
)
from backend.engine.scoring.signal_builder import SignalBuilder, build_sector_data
from backend.engine.backtest.backtest_engine import BacktestEngine, BacktestMetrics
from backend.engine.consensus.skill_loader import SkillLoader, StockMeta
from backend.engine.consensus.consensus_engine import (
    ConsensusEngine, SkillAnalysis, SkillVerdict
)


# ---------------------------------------------------------------------------
# Scoring Service
# ---------------------------------------------------------------------------

class ScoringService:
    """
    End-to-end scoring for one symbol.
    Reads from: StockMaster, FundamentalsCache, PriceHistory, SignalsCache
    Writes to:  SignalsCache (score columns), AIInsights (if AI run)
    """

    def __init__(
        self,
        db: AsyncSession,
        profile: str = "long_term_compounding",
    ) -> None:
        self.db = db
        self.scorer = CompositeScorer(ScoreConfig(profile=profile))
        self.builder = SignalBuilder(db)
        self.skill_loader = SkillLoader()
        self.consensus = ConsensusEngine()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def score_symbol(
        self,
        symbol: str,
        exchange: str = "NSE",
        as_of_date: Optional[date] = None,
        run_backtest: bool = False,
        run_ai_skills: Optional[list[str]] = None,
        ai_client=None,  # your existing AI client (Claude/GPT/Grok)
    ) -> CompositeScoreResult:
        """
        Full pipeline:
          1. Fetch StockMaster metadata
          2. Build FA from FundamentalsCache
          3. Compute TA + Momentum from PriceHistory
          4. Build SectorData from peer SignalsCache
          5. Run CompositeScorer
          6. Persist to SignalsCache
          7. Optional: run backtest, persist metrics
          8. Optional: run AI skills, persist to AIInsights
        """
        if as_of_date is None:
            as_of_date = date.today()

        # 1. StockMaster
        stock = await self._fetch_stock(symbol, exchange)
        isin = stock.isin or ""
        sector = stock.sector or ""

        # 2. Fundamentals
        fa = await self.builder.build_fa(symbol, isin)

        # 3. Technical + Momentum
        ta  = await self.builder.build_ta(symbol, exchange, as_of_date)
        mom = await self.builder.build_momentum(symbol, exchange, as_of_date)

        # 4. Sector peers
        sector_data = await build_sector_data(self.db, symbol, sector, exchange)

        # 5. Score
        result = self.scorer.score(
            symbol=symbol,
            isin=isin,
            fa=fa,
            ta=ta,
            momentum=mom,
            sector=sector_data,
        )

        # 6. Persist scores to SignalsCache
        current_price = await self._latest_close(symbol, exchange, as_of_date)
        await self._upsert_signals_cache(symbol, exchange, result, current_price, as_of_date)

        # 7. Backtest (optional — expensive, run in background job not per-request)
        bt_metrics: Optional[BacktestMetrics] = None
        if run_backtest and isin:
            bt_metrics = await self._run_backtest(symbol, isin, exchange)
            if bt_metrics:
                await self._persist_backtest(symbol, exchange, bt_metrics)

        # 8. AI Skills (optional — triggered by frontend or weekly job)
        if run_ai_skills and ai_client:
            meta = self._build_stock_meta(stock, fa, mom, result)
            await self._run_ai_skills(
                run_ai_skills, meta, result, bt_metrics, ai_client
            )

        return result

    # ------------------------------------------------------------------
    # Batch scoring — all active stocks in portfolio/watchlist
    # ------------------------------------------------------------------

    async def score_all(
        self,
        exchange: str = "NSE",
        stock_type: Optional[str] = None,  # 'PORTFOLIO', 'WATCHLIST', or None = both
        as_of_date: Optional[date] = None,
    ) -> dict[str, CompositeScoreResult]:
        """
        Score every active stock in StockMaster.
        Typically called by APScheduler after EOD bhavcopy sync.
        """
        query = select(StockMaster).where(
            and_(
                StockMaster.is_active == True,
                StockMaster.exchange == exchange,
            )
        )
        if stock_type:
            query = query.where(StockMaster.type == stock_type)

        result = await self.db.execute(query)
        stocks = result.scalars().all()

        results: dict[str, CompositeScoreResult] = {}
        for stock in stocks:
            try:
                r = await self.score_symbol(
                    stock.symbol, stock.exchange, as_of_date
                )
                results[stock.symbol] = r
            except Exception as exc:
                # Log and continue — don't abort the batch
                print(f"[score_all] {stock.symbol} failed: {exc}")

        return results

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    async def _upsert_signals_cache(
        self,
        symbol: str,
        exchange: str,
        result: CompositeScoreResult,
        current_price: Optional[float],
        as_of_date: date,
    ) -> None:
        """
        UPDATE existing SignalsCache row's score columns, or INSERT if missing.
        Uses MySQL INSERT ... ON DUPLICATE KEY UPDATE pattern via symbol+exchange.
        Only touches the v2.1 score columns — leaves st_signal, lt_signal etc. intact.
        """
        now = datetime.now(timezone.utc)

        # Build the score column dict
        score_cols = {
            "composite_score":      result.composite_score,
            "fundamental_score":    result.fundamental_score,
            "technical_score":      result.technical_score,
            "momentum_score":       result.momentum_score,
            "sector_rank_score":    result.sector_rank_score,
            "sector_percentile":    result.sector_percentile,
            "sector_peer_count":    result.sector_peer_count,
            "data_confidence":      result.data_confidence,
            "fa_coverage":          result.fa_coverage,
            "ta_coverage":          result.ta_coverage,
            "momentum_coverage":    result.momentum_coverage,
            "promoter_pledge_warning": result.promoter_pledge_warning,
            "score_version":        result.score_version,
            "scored_at":            now,
            "score_profile":        result.profile,
            "fa_breakdown":         result.fa_breakdown,
            "ta_breakdown":         result.ta_breakdown,
            "momentum_breakdown":   result.momentum_breakdown,
        }

        # Check if a row already exists for this symbol+exchange
        existing = await self.db.execute(
            select(SignalsCache.id)
            .where(
                and_(
                    SignalsCache.symbol == symbol,
                    SignalsCache.exchange == exchange,
                )
            )
            .order_by(SignalsCache.computed_at.desc())
            .limit(1)
        )
        row_id = existing.scalar_one_or_none()

        if row_id:
            # Update existing row
            await self.db.execute(
                update(SignalsCache)
                .where(SignalsCache.id == row_id)
                .values(**score_cols)
            )
        else:
            # Insert minimal new row
            new_row = SignalsCache(
                symbol=symbol,
                exchange=exchange,
                computed_at=now,
                market_session="EOD",
                current_price=current_price,
                **score_cols,
            )
            self.db.add(new_row)

        await self.db.commit()

    async def _persist_backtest(
        self,
        symbol: str,
        exchange: str,
        bt: BacktestMetrics,
    ) -> None:
        """Writes backtest results into SignalsCache backtest_* columns."""
        await self.db.execute(
            update(SignalsCache)
            .where(
                and_(
                    SignalsCache.symbol == symbol,
                    SignalsCache.exchange == exchange,
                )
            )
            .values(
                backtest_cagr=bt.cagr,
                backtest_win_rate=bt.win_rate,
                backtest_sharpe=bt.sharpe_ratio,
                backtest_max_drawdown=bt.max_drawdown,
                backtest_trades=bt.trades_taken,
            )
        )
        await self.db.commit()

    async def _persist_ai_insight(
        self,
        symbol: str,
        skill_id: str,
        narrative: str,
        verdict: str,
        score_result: CompositeScoreResult,
        consensus_score: Optional[float] = None,
        bull_count: int = 0,
        bear_count: int = 0,
        neutral_count: int = 0,
        forensic_veto: bool = False,
        all_verdicts: Optional[dict] = None,
    ) -> None:
        """Persists AI narrative to AIInsights table."""
        insight = AIInsights(
            symbol=symbol,
            generated_at=datetime.now(timezone.utc),
            trigger_reason="MANUAL",
            short_summary=narrative[:500] if narrative else "",
            long_summary=narrative[:5000] if narrative else "",
            skill_id=skill_id,
            verdict=verdict[:20] if verdict else "",
            sentiment_score=self._verdict_to_sentiment(verdict),
            # New v2.1 fields
            consensus_score=consensus_score,
            bull_count=bull_count,
            bear_count=bear_count,
            neutral_count=neutral_count,
            forensic_veto=forensic_veto,
            all_verdicts=all_verdicts or {},
            composite_score_snapshot=score_result.composite_score,
            score_version_snapshot=score_result.score_version,
        )
        self.db.add(insight)
        await self.db.commit()

    # ------------------------------------------------------------------
    # Backtest runner
    # ------------------------------------------------------------------

    async def _run_backtest(
        self,
        symbol: str,
        isin: str,
        exchange: str,
        score_threshold: float = 65.0,
        hold_days: int = 252,
    ) -> Optional[BacktestMetrics]:
        try:
            bt_engine = BacktestEngine(
                price_fetcher=self._price_fetcher,
                signal_fetcher=self._signal_fetcher,
            )
            return await bt_engine.run(
                symbol=symbol,
                isin=isin,
                signal_type="composite_score",
                score_threshold=score_threshold,
                hold_days=hold_days,
                start_date=date(2016, 1, 1),
            )
        except Exception as exc:
            print(f"[backtest] {symbol} failed: {exc}")
            return None

    async def _price_fetcher(self, isin: str, start_date: date, end_date: date):
        """Adapts PriceHistory query → list[PriceBar] for BacktestEngine."""
        from backend.engine.backtest.backtest_engine import PriceBar
        result = await self.db.execute(
            select(PriceHistory)
            .where(
                and_(
                    PriceHistory.isin == isin,
                    PriceHistory.date >= start_date,
                    PriceHistory.date <= end_date,
                )
            )
            .order_by(PriceHistory.date.asc())
        )
        return [
            PriceBar(
                date=row.date,
                open=float(row.open or row.close),
                high=float(row.high or row.close),
                low=float(row.low  or row.close),
                close=float(row.close),
                volume=int(row.volume or 0),
            )
            for row in result.scalars().all()
        ]

    async def _signal_fetcher(
        self, isin: str, signal_type: str, start_date: date, end_date: date
    ):
        """Adapts SignalsCache history → list[SignalEvent] for BacktestEngine."""
        from backend.engine.backtest.backtest_engine import SignalEvent
        # Join SignalsCache with StockMaster to get isin
        stmt = (
            select(
                SignalsCache.symbol,
                SignalsCache.composite_score,
                SignalsCache.computed_at,
                StockMaster.sector,
            )
            .join(StockMaster, StockMaster.symbol == SignalsCache.symbol)
            .where(
                and_(
                    StockMaster.isin == isin,
                    SignalsCache.computed_at >= start_date,
                    SignalsCache.computed_at <= end_date,
                    SignalsCache.composite_score.isnot(None),
                )
            )
            .order_by(SignalsCache.computed_at.asc())
        )
        result = await self.db.execute(stmt)
        return [
            SignalEvent(
                signal_date=(
                    row.computed_at.date()
                    if isinstance(row.computed_at, datetime)
                    else row.computed_at
                ),
                symbol=row.symbol,
                isin=isin,
                signal_type="composite_score",
                signal_value=float(row.composite_score),
                composite_score=float(row.composite_score),
                sector=row.sector or "",
            )
            for row in result.fetchall()
        ]

    # ------------------------------------------------------------------
    # AI skills runner
    # ------------------------------------------------------------------

    async def _run_ai_skills(
        self,
        skill_names: list[str],
        meta: StockMeta,
        score_result: CompositeScoreResult,
        bt_metrics: Optional[BacktestMetrics],
        ai_client,
    ) -> None:
        skill_analyses: list[SkillAnalysis] = []

        for skill_name in skill_names:
            try:
                prompt = self.skill_loader.build_prompt(
                    skill_name=skill_name,
                    stock_meta=meta,
                    score_result=score_result,
                    backtest_metrics=bt_metrics,
                )
                narrative = await ai_client.complete(
                    prompt=prompt,
                    max_tokens=1200,
                    temperature=0.3,
                )
                verdict = self.consensus.extract_verdict(narrative)
                info = self.skill_loader.SKILL_REGISTRY.get(skill_name, {})
                skill_analyses.append(SkillAnalysis(
                    skill_name=skill_name,
                    display_name=info.get("display_name", skill_name),
                    verdict=verdict,
                    narrative=narrative,
                    confidence=score_result.data_confidence,
                ))
            except Exception as exc:
                print(f"[AI skill] {skill_name} failed: {exc}")

        if not skill_analyses:
            return

        consensus = self.consensus.compute_consensus(
            symbol=meta.symbol,
            isin=meta.isin,
            skill_analyses=skill_analyses,
        )

        # Persist each skill narrative individually to AIInsights
        for analysis in skill_analyses:
            await self._persist_ai_insight(
                symbol=meta.symbol,
                skill_id=analysis.skill_name,
                narrative=analysis.narrative,
                verdict=analysis.verdict.value,
                score_result=score_result,
                consensus_score=consensus.consensus_score,
                bull_count=consensus.bull_count,
                bear_count=consensus.bear_count,
                neutral_count=consensus.neutral_count,
                forensic_veto=consensus.forensic_veto,
                all_verdicts={a.skill_name: a.verdict.value for a in skill_analyses},
            )

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------

    async def _fetch_stock(self, symbol: str, exchange: str) -> StockMaster:
        result = await self.db.execute(
            select(StockMaster).where(
                and_(
                    StockMaster.symbol == symbol,
                    StockMaster.exchange == exchange,
                    StockMaster.is_active == True,
                )
            )
        )
        stock = result.scalar_one_or_none()
        if not stock:
            raise ValueError(f"Stock {symbol} ({exchange}) not found in StockMaster")
        return stock

    async def _latest_close(
        self, symbol: str, exchange: str, as_of_date: date
    ) -> Optional[float]:
        result = await self.db.execute(
            select(PriceHistory.close)
            .where(
                and_(
                    PriceHistory.symbol == symbol,
                    PriceHistory.exchange == exchange,
                    PriceHistory.date <= as_of_date,
                )
            )
            .order_by(PriceHistory.date.desc())
            .limit(1)
        )
        val = result.scalar_one_or_none()
        return float(val) if val else None

    def _build_stock_meta(
        self,
        stock: StockMaster,
        fa,
        mom,
        result: CompositeScoreResult,
    ) -> StockMeta:
        """Builds StockMeta for skill template injection."""
        return StockMeta(
            symbol=stock.symbol,
            isin=stock.isin or "",
            exchange=stock.exchange,
            sector=stock.sector or "",
            market_cap_cr=float(stock.market_cap_cat or 0),
            current_price=0.0,  # filled by caller if needed
            pe_ratio=fa.pe_ratio,
            pe_5yr_avg=fa.pe_5yr_avg,
            roe=fa.roe,
            roe_3yr_avg=fa.roe_3yr_avg,
            debt_equity=fa.debt_equity,
            revenue_growth_3yr=fa.revenue_growth_3yr,
            pat_growth_3yr=fa.pat_growth_3yr,
            operating_margin=fa.operating_margin,
            promoter_holding=fa.promoter_holding,
            promoter_pledge_pct=fa.promoter_pledge_pct,
            roc_252=mom.roc_252,
            roc_60=mom.roc_60,
            volume_ratio_20_90=mom.volume_ratio_20_90,
        )

    @staticmethod
    def _verdict_to_sentiment(verdict: str) -> float:
        """Maps verdict string to sentiment_score for AIInsights."""
        mapping = {
            "STRONG_BUY": 1.0,  "HIGH_CONVICTION_BUY": 1.0,
            "BUY": 0.75,        "ACCUMULATE": 0.6,
            "HOLD": 0.5,        "TRACKER": 0.5,
            "WATCH": 0.35,      "CAUTION": 0.35,
            "AVOID": 0.15,      "CRITICAL": 0.0,
            "NOT_APPLICABLE": 0.5,
        }
        return mapping.get(verdict.upper(), 0.5)
