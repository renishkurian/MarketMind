"""
MarketMind — Composite Scoring Engine v2
=========================================
Designed for long-term wealth compounding (3–10 yr holds).

Score Architecture
------------------
  CompositeScore (0–100)
  ├── FundamentalScore   (0–100)  — quality, value, growth
  ├── TechnicalScore     (0–100)  — trend, momentum, structure
  ├── MomentumScore      (0–100)  — price + volume acceleration
  └── SectorRelativeRank (0–100)  — percentile vs sector peers

Default weights (long-term compounding profile):
  FA: 45%  |  TA: 25%  |  Momentum: 15%  |  Sector Rank: 15%

All raw metric values are pulled from SignalsCache + PriceHistory.
Results are written back to SignalsCache (new columns) and AIInsights context.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Weight profiles — swap at runtime via ScoreConfig
# ---------------------------------------------------------------------------

WEIGHT_PROFILES = {
    "long_term_compounding": {
        "fa": 0.45,
        "ta": 0.25,
        "momentum": 0.15,
        "sector_rank": 0.15,
    },
    "swing_trading": {
        "fa": 0.20,
        "ta": 0.45,
        "momentum": 0.25,
        "sector_rank": 0.10,
    },
    "momentum_following": {
        "fa": 0.10,
        "ta": 0.30,
        "momentum": 0.45,
        "sector_rank": 0.15,
    },
    "balanced": {
        "fa": 0.30,
        "ta": 0.30,
        "momentum": 0.20,
        "sector_rank": 0.20,
    },
}


@dataclass
class ScoreConfig:
    """Runtime configuration for the scoring engine."""
    profile: str = "long_term_compounding"
    fa_weight: float = 0.0
    ta_weight: float = 0.0
    momentum_weight: float = 0.0
    sector_rank_weight: float = 0.0

    def __post_init__(self):
        if self.profile in WEIGHT_PROFILES:
            w = WEIGHT_PROFILES[self.profile]
            self.fa_weight = w["fa"]
            self.ta_weight = w["ta"]
            self.momentum_weight = w["momentum"]
            self.sector_rank_weight = w["sector_rank"]
        # Custom weights — normalise to sum = 1.0
        total = self.fa_weight + self.ta_weight + self.momentum_weight + self.sector_rank_weight
        if total > 0 and abs(total - 1.0) > 0.001:
            self.fa_weight /= total
            self.ta_weight /= total
            self.momentum_weight /= total
            self.sector_rank_weight /= total


# ---------------------------------------------------------------------------
# Input data containers
# ---------------------------------------------------------------------------

@dataclass
class FundamentalData:
    """
    Sourced from SignalsCache.fa_* columns.
    All values as floats; None = data unavailable (score degrades gracefully).
    """
    pe_ratio: Optional[float] = None            # Current trailing PE
    pe_5yr_avg: Optional[float] = None          # 5-year average PE (from PriceHistory)
    roe: Optional[float] = None                 # Return on equity (%)
    roe_3yr_avg: Optional[float] = None         # 3-year average ROE (%)
    debt_equity: Optional[float] = None         # Debt / Equity ratio
    revenue_growth_3yr: Optional[float] = None  # 3-year CAGR revenue growth (%)
    pat_growth_3yr: Optional[float] = None      # 3-year CAGR PAT growth (%)
    operating_margin: Optional[float] = None    # Operating margin (%)
    promoter_holding: Optional[float] = None    # Promoter holding % (NSE/BSE filing)
    promoter_pledge_pct: Optional[float] = None # Pledged promoter shares % — penalty signal


@dataclass
class TechnicalData:
    """Sourced from SignalsCache.ta_* columns."""
    rsi_14: Optional[float] = None        # 0–100
    macd_signal: Optional[float] = None   # MACD line minus signal line
    price_vs_sma20: Optional[float] = None  # % above/below 20-day SMA
    price_vs_sma50: Optional[float] = None  # % above/below 50-day SMA
    price_vs_sma200: Optional[float] = None # % above/below 200-day SMA
    bb_position: Optional[float] = None   # 0 = lower band, 1 = upper band
    adx: Optional[float] = None           # Average Directional Index (trend strength)
    
    # Institutional/Liquidity Metrics
    avg_trades_20: Optional[float] = None
    trades_shock: Optional[float] = None  # ratio (Current / SMA20)


@dataclass
class MomentumData:
    """
    Derived from PriceHistory — computed fresh each EOD.
    Using 2016-present data gives statistically robust lookback windows.
    """
    roc_20: Optional[float] = None        # 20-day rate of change (%)
    roc_60: Optional[float] = None        # 60-day rate of change (%)
    roc_252: Optional[float] = None       # 252-day (1yr) rate of change (%)
    volume_ratio_20_90: Optional[float] = None  # 20-day avg vol / 90-day avg vol
    price_52w_rank: Optional[float] = None      # 0–1: where price sits in 52w range
    relative_strength_nifty: Optional[float] = None  # Stock returns / Nifty returns (6m)


@dataclass
class SectorData:
    """
    Peer group data for sector-relative Z-score ranking.
    sector_pe_list etc. are lists of all peers' values in that sector.
    """
    sector: str = ""
    sector_pe_list: list[float] = field(default_factory=list)
    sector_roe_list: list[float] = field(default_factory=list)
    sector_revenue_growth_list: list[float] = field(default_factory=list)
    sector_momentum_list: list[float] = field(default_factory=list)  # raw momentum scores


# ---------------------------------------------------------------------------
# Score output
# ---------------------------------------------------------------------------

@dataclass
class CompositeScoreResult:
    symbol: str
    isin: str

    # Component scores (0–100)
    fundamental_score: float = 0.0
    technical_score: float = 0.0
    momentum_score: float = 0.0
    sector_rank_score: float = 0.0  # 0 = worst in sector, 100 = best

    # Final blended score
    composite_score: float = 0.0

    # Confidence: 0–1 based on data completeness
    data_confidence: float = 0.0

    # Sub-signals for AI skill context
    fa_breakdown: dict = field(default_factory=dict)
    ta_breakdown: dict = field(default_factory=dict)
    momentum_breakdown: dict = field(default_factory=dict)

    # Flags
    promoter_pledge_warning: bool = False
    sector_percentile: float = 0.0  # 0–100: percentile rank in sector

    # Scoring profile used
    profile: str = "long_term_compounding"


# ---------------------------------------------------------------------------
# Scoring Engine
# ---------------------------------------------------------------------------

class CompositeScorer:
    """
    Main scoring engine. Instantiate once, call score() per stock.

    Usage:
        config = ScoreConfig(profile="long_term_compounding")
        scorer = CompositeScorer(config)
        result = scorer.score(
            symbol="RELIANCE",
            isin="INE002A01018",
            fa=FundamentalData(...),
            ta=TechnicalData(...),
            momentum=MomentumData(...),
            sector=SectorData(...),
        )
    """

    def __init__(self, config: ScoreConfig | None = None):
        self.config = config or ScoreConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        symbol: str,
        isin: str,
        fa: FundamentalData,
        ta: TechnicalData,
        momentum: MomentumData,
        sector: SectorData,
    ) -> CompositeScoreResult:

        result = CompositeScoreResult(symbol=symbol, isin=isin, profile=self.config.profile)

        fa_score, fa_bd, fa_conf = self._score_fundamentals(fa)
        ta_score, ta_bd, ta_conf = self._score_technicals(ta)
        mom_score, mom_bd, mom_conf = self._score_momentum(momentum)
        sec_score, sec_pct = self._score_sector_relative(fa, momentum, sector)

        result.fundamental_score = fa_score
        result.technical_score = ta_score
        result.momentum_score = mom_score
        result.sector_rank_score = sec_score
        result.sector_percentile = sec_pct

        result.fa_breakdown = fa_bd
        result.ta_breakdown = ta_bd
        result.momentum_breakdown = mom_bd

        # Confidence-weighted composite
        total_conf = fa_conf + ta_conf + mom_conf + 1.0  # sector always has 1.0 conf
        w = self.config

        result.composite_score = round(
            (fa_score * w.fa_weight)
            + (ta_score * w.ta_weight)
            + (mom_score * w.momentum_weight)
            + (sec_score * w.sector_rank_weight),
            2,
        )

        result.data_confidence = round(
            (fa_conf * w.fa_weight + ta_conf * w.ta_weight + mom_conf * w.momentum_weight + 1.0 * w.sector_rank_weight),
            3,
        )

        # Promoter pledge warning (hard flag regardless of score)
        if fa.promoter_pledge_pct is not None and fa.promoter_pledge_pct > 20.0:
            result.promoter_pledge_warning = True

        return result

    # ------------------------------------------------------------------
    # Fundamental Score (0–100)
    # Long-term focus: quality > value > growth
    # ------------------------------------------------------------------

    def _score_fundamentals(
        self, fa: FundamentalData
    ) -> tuple[float, dict, float]:
        points = []
        breakdown = {}
        available = 0

        # 1. PE relative to 5yr avg (value signal — 20 pts)
        if fa.pe_ratio is not None and fa.pe_5yr_avg is not None and fa.pe_5yr_avg > 0:
            discount = (fa.pe_5yr_avg - fa.pe_ratio) / fa.pe_5yr_avg  # positive = cheaper
            pe_score = _clamp(_linear_scale(discount, -0.5, 0.5, 0, 100), 0, 100)
            points.append((pe_score, 20))
            breakdown["pe_vs_5yr"] = round(pe_score, 1)
            available += 20

        # 2. ROE quality (25 pts) — long-term compounders need sustained ROE > 15%
        if fa.roe is not None:
            roe_score = _clamp(_linear_scale(fa.roe, 5.0, 30.0, 0, 100), 0, 100)
            roe_consistency_bonus = 0.0
            if fa.roe_3yr_avg is not None:
                # Reward consistency: current ROE close to 3yr avg = stable business
                consistency = 1.0 - abs(fa.roe - fa.roe_3yr_avg) / max(fa.roe_3yr_avg, 1.0)
                roe_consistency_bonus = _clamp(consistency * 15, 0, 15)
            combined_roe = _clamp(roe_score * 0.85 + roe_consistency_bonus, 0, 100)
            points.append((combined_roe, 25))
            breakdown["roe_quality"] = round(combined_roe, 1)
            available += 25

        # 3. Debt/Equity (15 pts) — Buffett/RJ prefer low-debt compounders
        if fa.debt_equity is not None:
            de_score = _clamp(_linear_scale(fa.debt_equity, 2.0, 0.0, 0, 100), 0, 100)
            points.append((de_score, 15))
            breakdown["debt_equity"] = round(de_score, 1)
            available += 15

        # 4. Revenue growth 3yr CAGR (15 pts)
        if fa.revenue_growth_3yr is not None:
            rev_score = _clamp(_linear_scale(fa.revenue_growth_3yr, -5.0, 25.0, 0, 100), 0, 100)
            points.append((rev_score, 15))
            breakdown["revenue_growth_3yr"] = round(rev_score, 1)
            available += 15

        # 5. PAT growth 3yr CAGR (15 pts) — earnings must validate revenue
        if fa.pat_growth_3yr is not None:
            pat_score = _clamp(_linear_scale(fa.pat_growth_3yr, -10.0, 30.0, 0, 100), 0, 100)
            points.append((pat_score, 15))
            breakdown["pat_growth_3yr"] = round(pat_score, 1)
            available += 15

        # 6. Operating margin (10 pts)
        if fa.operating_margin is not None:
            om_score = _clamp(_linear_scale(fa.operating_margin, 5.0, 30.0, 0, 100), 0, 100)
            points.append((om_score, 10))
            breakdown["operating_margin"] = round(om_score, 1)
            available += 10

        # --- Penalty: promoter pledge > 20% reduces score ---
        pledge_penalty = 0.0
        if fa.promoter_pledge_pct is not None and fa.promoter_pledge_pct > 20.0:
            pledge_penalty = min((fa.promoter_pledge_pct - 20.0) * 0.5, 15.0)
            breakdown["pledge_penalty"] = round(-pledge_penalty, 1)

        if not points:
            return 0.0, breakdown, 0.0

        raw_score = sum(s * w for s, w in points) / sum(w for _, w in points)
        final_score = _clamp(raw_score - pledge_penalty, 0, 100)
        confidence = min(available / 100.0, 1.0)

        return round(final_score, 2), breakdown, confidence

    # ------------------------------------------------------------------
    # Technical Score (0–100)
    # Trend alignment — for long-term holds, 200-SMA matters most
    # ------------------------------------------------------------------

    def _score_technicals(
        self, ta: TechnicalData
    ) -> tuple[float, dict, float]:
        points = []
        breakdown = {}
        available = 0

        # 1. RSI (15 pts) — for long-term, mid-range RSI on pullback is ideal entry
        if ta.rsi_14 is not None:
            # Sweet spot 40–65 for long-term accumulation
            if 40 <= ta.rsi_14 <= 65:
                rsi_score = 80.0 + (ta.rsi_14 - 40) / 25 * 20
            elif ta.rsi_14 < 40:
                rsi_score = _linear_scale(ta.rsi_14, 20, 40, 40, 80)
            else:  # > 65, overbought — still positive but less ideal entry
                rsi_score = _linear_scale(ta.rsi_14, 65, 85, 80, 30)
            points.append((_clamp(rsi_score, 0, 100), 15))
            breakdown["rsi"] = round(_clamp(rsi_score, 0, 100), 1)
            available += 15

        # 2. MACD signal (15 pts)
        if ta.macd_signal is not None:
            macd_score = 75.0 if ta.macd_signal > 0 else 35.0
            if ta.macd_signal > 0:
                macd_score = _clamp(75.0 + ta.macd_signal * 5, 0, 100)
            else:
                macd_score = _clamp(35.0 + ta.macd_signal * 5, 0, 100)
            points.append((_clamp(macd_score, 0, 100), 15))
            breakdown["macd"] = round(_clamp(macd_score, 0, 100), 1)
            available += 15

        # 3. Price vs SMA200 (25 pts) — most important for long-term trend
        if ta.price_vs_sma200 is not None:
            # Above 200 SMA: bullish. 0–10% above = ideal (not over-extended)
            pct = ta.price_vs_sma200
            if pct >= 0:
                sma200_score = _clamp(_linear_scale(pct, 0, 20, 70, 100) if pct <= 20 else _linear_scale(pct, 20, 60, 100, 40), 0, 100)
            else:
                sma200_score = _clamp(_linear_scale(pct, -30, 0, 10, 70), 0, 100)
            points.append((sma200_score, 25))
            breakdown["price_vs_sma200"] = round(sma200_score, 1)
            available += 25

        # 4. Price vs SMA50 (20 pts)
        if ta.price_vs_sma50 is not None:
            pct = ta.price_vs_sma50
            sma50_score = _clamp(_linear_scale(pct, -20, 15, 10, 100), 0, 100)
            points.append((sma50_score, 20))
            breakdown["price_vs_sma50"] = round(sma50_score, 1)
            available += 20

        # 5. ADX — trend strength (15 pts)
        if ta.adx is not None:
            adx_score = _clamp(_linear_scale(ta.adx, 15, 40, 30, 100), 0, 100)
            points.append((adx_score, 15))
            breakdown["adx"] = round(adx_score, 1)
            available += 15

        # 6. Bollinger position (10 pts) — mid-band preferred for long-term entry
        if ta.bb_position is not None:
            bb_score = _clamp(_linear_scale(abs(ta.bb_position - 0.5), 0.5, 0, 50, 100), 0, 100)
            points.append((bb_score, 10))
            breakdown["bb_position"] = round(bb_score, 1)
            available += 10

        # 7. Institutional Footprint / Trade Activity (15 pts)
        if ta.trades_shock:
            # Reward high activity shock (institutional focus)
            if ta.trades_shock >= 1.5:
                trade_score = _clamp(_linear_scale(ta.trades_shock, 1.5, 4.0, 80, 100), 0, 100)
            elif ta.trades_shock >= 0.8:
                trade_score = _clamp(_linear_scale(ta.trades_shock, 0.8, 1.5, 50, 80), 0, 100)
            else:
                trade_score = _clamp(_linear_scale(ta.trades_shock, 0.3, 0.8, 20, 50), 0, 100)
            
            points.append((trade_score, 15))
            breakdown["trade_activity"] = round(trade_score, 1)
            available += 15

        if not points:
            return 0.0, breakdown, 0.0

        raw = sum(s * w for s, w in points) / sum(w for _, w in points)
        confidence = min(available / 100.0, 1.0)
        return round(_clamp(raw, 0, 100), 2), breakdown, confidence

    # ------------------------------------------------------------------
    # Momentum Score (0–100)
    # Uses your 2016-present PriceHistory depth
    # ------------------------------------------------------------------

    def _score_momentum(
        self, m: MomentumData
    ) -> tuple[float, dict, float]:
        points = []
        breakdown = {}
        available = 0

        # 1. 252-day (1yr) ROC — primary long-term momentum signal (30 pts)
        if m.roc_252 is not None:
            score = _clamp(_linear_scale(m.roc_252, -30, 60, 0, 100), 0, 100)
            points.append((score, 30))
            breakdown["roc_1yr"] = round(score, 1)
            available += 30

        # 2. 60-day ROC — medium-term trend confirmation (25 pts)
        if m.roc_60 is not None:
            score = _clamp(_linear_scale(m.roc_60, -20, 40, 0, 100), 0, 100)
            points.append((score, 25))
            breakdown["roc_60d"] = round(score, 1)
            available += 25

        # 3. Volume trend ratio: 20d avg / 90d avg (20 pts)
        # Ratio > 1 means rising participation — conviction signal
        if m.volume_ratio_20_90 is not None:
            score = _clamp(_linear_scale(m.volume_ratio_20_90, 0.6, 1.8, 0, 100), 0, 100)
            points.append((score, 20))
            breakdown["volume_trend"] = round(score, 1)
            available += 20

        # 4. 52-week range position (15 pts)
        # For long-term, we want stocks making new highs, not distressed
        if m.price_52w_rank is not None:
            # 0.6–1.0 = near highs = positive momentum
            score = _clamp(_linear_scale(m.price_52w_rank, 0.3, 1.0, 20, 100), 0, 100)
            points.append((score, 15))
            breakdown["52w_rank"] = round(score, 1)
            available += 15

        # 5. Relative strength vs Nifty 50 (10 pts)
        if m.relative_strength_nifty is not None:
            score = _clamp(_linear_scale(m.relative_strength_nifty, 0.5, 2.0, 0, 100), 0, 100)
            points.append((score, 10))
            breakdown["rs_vs_nifty"] = round(score, 1)
            available += 10

        if not points:
            return 0.0, breakdown, 0.0

        raw = sum(s * w for s, w in points) / sum(w for _, w in points)
        confidence = min(available / 105.0, 1.0)
        return round(_clamp(raw, 0, 100), 2), breakdown, confidence

    # ------------------------------------------------------------------
    # Sector Relative Score (0–100)
    # Percentile rank vs sector peers on combined FA + momentum
    # ------------------------------------------------------------------

    def _score_sector_relative(
        self, fa: FundamentalData, m: MomentumData, sector: SectorData
    ) -> tuple[float, float]:
        if not sector.sector:
            return 50.0, 50.0  # neutral if no sector data

        # Build a simple composite for ranking: ROE + growth - D/E + momentum
        own_vector = []
        peer_matrix = []

        # ROE dimension
        if fa.roe is not None and sector.sector_roe_list:
            own_vector.append(fa.roe)
            peer_matrix.append(sector.sector_roe_list)

        # Revenue growth dimension
        if fa.revenue_growth_3yr is not None and sector.sector_revenue_growth_list:
            own_vector.append(fa.revenue_growth_3yr)
            peer_matrix.append(sector.sector_revenue_growth_list)

        # Momentum (1yr ROC as proxy)
        if m.roc_252 is not None and sector.sector_momentum_list:
            own_vector.append(m.roc_252)
            peer_matrix.append(sector.sector_momentum_list)

        if not own_vector:
            return 50.0, 50.0

        # Z-score each dimension, then average
        z_scores = []
        for own_val, peer_list in zip(own_vector, peer_matrix):
            if len(peer_list) < 2:
                continue
            arr = np.array(peer_list, dtype=float)
            mu, sigma = arr.mean(), arr.std()
            if sigma < 1e-9:
                z_scores.append(0.5)
            else:
                z = (own_val - mu) / sigma
                # Convert Z to 0–1 using normal CDF approximation
                pct = _norm_cdf(z)
                z_scores.append(pct)

        if not z_scores:
            return 50.0, 50.0

        percentile = float(np.mean(z_scores)) * 100.0  # 0–100
        sector_score = _clamp(percentile, 0, 100)

        return round(sector_score, 2), round(percentile, 2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _linear_scale(
    value: float, in_min: float, in_max: float, out_min: float, out_max: float
) -> float:
    if in_max == in_min:
        return out_min
    ratio = (value - in_min) / (in_max - in_min)
    return out_min + ratio * (out_max - out_min)


def _norm_cdf(z: float) -> float:
    """Approximation of standard normal CDF using Abramowitz & Stegun."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


# ---------------------------------------------------------------------------
# Convenience: build result dict for SignalsCache update
# ---------------------------------------------------------------------------

def result_to_cache_dict(r: CompositeScoreResult) -> dict:
    """Convert result to a flat dict for SQLAlchemy bulk update."""
    return {
        "composite_score": r.composite_score,
        "fundamental_score": r.fundamental_score,
        "technical_score": r.technical_score,
        "momentum_score": r.momentum_score,
        "sector_rank_score": r.sector_rank_score,
        "sector_percentile": r.sector_percentile,
        "data_confidence": r.data_confidence,
        "promoter_pledge_warning": r.promoter_pledge_warning,
        "score_profile": r.profile,
        "fa_breakdown": r.fa_breakdown,
        "ta_breakdown": r.ta_breakdown,
        "momentum_breakdown": r.momentum_breakdown,
    }
