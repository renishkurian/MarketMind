"""
MarketMind — Composite Scoring Engine v2.1
==========================================
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

Changelog v2.0 → v2.1
-----------------------
  FIX-1  data_confidence was computing a nonsense weighted blend of confidence
         values using the score weights instead of measuring true data
         completeness. Replaced with a clean field-coverage-based metric:
         coverage = fields_present / total_fields per component.

  FIX-2  FA normalisation bug: raw_score = sum(s*w)/sum(w) was dividing by only
         the AVAILABLE weights, so a stock missing PE data would score lower
         than one with a mediocre PE. Correct behaviour: missing fields are
         treated as neutral (50) so the score reflects quality of available
         data, not absence of data. data_confidence tells the investor how
         complete the picture is.

  FIX-3  TA weight budget exceeded 100 when trades_shock was added
         (original 6 signals = 100 pts, +15 for shock = 115). Fixed:
         BB reduced 10→5 pts, trades_shock set to 5 pts. Total = 100.

  FIX-4  Sector rank confidence was hardcoded to 1.0 regardless of peer count.
         Now returns confidence = sqrt(peer_count / MIN_PEERS_FULL_CONF),
         capped at 1.0. Minimum MIN_SECTOR_PEERS peers required for any
         non-neutral score; below that returns 50.0 with confidence 0.3.

  FIX-5  Pledge penalty was subtracted from the final FA score after the
         weighted average. Correct: apply the penalty to the ROE quality
         sub-component before the weighted average runs, so it propagates
         correctly into the composite.

  FIX-6  roc_20 was declared in MomentumData but never scored. Added as
         10-pt short-term confirmation signal. roc_252 reduced 30→25 pts,
         price_52w_rank reduced 15→10 pts. Total = 100.

  ENH-1  Dynamic sector peer floor: MIN_SECTOR_PEERS = 5. Below this the
         sector score returns 50.0 (neutral) with confidence 0.3.

  ENH-2  CompositeScoreResult now carries score_version and scored_at for
         SignalsCache audit trail. score_version bumps on any logic change.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

SCORE_VERSION = "2.1"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_SECTOR_PEERS = 5               # Below this: sector rank returns neutral 50
MIN_SECTOR_PEERS_FULL_CONF = 15    # At this count sector confidence = 1.0

# ---------------------------------------------------------------------------
# Weight profiles
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

    def __post_init__(self) -> None:
        if self.profile in WEIGHT_PROFILES:
            w = WEIGHT_PROFILES[self.profile]
            self.fa_weight = w["fa"]
            self.ta_weight = w["ta"]
            self.momentum_weight = w["momentum"]
            self.sector_rank_weight = w["sector_rank"]
        total = (
            self.fa_weight + self.ta_weight
            + self.momentum_weight + self.sector_rank_weight
        )
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
    None = data unavailable; treated as neutral (50) in scoring — see FIX-2.
    """
    pe_ratio: Optional[float] = None
    pe_5yr_avg: Optional[float] = None
    roe: Optional[float] = None
    roe_3yr_avg: Optional[float] = None
    debt_equity: Optional[float] = None
    revenue_growth_3yr: Optional[float] = None
    revenue_growth: Optional[float] = None
    pat_growth_3yr: Optional[float] = None
    operating_margin: Optional[float] = None
    promoter_holding: Optional[float] = None
    promoter_pledge_pct: Optional[float] = None


@dataclass
class TechnicalData:
    """
    Sourced from SignalsCache.ta_* columns.
    TA weight budget = 100 pts (FIX-3):
      RSI 15 | MACD 15 | SMA200 25 | SMA50 20 | ADX 15 | BB 5 | TradesShock 5
    """
    rsi_14: Optional[float] = None
    macd_signal: Optional[float] = None
    price_vs_sma20: Optional[float] = None
    price_vs_sma50: Optional[float] = None
    price_vs_sma200: Optional[float] = None
    bb_position: Optional[float] = None
    adx: Optional[float] = None
    avg_trades_20: Optional[float] = None
    trades_shock: Optional[float] = None   # current trades / avg_trades_20


@dataclass
class MomentumData:
    """
    Derived from PriceHistory — computed fresh each EOD.
    Weight budget = 100 pts:
      roc_252 25 | roc_60 25 | roc_20 10 | vol_ratio 20 | 52w_rank 10 | rs_nifty 10
    roc_20 now scored (FIX-6).
    """
    roc_20: Optional[float] = None
    roc_60: Optional[float] = None
    roc_252: Optional[float] = None
    volume_ratio_20_90: Optional[float] = None
    price_52w_rank: Optional[float] = None
    relative_strength_nifty: Optional[float] = None


@dataclass
class SectorData:
    """
    Peer group data. Must have >= MIN_SECTOR_PEERS entries for non-neutral scoring.
    """
    sector: str = ""
    sector_pe_list: list[float] = field(default_factory=list)
    sector_roe_list: list[float] = field(default_factory=list)
    sector_revenue_growth_list: list[float] = field(default_factory=list)
    sector_momentum_list: list[float] = field(default_factory=list)


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
    sector_rank_score: float = 0.0

    # Final blended score
    composite_score: float = 0.0

    # Data completeness (0–1): fields_present / total_fields per component (FIX-1)
    data_confidence: float = 0.0

    # Per-component field coverage for dashboard transparency
    fa_coverage: float = 0.0
    ta_coverage: float = 0.0
    momentum_coverage: float = 0.0
    sector_peer_count: int = 0

    # Sub-signals for AI skill context
    fa_breakdown: dict = field(default_factory=dict)
    ta_breakdown: dict = field(default_factory=dict)
    momentum_breakdown: dict = field(default_factory=dict)

    # Flags
    promoter_pledge_warning: bool = False
    sector_percentile: float = 0.0

    # Audit trail (ENH-2)
    score_version: str = SCORE_VERSION
    scored_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    profile: str = "long_term_compounding"


# ---------------------------------------------------------------------------
# Scoring Engine
# ---------------------------------------------------------------------------

class CompositeScorer:
    """
    Instantiate once per process; call score() per stock.

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

    def __init__(self, config: ScoreConfig | None = None) -> None:
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

        result = CompositeScoreResult(
            symbol=symbol, isin=isin, profile=self.config.profile
        )
        w = self.config

        fa_score,  fa_bd,  fa_cov  = self._score_fundamentals(fa)
        ta_score,  ta_bd,  ta_cov  = self._score_technicals(ta)
        mom_score, mom_bd, mom_cov = self._score_momentum(momentum)
        sec_score, sec_pct, sec_peers, sec_conf = self._score_sector_relative(
            fa, momentum, sector
        )

        result.fundamental_score  = fa_score
        result.technical_score    = ta_score
        result.momentum_score     = mom_score
        result.sector_rank_score  = sec_score
        result.sector_percentile  = sec_pct
        result.sector_peer_count  = sec_peers

        result.fa_breakdown       = fa_bd
        result.ta_breakdown       = ta_bd
        result.momentum_breakdown = mom_bd

        result.fa_coverage        = fa_cov
        result.ta_coverage        = ta_cov
        result.momentum_coverage  = mom_cov

        # Composite score
        result.composite_score = round(
            fa_score  * w.fa_weight
            + ta_score  * w.ta_weight
            + mom_score * w.momentum_weight
            + sec_score * w.sector_rank_weight,
            2,
        )

        # FIX-1: true data completeness — weighted avg of per-component coverage
        result.data_confidence = round(
            fa_cov  * w.fa_weight
            + ta_cov  * w.ta_weight
            + mom_cov * w.momentum_weight
            + sec_conf * w.sector_rank_weight,
            3,
        )

        if fa.promoter_pledge_pct is not None and fa.promoter_pledge_pct > 20.0:
            result.promoter_pledge_warning = True

        return result

    # ------------------------------------------------------------------
    # Fundamental Score (0–100)
    # Budget: PE 20 | ROE 25 | D/E 15 | RevGrowth 15 | PAT 15 | OpMgn 10
    # ------------------------------------------------------------------

    def _score_fundamentals(
        self, fa: FundamentalData
    ) -> tuple[float, dict, float]:
        """
        FIX-2: Missing fields → neutral 50 (never excluded from denominator).
        FIX-5: Pledge penalty applied inside ROE component before blending.
        Returns (score 0–100, breakdown dict, coverage 0–1).
        """

        # 1. PE vs 5yr avg (20 pts)
        pe_present = fa.pe_ratio is not None
        if pe_present:
            # Fallback benchmark if 5yr avg is missing
            ref_pe = fa.pe_5yr_avg if (fa.pe_5yr_avg and fa.pe_5yr_avg > 0) else 20.0
            discount = (ref_pe - fa.pe_ratio) / ref_pe
            pe_score = _clamp(_linear_scale(discount, -0.5, 0.5, 0, 100), 0, 100)
        else:
            pe_score = 50.0

        # 2. ROE quality (25 pts) + pledge penalty here (FIX-5)
        roe_present = fa.roe is not None
        if roe_present:
            roe_raw = _clamp(_linear_scale(fa.roe, 5.0, 30.0, 0, 100), 0, 100)
            if fa.roe_3yr_avg is not None and fa.roe_3yr_avg > 0:
                consistency = 1.0 - abs(fa.roe - fa.roe_3yr_avg) / fa.roe_3yr_avg
                bonus = _clamp(consistency * 15, 0, 15)
            else:
                bonus = 0.0
            roe_score = _clamp(roe_raw * 0.85 + bonus, 0, 100)
            # FIX-5: penalty lives here, not after the final average
            if fa.promoter_pledge_pct is not None and fa.promoter_pledge_pct > 20.0:
                penalty = min((fa.promoter_pledge_pct - 20.0) * 0.5, 20.0)
                roe_score = _clamp(roe_score - penalty, 0, 100)
        else:
            roe_score = 50.0

        # 3. Debt/Equity (15 pts)
        de_present = fa.debt_equity is not None
        de_score = (
            _clamp(_linear_scale(fa.debt_equity, 2.0, 0.0, 0, 100), 0, 100)
            if de_present else 50.0
        )

        # 4. Revenue CAGR 3yr (15 pts) - Fallback to current YoY
        rev_val = fa.revenue_growth_3yr if fa.revenue_growth_3yr is not None else fa.revenue_growth
        rev_present = rev_val is not None
        rev_score = (
            _clamp(_linear_scale(rev_val, -5.0, 25.0, 0, 100), 0, 100)
            if rev_present else 50.0
        )

        # 5. PAT CAGR 3yr (15 pts)
        pat_present = fa.pat_growth_3yr is not None
        pat_score = (
            _clamp(_linear_scale(fa.pat_growth_3yr, -10.0, 30.0, 0, 100), 0, 100)
            if pat_present else 50.0
        )

        # 6. Operating margin (10 pts)
        om_present = fa.operating_margin is not None
        om_score = (
            _clamp(_linear_scale(fa.operating_margin, 5.0, 30.0, 0, 100), 0, 100)
            if om_present else 50.0
        )

        # FIX-2: denominator always 100 — missing fields contribute neutral 50
        raw = (
            pe_score  * 20
            + roe_score * 25
            + de_score  * 15
            + rev_score * 15
            + pat_score * 15
            + om_score  * 10
        ) / 100.0

        breakdown: dict = {
            "pe_vs_5yr":            round(pe_score, 1)  if pe_present  else None,
            "roe_quality":          round(roe_score, 1) if roe_present  else None,
            "debt_equity":          round(de_score, 1)  if de_present  else None,
            "revenue_growth_3yr":   round(rev_score, 1) if rev_present  else None,
            "pat_growth_3yr":       round(pat_score, 1) if pat_present  else None,
            "operating_margin":     round(om_score, 1)  if om_present  else None,
        }
        if fa.promoter_pledge_pct is not None and fa.promoter_pledge_pct > 20.0:
            penalty = min((fa.promoter_pledge_pct - 20.0) * 0.5, 20.0)
            breakdown["pledge_penalty_on_roe"] = round(-penalty, 1)

        n_present = sum([pe_present, roe_present, de_present,
                         rev_present, pat_present, om_present])
        coverage = round(n_present / 6.0, 3)

        return round(_clamp(raw, 0, 100), 2), breakdown, coverage

    # ------------------------------------------------------------------
    # Technical Score (0–100)
    # FIX-3 weight budget: RSI 15|MACD 15|SMA200 25|SMA50 20|ADX 15|BB 5|Shock 5
    # ------------------------------------------------------------------

    def _score_technicals(
        self, ta: TechnicalData
    ) -> tuple[float, dict, float]:
        """FIX-3: total pts = 100 exactly. Missing → neutral 50."""

        # 1. RSI (15 pts)
        rsi_present = ta.rsi_14 is not None
        if rsi_present:
            v = ta.rsi_14
            if 40 <= v <= 65:
                rsi_score = 80.0 + (v - 40) / 25 * 20
            elif v < 40:
                rsi_score = _linear_scale(v, 20, 40, 40, 80)
            else:
                rsi_score = _linear_scale(v, 65, 85, 80, 30)
            rsi_score = _clamp(rsi_score, 0, 100)
        else:
            rsi_score = 50.0

        # 2. MACD signal (15 pts)
        macd_present = ta.macd_signal is not None
        if macd_present:
            base = 75.0 if ta.macd_signal >= 0 else 35.0
            macd_score = _clamp(base + ta.macd_signal * 5, 0, 100)
        else:
            macd_score = 50.0

        # 3. Price vs SMA200 (25 pts)
        sma200_present = ta.price_vs_sma200 is not None
        if sma200_present:
            pct = ta.price_vs_sma200
            if pct >= 0:
                s = (
                    _linear_scale(pct, 0, 20, 70, 100) if pct <= 20
                    else _linear_scale(pct, 20, 60, 100, 40)
                )
            else:
                s = _linear_scale(pct, -30, 0, 10, 70)
            sma200_score = _clamp(s, 0, 100)
        else:
            sma200_score = 50.0

        # 4. Price vs SMA50 (20 pts)
        sma50_present = ta.price_vs_sma50 is not None
        sma50_score = (
            _clamp(_linear_scale(ta.price_vs_sma50, -20, 15, 10, 100), 0, 100)
            if sma50_present else 50.0
        )

        # 5. ADX (15 pts)
        adx_present = ta.adx is not None
        adx_score = (
            _clamp(_linear_scale(ta.adx, 15, 40, 30, 100), 0, 100)
            if adx_present else 50.0
        )

        # 6. Bollinger position (5 pts — reduced from 10, FIX-3)
        bb_present = ta.bb_position is not None
        bb_score = (
            _clamp(_linear_scale(abs(ta.bb_position - 0.5), 0.5, 0, 50, 100), 0, 100)
            if bb_present else 50.0
        )

        # 7. Trades shock (5 pts — reduced from 15, FIX-3)
        shock_present = ta.trades_shock is not None
        if shock_present:
            v = ta.trades_shock
            if v >= 1.5:
                shock_score = _clamp(_linear_scale(v, 1.5, 4.0, 80, 100), 0, 100)
            elif v >= 0.8:
                shock_score = _clamp(_linear_scale(v, 0.8, 1.5, 50, 80), 0, 100)
            else:
                shock_score = _clamp(_linear_scale(v, 0.3, 0.8, 20, 50), 0, 100)
        else:
            shock_score = 50.0

        # FIX-3: denominator exactly 100
        raw = (
            rsi_score    * 15
            + macd_score   * 15
            + sma200_score * 25
            + sma50_score  * 20
            + adx_score    * 15
            + bb_score     *  5
            + shock_score  *  5
        ) / 100.0

        breakdown: dict = {
            "rsi":              round(rsi_score, 1)    if rsi_present    else None,
            "macd":             round(macd_score, 1)   if macd_present   else None,
            "price_vs_sma200":  round(sma200_score, 1) if sma200_present else None,
            "price_vs_sma50":   round(sma50_score, 1)  if sma50_present  else None,
            "adx":              round(adx_score, 1)    if adx_present    else None,
            "bb_position":      round(bb_score, 1)     if bb_present     else None,
            "trade_activity":   round(shock_score, 1)  if shock_present  else None,
        }

        n_present = sum([rsi_present, macd_present, sma200_present, sma50_present,
                         adx_present, bb_present, shock_present])
        coverage = round(n_present / 7.0, 3)

        return round(_clamp(raw, 0, 100), 2), breakdown, coverage

    # ------------------------------------------------------------------
    # Momentum Score (0–100)
    # FIX-6: roc_20 now included (10 pts)
    # Budget: roc_252 25|roc_60 25|roc_20 10|vol 20|52w 10|rs_nifty 10
    # ------------------------------------------------------------------

    def _score_momentum(
        self, m: MomentumData
    ) -> tuple[float, dict, float]:
        """FIX-6: roc_20 scored. Total = 100 pts. Missing → neutral 50."""

        # 1. 1yr ROC (25 pts)
        roc252_present = m.roc_252 is not None
        roc252_score = (
            _clamp(_linear_scale(m.roc_252, -30, 60, 0, 100), 0, 100)
            if roc252_present else 50.0
        )

        # 2. 60d ROC (25 pts)
        roc60_present = m.roc_60 is not None
        roc60_score = (
            _clamp(_linear_scale(m.roc_60, -20, 40, 0, 100), 0, 100)
            if roc60_present else 50.0
        )

        # 3. 20d ROC — short-term confirmation (10 pts) — FIX-6
        roc20_present = m.roc_20 is not None
        roc20_score = (
            _clamp(_linear_scale(m.roc_20, -10, 20, 0, 100), 0, 100)
            if roc20_present else 50.0
        )

        # 4. Volume trend ratio (20 pts)
        vol_present = m.volume_ratio_20_90 is not None
        vol_score = (
            _clamp(_linear_scale(m.volume_ratio_20_90, 0.6, 1.8, 0, 100), 0, 100)
            if vol_present else 50.0
        )

        # 5. 52-week rank (10 pts)
        rank_present = m.price_52w_rank is not None
        rank_score = (
            _clamp(_linear_scale(m.price_52w_rank, 0.3, 1.0, 20, 100), 0, 100)
            if rank_present else 50.0
        )

        # 6. Relative strength vs Nifty (10 pts)
        rs_present = m.relative_strength_nifty is not None
        rs_score = (
            _clamp(_linear_scale(m.relative_strength_nifty, 0.5, 2.0, 0, 100), 0, 100)
            if rs_present else 50.0
        )

        raw = (
            roc252_score * 25
            + roc60_score  * 25
            + roc20_score  * 10
            + vol_score    * 20
            + rank_score   * 10
            + rs_score     * 10
        ) / 100.0

        breakdown: dict = {
            "roc_1yr":          round(roc252_score, 1) if roc252_present else None,
            "roc_60d":          round(roc60_score, 1)  if roc60_present  else None,
            "roc_20d":          round(roc20_score, 1)  if roc20_present  else None,
            "volume_trend":     round(vol_score, 1)    if vol_present    else None,
            "52w_rank":         round(rank_score, 1)   if rank_present   else None,
            "rs_vs_nifty":      round(rs_score, 1)     if rs_present     else None,
        }

        n_present = sum([roc252_present, roc60_present, roc20_present,
                         vol_present, rank_present, rs_present])
        coverage = round(n_present / 6.0, 3)

        return round(_clamp(raw, 0, 100), 2), breakdown, coverage

    # ------------------------------------------------------------------
    # Sector Relative Score (0–100)
    # FIX-4: confidence scales with peer count
    # ENH-1: minimum peer floor = MIN_SECTOR_PEERS
    # Returns (score, percentile, peer_count, confidence)
    # ------------------------------------------------------------------

    def _score_sector_relative(
        self,
        fa: FundamentalData,
        m: MomentumData,
        sector: SectorData,
    ) -> tuple[float, float, int, float]:

        if not sector.sector:
            return 50.0, 50.0, 0, 0.0

        own_vector: list[float] = []
        peer_matrix: list[list[float]] = []

        if fa.roe is not None and len(sector.sector_roe_list) >= MIN_SECTOR_PEERS:
            own_vector.append(fa.roe)
            peer_matrix.append(sector.sector_roe_list)

        if (fa.revenue_growth_3yr is not None
                and len(sector.sector_revenue_growth_list) >= MIN_SECTOR_PEERS):
            own_vector.append(fa.revenue_growth_3yr)
            peer_matrix.append(sector.sector_revenue_growth_list)

        if m.roc_252 is not None and len(sector.sector_momentum_list) >= MIN_SECTOR_PEERS:
            own_vector.append(m.roc_252)
            peer_matrix.append(sector.sector_momentum_list)

        peer_count = max((len(lst) for lst in peer_matrix), default=0)

        # ENH-1: require minimum peer count for non-neutral score
        if not own_vector or peer_count < MIN_SECTOR_PEERS:
            return 50.0, 50.0, peer_count, 0.3

        z_scores: list[float] = []
        for own_val, peers in zip(own_vector, peer_matrix):
            if len(peers) < 2:
                continue
            arr = np.array(peers, dtype=float)
            mu, sigma = arr.mean(), arr.std()
            if sigma < 1e-9:
                z_scores.append(0.5)
            else:
                z_scores.append(_norm_cdf((own_val - mu) / sigma))

        if not z_scores:
            return 50.0, 50.0, peer_count, 0.3

        percentile = float(np.mean(z_scores)) * 100.0
        sector_score = _clamp(percentile, 0, 100)

        # FIX-4: confidence scales with peer count
        confidence = min(
            math.sqrt(peer_count / MIN_SECTOR_PEERS_FULL_CONF),
            1.0,
        )

        return round(sector_score, 2), round(percentile, 2), peer_count, round(confidence, 3)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _linear_scale(
    value: float,
    in_min: float,
    in_max: float,
    out_min: float,
    out_max: float,
) -> float:
    if in_max == in_min:
        return out_min
    ratio = (value - in_min) / (in_max - in_min)
    return out_min + ratio * (out_max - out_min)


def _norm_cdf(z: float) -> float:
    """Standard normal CDF — Abramowitz & Stegun approximation."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


# ---------------------------------------------------------------------------
# Convenience: flat dict for SQLAlchemy SignalsCache bulk update
# ---------------------------------------------------------------------------

def result_to_cache_dict(r: CompositeScoreResult) -> dict:
    return {
        "composite_score":         r.composite_score,
        "fundamental_score":       r.fundamental_score,
        "technical_score":         r.technical_score,
        "momentum_score":          r.momentum_score,
        "sector_rank_score":       r.sector_rank_score,
        "sector_percentile":       r.sector_percentile,
        "sector_peer_count":       r.sector_peer_count,
        "data_confidence":         r.data_confidence,
        "fa_coverage":             r.fa_coverage,
        "ta_coverage":             r.ta_coverage,
        "momentum_coverage":       r.momentum_coverage,
        "promoter_pledge_warning": r.promoter_pledge_warning,
        "score_version":           r.score_version,
        "scored_at":               r.scored_at,
        "score_profile":           r.profile,
        "fa_breakdown":            r.fa_breakdown,
        "ta_breakdown":            r.ta_breakdown,
        "momentum_breakdown":      r.momentum_breakdown,
    }
