"""
MarketMind — AI Skill Loader & Context Builder
================================================
Loads skill .md files and injects live scoring + backtest data
into the template before sending to the AI provider.

Usage:
    loader = SkillLoader(skills_dir="backend/engine/skills")
    prompt = loader.build_prompt(
        skill_name="warren_buffett_quality",
        score_result=composite_result,
        backtest_metrics=backtest_result,
        stock_meta=stock_meta_dict,
    )
    # Then send prompt to Claude / GPT / Grok via your existing AI client
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Any

from backend.engine.scoring.composite_score import CompositeScoreResult
from backend.engine.backtest.backtest_engine import BacktestMetrics


# ---------------------------------------------------------------------------
# Available Skills Registry
# ---------------------------------------------------------------------------

SKILL_REGISTRY = {
    "warren_buffett_quality": {
        "file": "warren_buffett_quality.md",
        "display_name": "Warren Buffett: Quality Compounder",
        "persona": "Investment — Value",
        "best_for": ["long_term_compounding"],
        "description": "Evaluates moat quality, management integrity, and margin of safety.",
    },
    "rj_india_growth": {
        "file": "rj_india_growth.md",
        "display_name": "RJ: Indian Market Intuition",
        "persona": "Investment — India Growth",
        "best_for": ["long_term_compounding"],
        "description": "India structural themes, sector cycle timing, promoter quality.",
    },
    "sebi_forensic": {
        "file": "sebi_forensic.md",
        "display_name": "SEBI Forensic: Compliance & Governance",
        "persona": "Forensic — Risk",
        "best_for": ["all"],
        "description": "Accounting red flags, promoter pledge risk, regulatory history.",
    },
    "sequoia_moat": {
        "file": "sequoia_moat.md",
        "display_name": "Sequoia: Scalable Moat",
        "persona": "Growth — Quality",
        "best_for": ["long_term_compounding"],
        "description": "Network effects, switching costs, unit economics, scalability.",
    },
    "ark_disruptive": {
        "file": "ark_disruptive.md",
        "display_name": "ARK: Disruptive Innovation",
        "persona": "Growth — Innovation",
        "best_for": ["long_term_compounding", "momentum_following"],
        "description": "S-curve positioning, TAM expansion, Wright's Law cost deflation.",
    },
    # Original skills preserved
    "goldman_screener": {
        "file": "goldman_screener.md",
        "display_name": "Goldman Sachs: Institutional Screener",
        "persona": "Investment Banking",
        "best_for": ["all"],
        "description": "Risk-adjusted returns, institutional-grade screening.",
    },
    "renaissance_patterns": {
        "file": "renaissance_patterns.md",
        "display_name": "Renaissance: Statistical Patterns",
        "persona": "Quant",
        "best_for": ["momentum_following", "swing_trading"],
        "description": "Statistical anomalies, pattern-based signals.",
    },
    "hindenburg_forensic": {
        "file": "hindenburg_forensic.md",
        "display_name": "Hindenburg: Forensic Short",
        "persona": "Forensic — Short",
        "best_for": ["all"],
        "description": "Accounting fraud, structural short thesis.",
    },
    "bain_strategy": {
        "file": "bain_strategy.md",
        "display_name": "Bain: Strategic Moat",
        "persona": "Consulting",
        "best_for": ["long_term_compounding"],
        "description": "Competitive positioning, strategic moat analysis.",
    },
    "mckinsey_macro": {
        "file": "mckinsey_macro.md",
        "display_name": "McKinsey: Macro & Sector",
        "persona": "Consulting",
        "best_for": ["all"],
        "description": "Sector tailwinds, macro environment analysis.",
    },
    "peter_lynch_simple": {
        "file": "peter_lynch_simple.md",
        "display_name": "Peter Lynch: Fundamental Simplicity",
        "persona": "Retail Growth",
        "best_for": ["long_term_compounding"],
        "description": "PEG ratio, simple business test, retail investor lens.",
    },
}


# ---------------------------------------------------------------------------
# Stock metadata container
# ---------------------------------------------------------------------------

@dataclass
class StockMeta:
    symbol: str
    isin: str
    exchange: str
    sector: str
    market_cap_cr: float
    current_price: float

    # Raw FA values (for template injection)
    pe_ratio: Optional[float] = None
    pe_5yr_avg: Optional[float] = None
    roe: Optional[float] = None
    roe_3yr_avg: Optional[float] = None
    debt_equity: Optional[float] = None
    revenue_growth_3yr: Optional[float] = None
    pat_growth_3yr: Optional[float] = None
    operating_margin: Optional[float] = None
    promoter_holding: Optional[float] = None
    promoter_pledge_pct: Optional[float] = None

    # Raw momentum values
    roc_252: Optional[float] = None
    roc_60: Optional[float] = None
    volume_ratio_20_90: Optional[float] = None

    def fmt(self, value: Optional[float], decimals: int = 1, suffix: str = "") -> str:
        if value is None:
            return "N/A"
        return f"{value:.{decimals}f}{suffix}"


# ---------------------------------------------------------------------------
# Skill Loader
# ---------------------------------------------------------------------------

class SkillLoader:

    def __init__(self, skills_dir: str = "backend/engine/skills"):
        self.skills_dir = Path(skills_dir)
        self._cache: dict[str, str] = {}

    def load_skill(self, skill_name: str) -> str:
        if skill_name in self._cache:
            return self._cache[skill_name]

        info = SKILL_REGISTRY.get(skill_name)
        if not info:
            raise ValueError(f"Unknown skill: {skill_name}. Available: {list(SKILL_REGISTRY.keys())}")

        skill_path = self.skills_dir / info["file"]
        if not skill_path.exists():
            raise FileNotFoundError(f"Skill file not found: {skill_path}")

        content = skill_path.read_text(encoding="utf-8")
        self._cache[skill_name] = content
        return content

    def build_prompt(
        self,
        skill_name: str,
        stock_meta: StockMeta,
        score_result: CompositeScoreResult,
        backtest_metrics: Optional[BacktestMetrics] = None,
    ) -> str:
        """
        Loads the skill template and substitutes all {{PLACEHOLDER}} tokens
        with live data from SignalsCache + BacktestMetrics.

        Returns the fully-rendered prompt string ready for the AI provider.
        """
        template = self.load_skill(skill_name)

        # Build substitution map
        subs = self._build_substitutions(stock_meta, score_result, backtest_metrics)

        # Replace all {{PLACEHOLDER}} tokens
        prompt = template
        for key, value in subs.items():
            prompt = prompt.replace(f"{{{{{key}}}}}", str(value))

        # Warn on any unfilled placeholders (log, don't raise)
        unfilled = re.findall(r"\{\{[A-Z_]+\}\}", prompt)
        if unfilled:
            unfilled_str = ", ".join(set(unfilled))
            prompt = prompt.replace(
                unfilled_str,
                f"[Data unavailable: {unfilled_str}]"
            )

        return prompt

    def build_multi_skill_prompt(
        self,
        skill_names: list[str],
        stock_meta: StockMeta,
        score_result: CompositeScoreResult,
        backtest_metrics: Optional[BacktestMetrics] = None,
    ) -> dict[str, str]:
        """Build prompts for multiple skills at once — for consensus mode."""
        return {
            name: self.build_prompt(name, stock_meta, score_result, backtest_metrics)
            for name in skill_names
        }

    # ------------------------------------------------------------------
    # Long-term compounding — recommended skill order
    # ------------------------------------------------------------------

    def recommended_skills_for_profile(self, profile: str) -> list[str]:
        """Return skills ordered by relevance for the given scoring profile."""
        order = {
            "long_term_compounding": [
                "sebi_forensic",        # Run forensic FIRST — no point analysing a fraud
                "warren_buffett_quality",
                "rj_india_growth",
                "sequoia_moat",
                "ark_disruptive",
            ],
            "swing_trading": [
                "goldman_screener",
                "renaissance_patterns",
                "sebi_forensic",
            ],
            "momentum_following": [
                "renaissance_patterns",
                "ark_disruptive",
                "goldman_screener",
            ],
        }
        return order.get(profile, list(SKILL_REGISTRY.keys()))

    # ------------------------------------------------------------------
    # Substitution map builder
    # ------------------------------------------------------------------

    def _build_substitutions(
        self,
        m: StockMeta,
        r: CompositeScoreResult,
        bt: Optional[BacktestMetrics],
    ) -> dict[str, Any]:

        f = m.fmt  # shorthand
        
        def safe_round(val: Optional[float], ndigits: int) -> str:
            return str(round(val, ndigits)) if val is not None else "N/A"

        subs: dict[str, Any] = {
            # Stock identity
            "COMPANY_NAME": getattr(m, "company_name", m.symbol),
            "SYMBOL": m.symbol,
            "ISIN": m.isin,
            "EXCHANGE": m.exchange,
            "SECTOR": m.sector,
            "CURRENT_PRICE": f(m.current_price, 2),
            "MARKET_CAP": f(m.market_cap_cr, 0),

            # Composite scores
            "COMPOSITE_SCORE": safe_round(r.composite_score, 1),
            "FUNDAMENTAL_SCORE": safe_round(r.fundamental_score, 1),
            "TECHNICAL_SCORE": safe_round(r.technical_score, 1),
            "MOMENTUM_SCORE": safe_round(r.momentum_score, 1),
            "SECTOR_RANK_SCORE": safe_round(r.sector_rank_score, 1),
            "SECTOR_PERCENTILE": safe_round(r.sector_percentile, 1),
            "DATA_CONFIDENCE": safe_round(r.data_confidence * 100 if r.data_confidence is not None else None, 0),

            # FA raw values
            "PE_RATIO": f(m.pe_ratio),
            "PE_5YR_AVG": f(m.pe_5yr_avg),
            "ROE": f(m.roe),
            "ROE_3YR_AVG": f(m.roe_3yr_avg),
            "DEBT_EQUITY": f(m.debt_equity, 2),
            "REVENUE_GROWTH_3YR": f(m.revenue_growth_3yr),
            "PAT_GROWTH_3YR": f(m.pat_growth_3yr),
            "OPERATING_MARGIN": f(m.operating_margin),
            "PROMOTER_HOLDING": f(m.promoter_holding),
            "PROMOTER_PLEDGE_PCT": f(m.promoter_pledge_pct),

            # FA breakdown scores (from scoring engine)
            "FA_PE_SCORE": safe_round(r.fa_breakdown.get("pe_vs_5yr"), 1),
            "FA_ROE_SCORE": safe_round(r.fa_breakdown.get("roe_quality"), 1),
            "FA_DE_SCORE": safe_round(r.fa_breakdown.get("debt_equity"), 1),
            "FA_REVENUE_SCORE": safe_round(r.fa_breakdown.get("revenue_growth_3yr"), 1),
            "FA_PAT_SCORE": safe_round(r.fa_breakdown.get("pat_growth_3yr"), 1),
            "FA_MARGIN_SCORE": safe_round(r.fa_breakdown.get("operating_margin"), 1),

            # Momentum raw values
            "ROC_252": f(m.roc_252),
            "ROC_60": f(m.roc_60),
            "VOLUME_RATIO": f(m.volume_ratio_20_90, 2),

            # Promoter flags
            "PROMOTER_HOLDING_FLAG": self._holding_flag(m.promoter_holding),
            "PLEDGE_FLAG": self._pledge_flag(m.promoter_pledge_pct),

            # Backtest context
            "BACKTEST_CONTEXT": bt.as_ai_context() if bt else "Backtest data not yet available for this stock.",

            # Placeholder target prices (AI fills these in its narrative)
            "BEAR_PRICE_PLACEHOLDER": "calculate",
            "BASE_PRICE_PLACEHOLDER": "calculate",
            "BULL_PRICE_PLACEHOLDER": "calculate",
        }

        return subs

    def _holding_flag(self, pct: Optional[float]) -> str:
        if pct is None:
            return "UNKNOWN"
        if pct >= 50:
            return "STRONG (>50%)"
        if pct >= 35:
            return "ADEQUATE (35–50%)"
        if pct >= 20:
            return "LOW (20–35%) — monitor"
        return "VERY LOW (<20%) — RED FLAG"

    def _pledge_flag(self, pct: Optional[float]) -> str:
        if pct is None:
            return "UNKNOWN"
        if pct == 0:
            return "CLEAN (0%)"
        if pct <= 10:
            return "WATCH (1–10%)"
        if pct <= 20:
            return f"CAUTION ({pct:.1f}%)"
        if pct <= 40:
            return f"RED FLAG ({pct:.1f}%) — lender dump risk"
        return f"CRITICAL ({pct:.1f}%) — potential cascade trigger"
