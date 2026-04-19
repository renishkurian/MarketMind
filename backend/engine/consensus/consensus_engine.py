"""
MarketMind — Multi-Skill Consensus Engine
==========================================
Runs all relevant AI skills on a stock and produces a consensus
"bull vs bear vote" with confidence weighting.

For long-term compounding, the order is:
  1. SEBI Forensic  (gate: if AVOID → stop, do not proceed)
  2. Warren Buffett (quality + value)
  3. RJ India       (India macro + sector cycle)
  4. Sequoia Moat   (business quality + scalability)
  5. ARK Disruptive (innovation angle, if applicable)

Consensus output stored in AIInsights with all five narratives and
a synthesised final verdict for the portfolio dashboard.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class SkillVerdict(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    ACCUMULATE = "ACCUMULATE"
    HOLD = "HOLD"
    WATCH = "WATCH"
    AVOID = "AVOID"
    CRITICAL = "CRITICAL"
    NOT_APPLICABLE = "NOT_APPLICABLE"


# Numeric weights for consensus (higher = stronger vote)
VERDICT_WEIGHTS = {
    SkillVerdict.STRONG_BUY:      3,
    SkillVerdict.BUY:             2,
    SkillVerdict.ACCUMULATE:      1,
    SkillVerdict.HOLD:            0,
    SkillVerdict.WATCH:          -1,
    SkillVerdict.AVOID:          -3,
    SkillVerdict.CRITICAL:       -5,  # Forensic CRITICAL = veto
    SkillVerdict.NOT_APPLICABLE:  0,
}

# Per-skill weights in consensus (forensic has highest weight — it's a veto)
SKILL_CONSENSUS_WEIGHTS = {
    "sebi_forensic":         2.0,  # Veto power
    "warren_buffett_quality": 1.5,
    "rj_india_growth":        1.5,
    "sequoia_moat":           1.0,
    "ark_disruptive":         0.8,
    "goldman_screener":       1.0,
    "hindenburg_forensic":    1.5,
    "peter_lynch_simple":     0.8,
    "bain_strategy":          0.8,
    "mckinsey_macro":         0.8,
    "renaissance_patterns":   1.0,
}


@dataclass
class SkillAnalysis:
    skill_name: str
    display_name: str
    verdict: SkillVerdict
    narrative: str              # Full AI-generated text
    confidence: float = 1.0    # 0–1 (set to 0 if data confidence low)
    key_points: list[str] = field(default_factory=list)  # Extracted bullets


@dataclass
class ConsensusResult:
    symbol: str
    isin: str

    skill_analyses: list[SkillAnalysis] = field(default_factory=list)

    # Aggregate
    consensus_score: float = 0.0         # -10 to +10
    consensus_verdict: SkillVerdict = SkillVerdict.HOLD
    bull_count: int = 0
    bear_count: int = 0
    neutral_count: int = 0

    # Hard veto
    forensic_veto: bool = False
    forensic_severity: str = ""

    # Final synthesised summary (one paragraph for dashboard)
    executive_summary: str = ""

    def to_dashboard_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "consensus_verdict": self.consensus_verdict.value,
            "consensus_score": round(self.consensus_score, 2),
            "bull_count": self.bull_count,
            "bear_count": self.bear_count,
            "neutral_count": self.neutral_count,
            "forensic_veto": self.forensic_veto,
            "skill_verdicts": {
                a.skill_name: a.verdict.value
                for a in self.skill_analyses
            },
            "executive_summary": self.executive_summary,
        }


class ConsensusEngine:
    """
    Aggregates individual skill verdicts into a consensus signal.

    The `extract_verdict()` method uses regex to find the verdict keyword
    in the AI-generated narrative. This is intentionally simple — the skill
    templates are designed to output the verdict in a predictable location.
    """

    # Verdict keyword patterns (order matters — more specific first)
    VERDICT_PATTERNS = [
        (SkillVerdict.CRITICAL,       r"CRITICAL|INVESTIGATE"),
        (SkillVerdict.STRONG_BUY,     r"STRONG\s+BUY|HIGH\s+CONVICTION\s+BUY|HIGH\s+CONVICTION\s+INNOVATOR"),
        (SkillVerdict.BUY,            r"\bBUY\b(?!\s+ON\s+DIPS)"),
        (SkillVerdict.ACCUMULATE,     r"ACCUMULATE|BUY\s+ON\s+DIPS|EMERGING\s+DISRUPTOR"),
        (SkillVerdict.HOLD,           r"\bHOLD\b|TRACKER"),
        (SkillVerdict.WATCH,          r"\bWATCH\b|CAUTION"),
        (SkillVerdict.AVOID,          r"\bAVOID\b|NOT\s+APPLICABLE"),
        (SkillVerdict.NOT_APPLICABLE, r"NOT\s+APPLICABLE"),
    ]

    def extract_verdict(self, narrative: str) -> SkillVerdict:
        """Extract the verdict keyword from an AI narrative."""
        upper = narrative.upper()
        for verdict, pattern in self.VERDICT_PATTERNS:
            if re.search(pattern, upper):
                return verdict
        return SkillVerdict.HOLD  # Default if no clear verdict found

    def compute_consensus(
        self,
        symbol: str,
        isin: str,
        skill_analyses: list[SkillAnalysis],
    ) -> ConsensusResult:

        result = ConsensusResult(symbol=symbol, isin=isin, skill_analyses=skill_analyses)

        if not skill_analyses:
            return result

        # Check for forensic veto first
        for analysis in skill_analyses:
            if analysis.skill_name in ("sebi_forensic", "hindenburg_forensic"):
                if analysis.verdict in (SkillVerdict.CRITICAL, SkillVerdict.AVOID):
                    result.forensic_veto = True
                    result.forensic_severity = analysis.verdict.value
                    result.consensus_verdict = SkillVerdict.AVOID
                    result.executive_summary = (
                        f"FORENSIC VETO: {analysis.display_name} flagged {symbol} as "
                        f"{analysis.verdict.value}. No long position recommended until "
                        f"governance concerns are resolved. Full forensic analysis attached."
                    )
                    return result

        # Weighted consensus score
        total_weight = 0.0
        weighted_score = 0.0

        for analysis in skill_analyses:
            if analysis.verdict == SkillVerdict.NOT_APPLICABLE:
                continue

            skill_weight = SKILL_CONSENSUS_WEIGHTS.get(analysis.skill_name, 1.0)
            verdict_score = VERDICT_WEIGHTS.get(analysis.verdict, 0)
            confidence_adjusted = skill_weight * analysis.confidence

            weighted_score += verdict_score * confidence_adjusted
            total_weight += confidence_adjusted

            if verdict_score > 0:
                result.bull_count += 1
            elif verdict_score < 0:
                result.bear_count += 1
            else:
                result.neutral_count += 1

        if total_weight > 0:
            result.consensus_score = round(weighted_score / total_weight * 3.33, 2)  # scale to ~-10 to +10

        # Map score to verdict
        s = result.consensus_score
        if s >= 5:
            result.consensus_verdict = SkillVerdict.STRONG_BUY
        elif s >= 2.5:
            result.consensus_verdict = SkillVerdict.BUY
        elif s >= 1:
            result.consensus_verdict = SkillVerdict.ACCUMULATE
        elif s >= -1:
            result.consensus_verdict = SkillVerdict.HOLD
        elif s >= -3:
            result.consensus_verdict = SkillVerdict.WATCH
        else:
            result.consensus_verdict = SkillVerdict.AVOID

        result.executive_summary = self._build_summary(result)
        return result

    def _build_summary(self, r: ConsensusResult) -> str:
        total = r.bull_count + r.bear_count + r.neutral_count
        if total == 0:
            return "Insufficient data for consensus."

        skill_votes = ", ".join(
            f"{a.display_name}: {a.verdict.value}"
            for a in r.skill_analyses
            if a.verdict != SkillVerdict.NOT_APPLICABLE
        )

        return (
            f"Consensus: {r.consensus_verdict.value} "
            f"(score {r.consensus_score:+.1f}/10). "
            f"{r.bull_count} bullish, {r.neutral_count} neutral, {r.bear_count} bearish "
            f"across {total} skill analyses. "
            f"Votes: {skill_votes}."
        )
