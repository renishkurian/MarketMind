# MarketMind Enhanced Engine
# ===========================
# Scoring Engine v2 + Backtesting + AI Skills
#
# Exports:
#   Scoring:    CompositeScorer, ScoreConfig, FundamentalData, TechnicalData, MomentumData, SectorData
#   Backtest:   BacktestEngine, BacktestMetrics, PriceBar, SignalEvent
#   Skills:     SkillLoader, SKILL_REGISTRY, StockMeta
#   Consensus:  ConsensusEngine, ConsensusResult, SkillVerdict

from backend.engine.scoring.composite_score import (
    CompositeScorer,
    ScoreConfig,
    FundamentalData,
    TechnicalData,
    MomentumData,
    SectorData,
    CompositeScoreResult,
    result_to_cache_dict,
    WEIGHT_PROFILES,
)

from backend.engine.backtest.backtest_engine import (
    BacktestEngine,
    BacktestMetrics,
    PriceBar,
    SignalEvent,
    Trade,
)

from backend.engine.skills.skill_loader import (
    SkillLoader,
    SKILL_REGISTRY,
    StockMeta,
)

from backend.engine.skills.consensus_engine import (
    ConsensusEngine,
    ConsensusResult,
    SkillAnalysis,
    SkillVerdict,
)

__all__ = [
    # Scoring
    "CompositeScorer", "ScoreConfig", "FundamentalData", "TechnicalData",
    "MomentumData", "SectorData", "CompositeScoreResult", "result_to_cache_dict",
    "WEIGHT_PROFILES",
    # Backtest
    "BacktestEngine", "BacktestMetrics", "PriceBar", "SignalEvent", "Trade",
    # Skills
    "SkillLoader", "SKILL_REGISTRY", "StockMeta",
    # Consensus
    "ConsensusEngine", "ConsensusResult", "SkillAnalysis", "SkillVerdict",
]
