"""
Alembic migration — MarketMind scoring engine v2.1
====================================================
Adds missing columns to SignalsCache and AIInsights.
Idempotent: each ALTER is wrapped in a column-exists check via
a helper so re-running on a partially-migrated database is safe.

Run:
    alembic upgrade head

Or directly (no alembic setup):
    python migrations/add_score_v2_1_columns.py
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.dialects import mysql


# revision identifiers
revision = "0002_score_v2_1"
down_revision = "0001_score_v2_0"   # adjust to your actual previous revision
branch_labels = None
depends_on = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _column_exists(conn, table: str, column: str) -> bool:
    result = conn.execute(
        text(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() "
            "AND table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    )
    return result.scalar() > 0


def _add_if_missing(conn, table: str, column: str, col_ddl: str) -> None:
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN {col_ddl}"))
        print(f"  + {table}.{column} added")
    else:
        print(f"  = {table}.{column} already exists, skipped")


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:
    conn = op.get_bind()

    print("\n[migration] signals_cache — adding v2.1 score audit columns")

    # -- Score audit trail (ENH-2) --
    _add_if_missing(conn, "signals_cache", "score_version",
        "`score_version` VARCHAR(10) NULL COMMENT 'Scoring engine version that produced this row'")

    _add_if_missing(conn, "signals_cache", "scored_at",
        "`scored_at` DATETIME NULL COMMENT 'UTC timestamp when score was computed'")

    # -- Per-component field coverage (FIX-1 support) --
    _add_if_missing(conn, "signals_cache", "fa_coverage",
        "`fa_coverage` DECIMAL(4,3) NULL COMMENT 'FA fields present / total FA fields (0–1)'")

    _add_if_missing(conn, "signals_cache", "ta_coverage",
        "`ta_coverage` DECIMAL(4,3) NULL COMMENT 'TA fields present / total TA fields (0–1)'")

    _add_if_missing(conn, "signals_cache", "momentum_coverage",
        "`momentum_coverage` DECIMAL(4,3) NULL COMMENT 'Momentum fields present / total (0–1)'")

    # -- Sector peer count (FIX-4 / ENH-1) --
    _add_if_missing(conn, "signals_cache", "sector_peer_count",
        "`sector_peer_count` SMALLINT UNSIGNED NULL COMMENT 'Number of sector peers used for relative rank'")

    # -- Backtest results (stored here for fast dashboard reads) --
    _add_if_missing(conn, "signals_cache", "backtest_cagr",
        "`backtest_cagr` DECIMAL(6,2) NULL COMMENT 'Annualised CAGR from signal backtest (2016–present)'")

    _add_if_missing(conn, "signals_cache", "backtest_win_rate",
        "`backtest_win_rate` DECIMAL(5,2) NULL COMMENT 'Win rate % from signal backtest'")

    _add_if_missing(conn, "signals_cache", "backtest_sharpe",
        "`backtest_sharpe` DECIMAL(5,2) NULL COMMENT 'Sharpe ratio from signal backtest'")

    _add_if_missing(conn, "signals_cache", "backtest_max_drawdown",
        "`backtest_max_drawdown` DECIMAL(6,2) NULL COMMENT 'Max drawdown % from signal backtest'")

    _add_if_missing(conn, "signals_cache", "backtest_trades",
        "`backtest_trades` SMALLINT UNSIGNED NULL COMMENT 'Total trades in backtest window'")

    print("\n[migration] ai_insights — adding consensus + backtest columns")

    # -- Multi-skill consensus fields --
    _add_if_missing(conn, "ai_insights", "consensus_score",
        "`consensus_score` DECIMAL(4,1) NULL COMMENT 'Weighted consensus score -10 to +10'")

    _add_if_missing(conn, "ai_insights", "bull_count",
        "`bull_count` TINYINT UNSIGNED NULL DEFAULT 0 COMMENT 'Number of bullish skill verdicts'")

    _add_if_missing(conn, "ai_insights", "bear_count",
        "`bear_count` TINYINT UNSIGNED NULL DEFAULT 0 COMMENT 'Number of bearish skill verdicts'")

    _add_if_missing(conn, "ai_insights", "neutral_count",
        "`neutral_count` TINYINT UNSIGNED NULL DEFAULT 0 COMMENT 'Number of neutral skill verdicts'")

    _add_if_missing(conn, "ai_insights", "forensic_veto",
        "`forensic_veto` BOOLEAN NULL DEFAULT FALSE COMMENT 'True if SEBI/Hindenburg forensic returned AVOID/CRITICAL'")

    _add_if_missing(conn, "ai_insights", "all_verdicts",
        "`all_verdicts` JSON NULL COMMENT 'Dict of skill_id → verdict for all skills run'")

    _add_if_missing(conn, "ai_insights", "composite_score_snapshot",
        "`composite_score_snapshot` DECIMAL(5,2) NULL COMMENT 'Composite score at time of AI generation'")

    _add_if_missing(conn, "ai_insights", "score_version_snapshot",
        "`score_version_snapshot` VARCHAR(10) NULL COMMENT 'Score engine version at time of generation'")

    print("\n[migration] Complete.\n")


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    conn = op.get_bind()

    signals_drop = [
        "score_version", "scored_at", "fa_coverage", "ta_coverage",
        "momentum_coverage", "sector_peer_count",
        "backtest_cagr", "backtest_win_rate", "backtest_sharpe",
        "backtest_max_drawdown", "backtest_trades",
    ]
    for col in signals_drop:
        if _column_exists(conn, "signals_cache", col):
            conn.execute(text(f"ALTER TABLE `signals_cache` DROP COLUMN `{col}`"))

    insights_drop = [
        "consensus_score", "bull_count", "bear_count", "neutral_count",
        "forensic_veto", "all_verdicts",
        "composite_score_snapshot", "score_version_snapshot",
    ]
    for col in insights_drop:
        if _column_exists(conn, "ai_insights", col):
            conn.execute(text(f"ALTER TABLE `ai_insights` DROP COLUMN `{col}`"))


# ---------------------------------------------------------------------------
# Standalone runner (no alembic required)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend.config import settings
    from sqlalchemy import create_engine

    # Use sync engine for migration
    sync_url = settings.async_database_url.replace(
        "mysql+aiomysql://", "mysql+pymysql://"
    )
    engine = create_engine(sync_url, echo=False)

    with engine.connect() as conn:
        # Patch op.get_bind to return our sync conn
        class _FakeOp:
            @staticmethod
            def get_bind():
                return conn

        import alembic.op as _op_module
        original_get_bind = getattr(_op_module, "get_bind", None)

        # Monkey-patch for standalone run
        _op_module.get_bind = lambda: conn  # type: ignore

        upgrade()
        conn.commit()

        if original_get_bind:
            _op_module.get_bind = original_get_bind

    print("Migration complete.")
