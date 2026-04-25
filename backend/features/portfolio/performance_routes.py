from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from backend.data.db import get_db, User
from backend.utils.auth import get_current_user
from backend.utils.limiter import limiter
from .performance_engine import PerformanceEngine

router = APIRouter(prefix="/api/portfolio-performance", tags=["Portfolio Performance"])

@router.get("/benchmark-comparison")
@limiter.limit("10/minute")
async def get_benchmark_comparison(
    request: Request,
    timeframe: str = Query("yearly", enum=["weekly", "monthly", "3month", "yearly"]),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Returns timeseries data comparing portfolio value vs Nifty 50.
    """
    engine = PerformanceEngine(db)
    return await engine.get_benchmark_comparison(current_user.id, timeframe)
