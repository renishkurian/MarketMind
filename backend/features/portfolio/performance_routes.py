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
    benchmark: str = Query("^NSEI"),
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Returns timeseries data comparing portfolio value vs selected benchmark.
    """
    engine = PerformanceEngine(db)
    return await engine.get_benchmark_comparison(current_user.id, timeframe, benchmark, force_refresh=refresh)


@router.get("/yearly-breakdown")
@limiter.limit("10/minute")
async def get_yearly_breakdown(
    request: Request,
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    engine = PerformanceEngine(db)
    return await engine.get_yearly_breakdown(current_user.id, force_refresh=refresh)


@router.get("/sector-performance")
@limiter.limit("10/minute")  
async def get_sector_performance(
    request: Request,
    year: int = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    engine = PerformanceEngine(db)
    return await engine.get_sector_performance(current_user.id, year)

@router.get("/stock-performance-matrix")
@limiter.limit("5/minute")
async def get_stock_performance_matrix(
    request: Request,
    refresh: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    engine = PerformanceEngine(db)
    return await engine.get_stock_performance_matrix(current_user.id, force_refresh=refresh)
