from fastapi import APIRouter, Depends, HTTPException, Request
from backend.utils.limiter import limiter
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import asyncio

from backend.data.db import get_db, StockMaster, User, AllocationLog
from backend.utils.auth import get_current_user
from .opt_engine import PortfolioOptEngine

router = APIRouter(prefix="/api/portfolio-opt", tags=["Portfolio Optimization"])

@router.get("/optimize")
@limiter.limit("3/minute")
async def get_optimal_portfolio(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Analyzes current user portfolio and suggests optimal weights.
    """
    stmt = select(StockMaster.symbol).where(StockMaster.user_id == current_user.id, StockMaster.is_active == True)
    res = await db.execute(stmt)
    symbols = [r for (r,) in res.all()]

    if not symbols:
        raise HTTPException(status_code=400, detail="Add stocks to your portfolio first")

    engine = PortfolioOptEngine(db)
    result = await engine.optimize_portfolio(symbols)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    log = AllocationLog(
        user_id=current_user.id,
        allocation_type="INSTITUTIONAL_OPT",
        total_amount=0, # Just a calculation log, not a live buy
        allocations=result["weights"],
        lookback_days=1095,
        expected_return=result["metrics"]["expected_annual_return"],
        expected_volatility=result["metrics"]["annual_volatility"],
        expected_sharpe=result["metrics"]["sharpe_ratio"]
    )
    db.add(log)
    await db.commit()
        
    return result
