from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import asyncio

from backend.data.db import get_db, StockMaster, User
from backend.utils.auth import get_current_user
from .oracle_engine import OracleEngine

router = APIRouter(prefix="/api/oracle", tags=["Buffett Oracle AI"])

@router.get("/portfolio-conviction")
async def get_portfolio_conviction(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Runs the Oracle AI on the user's entire portfolio.
    """
    stmt = select(StockMaster).where(StockMaster.user_id == current_user.id, StockMaster.is_active == True)
    res = await db.execute(stmt)
    portfolio = res.scalars().all()
    
    if not portfolio:
        raise HTTPException(status_code=400, detail="Portfolio is empty")
        
    engine = OracleEngine(db)
    
    async def process_one(stock):
        try:
            return await engine.get_conviction_prediction(stock.symbol)
        except Exception:
            return None
            
    # Parallel analysis
    tasks = [process_one(s) for s in portfolio]
    raw = await asyncio.gather(*tasks)
    results = [r for r in raw if r and "error" not in r]
    
    return sorted(results, key=lambda x: x['conviction_score'], reverse=True)
