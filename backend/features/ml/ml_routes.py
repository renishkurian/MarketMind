from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict

from backend.data.db import get_db, StockMaster, User
from backend.utils.auth import get_current_user
from sqlalchemy import select
from .alpha_engine import AlphaDiscoveryEngine

router = APIRouter(prefix="/api/ml", tags=["Machine Learning"])

@router.get("/alpha/{isin}")
async def get_alpha_signals(
    isin: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Runs the Alpha Discovery ML engine for a specific ISIN. (New Feature #ML)
    """
    # Verify stock exists
    stmt = select(StockMaster).where(StockMaster.isin == isin)
    res = await db.execute(stmt)
    stock = res.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    engine = AlphaDiscoveryEngine(db)
    result = await engine.get_alpha_prediction(stock.symbol, isin)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    return result

@router.get("/portfolio-alpha")
async def get_portfolio_alpha(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Runs ML predictions for all stocks in the user's portfolio.
    """
    stmt = select(StockMaster).where(StockMaster.user_id == current_user.id, StockMaster.is_active == True)
    res = await db.execute(stmt)
    portfolio = res.scalars().all()
    
    engine = AlphaDiscoveryEngine(db)
    results = []
    for stock in portfolio:
        try:
            pred = await engine.get_alpha_prediction(stock.symbol, stock.isin)
            if "error" not in pred:
                results.append(pred)
        except Exception:
            continue
            
    return sorted(results, key=lambda x: x['prediction_5d_return'], reverse=True)
