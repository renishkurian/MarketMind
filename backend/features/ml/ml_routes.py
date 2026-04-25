import asyncio
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict

from backend.data.db import get_db, StockMaster, User, MLSnapshot
from backend.utils.auth import get_current_user
from sqlalchemy import select, desc
from backend.features.ml.alpha_engine import AlphaDiscoveryEngine

from backend.utils.limiter import limiter

from fastapi import APIRouter, Depends, HTTPException, Request

router = APIRouter(prefix="/api/ml", tags=["Machine Learning"])

@router.get("/alpha/{isin}")
@limiter.limit("5/minute")
async def get_alpha_signals(
    isin: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Runs the Alpha Discovery ML engine for a specific ISIN. (New Feature #ML)
    """
    # Verify stock exists and belongs to the user
    stmt = (
        select(StockMaster)
        .where(StockMaster.isin == isin, StockMaster.user_id == current_user.id)
    )
    res = await db.execute(stmt)
    stock = res.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    engine = AlphaDiscoveryEngine(db)
    result = await engine.get_alpha_prediction(stock.symbol)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    return result

@router.get("/portfolio-alpha")
@limiter.limit("3/minute")
async def get_portfolio_alpha(
    request: Request,
    save: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Runs ML predictions for all stocks in the user's portfolio in parallel and saves a snapshot.
    """
    stmt = select(StockMaster).where(StockMaster.user_id == current_user.id, StockMaster.is_active == True)
    res = await db.execute(stmt)
    portfolio = res.scalars().all()
    
    engine = AlphaDiscoveryEngine(db)
    sem = asyncio.Semaphore(10)
    
    async def safe_predict(stock):
        async with sem:
            try:
                pred = await engine.get_alpha_prediction(stock.symbol)
                if "error" not in pred:
                    return pred
            except Exception:
                pass
            return None

    tasks = [safe_predict(stock) for stock in portfolio]
    raw_results = await asyncio.gather(*tasks)
    results = [r for r in raw_results if r is not None]
    
    # Sort results by confidence by default
    results = sorted(results, key=lambda x: x['confidence_score'], reverse=True)

    if save and results:
        # Calculate summary with better precision
        total_ret = sum(float(r.get('prediction_5d_return', 0)) for r in results)
        avg_ret = total_ret / len(results)
        
        # Logging to debug zero average
        #logger.debug(f"ML Summary: Total={total_ret}, Count={len(results)}, Avg={avg_ret}")
        
        summary = {
            "avg_projected_return": float(avg_ret),
            "stock_count": len(results),
            "high_confidence_count": len([r for r in results if r.get('confidence_score', 0) > 0.7])
        }
        
        snapshot = MLSnapshot(
            user_id=current_user.id,
            data=results,
            summary=summary
        )
        db.add(snapshot)
        await db.commit()
        await db.refresh(snapshot)
        
        return {
            "snapshot_id": snapshot.id,
            "created_at": snapshot.created_at.isoformat(),
            "summary": summary,
            "data": results
        }
            
    return {"data": results}

@router.get("/history")
async def get_ml_history(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Returns a list of past ML snapshots for the user."""
    stmt = select(MLSnapshot).where(MLSnapshot.user_id == current_user.id).order_by(desc(MLSnapshot.created_at)).limit(20)
    res = await db.execute(stmt)
    snapshots = res.scalars().all()
    
    return [
        {
            "id": s.id,
            "created_at": s.created_at.isoformat(),
            "summary": s.summary
        }
        for s in snapshots
    ]

@router.get("/snapshot/{snapshot_id}")
async def get_ml_snapshot(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Returns a specific ML snapshot."""
    stmt = select(MLSnapshot).where(MLSnapshot.id == snapshot_id, MLSnapshot.user_id == current_user.id)
    res = await db.execute(stmt)
    snapshot = res.scalars().first()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
        
    return {
        "id": snapshot.id,
        "created_at": snapshot.created_at.isoformat(),
        "summary": snapshot.summary,
        "data": snapshot.data
    }
