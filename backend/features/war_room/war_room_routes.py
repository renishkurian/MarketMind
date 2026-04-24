from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from backend.utils.auth import get_current_user
from sqlalchemy import select
from backend.data.db import get_db, User, StockMaster
from .war_room_engine import WarRoomEngine

from backend.utils.limiter import limiter

from fastapi import APIRouter, Depends, HTTPException, Request

router = APIRouter(prefix="/api/war-room", tags=["War Room Intelligence"])

@router.get("/deep-research/{symbol}")
@limiter.limit("5/minute")
async def get_deep_research(
    symbol: str, 
    request: Request,
    rebuild: bool = False,
    current_user: User = Depends(get_current_user), 
    db: AsyncSession = Depends(get_db)
):
    """
    Triggers a Pro-Tier Deep Research on a symbol.
    If rebuild=False (default), it looks for a fresh snapshot first.
    """
    engine = WarRoomEngine(db)
    
    # Ownership Check: Only research stocks in user's portfolio
    ownership = await db.execute(
        select(StockMaster).where(
            StockMaster.symbol == symbol.upper(),
            StockMaster.user_id == current_user.id
        )
    )
    if not ownership.scalars().first():
        raise HTTPException(status_code=403, detail="Stock not in your portfolio")

    if not rebuild:
        snapshot = await engine.get_latest_snapshot(symbol, current_user.id)
        if snapshot:
            return snapshot

    result = await engine.get_deep_research(symbol, user_id=current_user.id)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    return result

@router.get("/latest/{symbol}")
async def get_latest(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    engine = WarRoomEngine(db)
    snapshot = await engine.get_latest_snapshot(symbol, current_user.id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return snapshot
