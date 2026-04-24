from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from backend.data.db import get_db, User
from backend.utils.auth import get_current_user
from .war_room_engine import WarRoomEngine

router = APIRouter(prefix="/api/war-room", tags=["War Room Intelligence"])

@router.get("/deep-research/{symbol}")
async def get_deep_research(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Triggers a Pro-Tier Deep Research on a symbol.
    """
    engine = WarRoomEngine(db)
    result = await engine.get_deep_research(symbol, user_id=current_user.id)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    return result
