import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
from sqlalchemy import select, and_
from backend.data.db import PriceHistory, StockMaster
import logging
import asyncio

logger = logging.getLogger(__name__)

class PerformanceEngine:
    def __init__(self, db):
        self.db = db

    async def get_benchmark_comparison(self, user_id: int, timeframe: str = "yearly"):
        """
        Calculates the historical portfolio value vs Nifty 50 benchmark.
        Normalized to 100 at the start of the period.
        """
        # 1. Define date range
        end_dt = date.today()
        if timeframe == "weekly":
            start_dt = end_dt - timedelta(days=7)
        elif timeframe == "monthly":
            start_dt = end_dt - timedelta(days=30)
        elif timeframe == "3month":
            start_dt = end_dt - timedelta(days=90)
        else: # yearly
            start_dt = end_dt - timedelta(days=365)

        # 2. Get Portfolio Stocks
        # We need quantity and buy_date to reconstruct historical holding
        stmt = select(StockMaster.symbol, StockMaster.quantity, StockMaster.buy_date, StockMaster.added_date).where(
            and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        )
        res = await self.db.execute(stmt)
        stocks = res.all()
        
        if not stocks:
            return {"error": "Portfolio is empty"}

        symbols = [s.symbol for s in stocks]
        
        # 3. Fetch Historical Prices for Portfolio
        # Fetch from DB for the range
        p_stmt = select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close).where(
            and_(PriceHistory.symbol.in_(symbols), PriceHistory.date >= start_dt)
        )
        p_res = await self.db.execute(p_stmt)
        p_history = p_res.all()
        
        if not p_history:
             return {"error": "No historical price data found in DB for portfolio stocks"}

        df_p = pd.DataFrame(p_history, columns=['symbol', 'date', 'close'])
        df_p['date'] = pd.to_datetime(df_p['date'])
        
        # Pivot to have dates as index and symbols as columns
        df_pivot = df_p.pivot(index='date', columns='symbol', values='close')
        df_pivot = df_pivot.sort_index()
        # Fill gaps (holidays etc)
        df_pivot = df_pivot.ffill().bfill()

        # 4. Calculate Portfolio Performance (NAV Model)
        # We calculate the value of the CURRENT basket of stocks historically.
        # This shows the performance of your selection regardless of when you added cash.
        portfolio_series = pd.Series(0.0, index=df_pivot.index)
        for s in stocks:
            if s.symbol in df_pivot.columns:
                qty = float(s.quantity or 0)
                portfolio_series += df_pivot[s.symbol].astype(float) * qty

        # 5. Fetch Nifty 50 Benchmark (^NSEI)
        nifty_symbol = "^NSEI"
        try:
            # Fetch from yfinance. We use start_dt - 7 days to ensure we have a price for the very first alignment day
            yf_data = await asyncio.to_thread(
                yf.download, 
                nifty_symbol, 
                start=(start_dt - timedelta(days=7)).strftime('%Y-%m-%d'), 
                end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'), 
                progress=False
            )
            
            if yf_data.empty:
                return {"error": "Benchmark data unavailable"}
                
            nifty_series = yf_data['Close']
            if isinstance(nifty_series, pd.DataFrame):
                nifty_series = nifty_series.iloc[:, 0]
                
            nifty_series.index = pd.to_datetime(nifty_series.index).tz_localize(None)
            nifty_series = nifty_series.rename("benchmark")
            # Fill gaps in index to align with portfolio trading days
            nifty_series = nifty_series.ffill().bfill()
        except Exception as e:
            logger.error(f"Error fetching benchmark: {e}")
            return {"error": "System error fetching benchmark"}

        # 6. Alignment & Normalization
        # Join both series to ensure we only compare dates where both have data
        comparison_df = pd.DataFrame({
            "portfolio": portfolio_series,
            "benchmark": nifty_series
        }).dropna()
        
        # Trim to the requested start date
        comparison_df = comparison_df[comparison_df.index >= pd.to_datetime(start_dt)]

        if comparison_df.empty:
             return {"error": "Insufficient overlapping data for comparison"}

        # Normalize to base 100 at the START of the window
        # This provides a clean point-to-point percentage return comparison
        first_p = float(comparison_df['portfolio'].iloc[0])
        first_b = float(comparison_df['benchmark'].iloc[0])
        
        if first_p <= 0:
             return {"error": "Portfolio has zero or negative value at period start"}

        comparison_df['portfolio_norm'] = (comparison_df['portfolio'] / first_p) * 100
        comparison_df['benchmark_norm'] = (comparison_df['benchmark'] / first_b) * 100
        
        # 7. Final Response Object
        chart_data = []
        for d, row in comparison_df.iterrows():
            chart_data.append({
                "time": int(d.timestamp()),
                "portfolio": round(row['portfolio_norm'], 2),
                "benchmark": round(row['benchmark_norm'], 2)
            })
            
        return {
            "timeframe": timeframe,
            "chart_data": chart_data,
            "metrics": {
                "portfolio_return": round(comparison_df['portfolio_norm'].iloc[-1] - 100, 2),
                "benchmark_return": round(comparison_df['benchmark_norm'].iloc[-1] - 100, 2),
                "alpha": round((comparison_df['portfolio_norm'].iloc[-1] - 100) - (comparison_df['benchmark_norm'].iloc[-1] - 100), 2),
                "start_date": comparison_df.index[0].strftime('%Y-%m-%d'),
                "end_date": comparison_df.index[-1].strftime('%Y-%m-%d')
            }
        }
