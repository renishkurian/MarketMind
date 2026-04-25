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

        from backend.data.db import PortfolioTransaction
        tx_stmt = select(
            PortfolioTransaction.symbol,
            PortfolioTransaction.quantity,
            PortfolioTransaction.buy_date
        ).where(
            and_(
                PortfolioTransaction.user_id == user_id,
                PortfolioTransaction.status == "OPEN"
            )
        )
        tx_res = await self.db.execute(tx_stmt)
        transactions = tx_res.all()

        if not transactions:
            # Fallback to StockMaster
            stmt = select(StockMaster.symbol, StockMaster.quantity, StockMaster.buy_date).where(
                and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
            )
            res = await self.db.execute(stmt)
            stocks = res.all()
            if not stocks:
                return {"error": "Portfolio is empty"}
            transactions = stocks

        symbols = list(set(t.symbol for t in transactions))
        
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
        for tx in transactions:
            if tx.symbol in df_pivot.columns:
                qty = float(tx.quantity or 0)
                entry_date = pd.to_datetime(tx.buy_date) if tx.buy_date else df_pivot.index[0]
                masked = df_pivot[tx.symbol].astype(float).copy()
                masked[masked.index < entry_date] = 0.0
                portfolio_series += masked * qty

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
                
            raw_close = yf_data['Close']
            if isinstance(raw_close, pd.DataFrame):
                nifty_series = raw_close.iloc[:, 0]
            else:
                nifty_series = raw_close
            nifty_series = nifty_series.squeeze()  # flatten any residual MultiIndex
            nifty_series.index = pd.to_datetime(nifty_series.index).tz_localize(None)
            nifty_series = nifty_series.sort_index().dropna()
            nifty_series = nifty_series.rename("benchmark")
        except Exception as e:
            logger.error(f"Error fetching benchmark: {e}")
            return {"error": "System error fetching benchmark"}

        # 6. Alignment & Normalization
        # Reindex nifty onto the portfolio's trading-day index, forward-fill gaps
        # (handles holidays, yfinance lag, and timezone mismatches cleanly)
        common_index = portfolio_series.index
        nifty_aligned = nifty_series.reindex(common_index, method='ffill')
        
        comparison_df = pd.DataFrame({
            "portfolio": portfolio_series,
            "benchmark": nifty_aligned
        })
        
        # Trim to the requested start date BEFORE dropna so we don't lose the tail
        comparison_df = comparison_df[comparison_df.index >= pd.to_datetime(start_dt)]
        comparison_df = comparison_df.dropna()

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
                "time": d.strftime('%Y-%m-%d'),
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

    async def get_yearly_breakdown(self, user_id: int):
        """
        Returns year-by-year portfolio return vs Nifty 50 return.
        Uses available PriceHistory from DB + yfinance for Nifty.
        """
        from sqlalchemy import select, and_
        stmt = select(StockMaster.symbol, StockMaster.quantity, StockMaster.buy_date).where(
            and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        )
        res = await self.db.execute(stmt)
        stocks = res.all()
        if not stocks:
            return {"error": "Portfolio is empty"}

        symbols = [s.symbol for s in stocks]
        p_stmt = select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close).where(
            PriceHistory.symbol.in_(symbols)
        )
        p_res = await self.db.execute(p_stmt)
        rows = p_res.all()
        if not rows:
            return {"error": "No price history found"}

        df = pd.DataFrame(rows, columns=['symbol', 'date', 'close'])
        df['date'] = pd.to_datetime(df['date'])
        df_pivot = df.pivot(index='date', columns='symbol', values='close').sort_index()
        df_pivot = df_pivot.ffill().bfill()

        portfolio_series = pd.Series(0.0, index=df_pivot.index)
        for s in stocks:
            if s.symbol in df_pivot.columns:
                qty = float(s.quantity or 0)
                entry = pd.to_datetime(s.buy_date) if s.buy_date else df_pivot.index[0]
                col = df_pivot[s.symbol].astype(float).copy()
                col[col.index < entry] = 0.0
                portfolio_series += col * qty

        min_year = portfolio_series.index.min().year
        max_year = portfolio_series.index.max().year

        # Fetch Nifty for the full range
        try:
            yf_data = await asyncio.to_thread(
                yf.download, "^NSEI",
                start=f"{min_year}-01-01",
                end=f"{max_year+1}-01-01",
                progress=False
            )
            close_col = yf_data['Close']
            if hasattr(close_col, 'columns') and isinstance(close_col.columns, pd.MultiIndex):
                nifty_series = close_col.iloc[:, 0]
            elif isinstance(close_col, pd.DataFrame):
                nifty_series = close_col.iloc[:, 0]
            else:
                nifty_series = close_col
            nifty_series.index = pd.to_datetime(nifty_series.index).tz_localize(None)
        except Exception as e:
            return {"error": f"Benchmark fetch failed: {e}"}

        years = list(range(min_year, max_year + 1))
        breakdown = []
        for yr in years:
            p_yr = portfolio_series[portfolio_series.index.year == yr]
            n_yr = nifty_series[nifty_series.index.year == yr]
            if p_yr.empty or n_yr.empty:
                continue
            p_return = round(((float(p_yr.iloc[-1]) / float(p_yr.iloc[0])) - 1) * 100, 2) if float(p_yr.iloc[0]) > 0 else 0
            n_return = round(((float(n_yr.iloc[-1]) / float(n_yr.iloc[0])) - 1) * 100, 2) if float(n_yr.iloc[0]) > 0 else 0
            breakdown.append({
                "year": yr,
                "portfolio_return": p_return,
                "nifty_return": n_return,
                "alpha": round(p_return - n_return, 2)
            })

        return {"years": breakdown, "available_years": [b["year"] for b in breakdown]}

    async def get_sector_performance(self, user_id: int, year: int = None):
        """
        Returns sector-wise portfolio allocation and return contribution.
        """
        stmt = select(
            StockMaster.symbol, StockMaster.quantity,
            StockMaster.avg_buy_price, StockMaster.sector, StockMaster.buy_date
        ).where(
            and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        )
        res = await self.db.execute(stmt)
        stocks = res.all()
        if not stocks:
            return {"error": "Portfolio is empty"}

        symbols = [s.symbol for s in stocks]
        today = date.today()
        if year:
            start_dt = date(year, 1, 1)
            end_dt = date(year, 12, 31) if year < today.year else today
        else:
            start_dt = today - timedelta(days=365)
            end_dt = today

        p_stmt = select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close).where(
            and_(PriceHistory.symbol.in_(symbols), PriceHistory.date >= start_dt, PriceHistory.date <= end_dt)
        )
        p_res = await self.db.execute(p_stmt)
        rows = p_res.all()

        if not rows:
            return {"error": "No price history found for the selected period"}

        df = pd.DataFrame(rows, columns=['symbol', 'date', 'close'])
        df['date'] = pd.to_datetime(df['date'])
        df_pivot = df.pivot(index='date', columns='symbol', values='close').sort_index().ffill().bfill()

        sector_data = {}
        for s in stocks:
            sector = s.sector or "Unknown"
            if sector not in sector_data:
                sector_data[sector] = {"symbols": [], "allocation": 0.0, "weighted_return": 0.0, "total_value": 0.0}
            
            if s.symbol in df_pivot.columns and not df_pivot[s.symbol].empty:
                qty = float(s.quantity or 0)
                first_price = float(df_pivot[s.symbol].iloc[0])
                last_price = float(df_pivot[s.symbol].iloc[-1])
                value = qty * last_price
                ret = ((last_price / first_price) - 1) * 100 if first_price > 0 else 0
                sector_data[sector]["symbols"].append(s.symbol)
                sector_data[sector]["total_value"] += value
                sector_data[sector]["weighted_return"] += ret * value

        total_portfolio = sum(v["total_value"] for v in sector_data.values())
        result = []
        for sector, data in sector_data.items():
            alloc = round((data["total_value"] / total_portfolio) * 100, 2) if total_portfolio > 0 else 0
            w_ret = round(data["weighted_return"] / data["total_value"], 2) if data["total_value"] > 0 else 0
            result.append({
                "sector": sector,
                "allocation_pct": alloc,
                "return_pct": w_ret,
                "symbols": data["symbols"],
                "value": round(data["total_value"], 2)
            })

        result.sort(key=lambda x: x["allocation_pct"], reverse=True)
        return {"sector_breakdown": result, "period": {"start": str(start_dt), "end": str(end_dt)}}
