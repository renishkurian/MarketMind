import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta, date
from sqlalchemy import select, and_, delete
from sqlalchemy.dialects.mysql import insert
from backend.data.db import PriceHistory, StockMaster, PerformanceCache, PortfolioTransaction
import logging
import asyncio
import datetime

logger = logging.getLogger(__name__)

class PerformanceEngine:
    def __init__(self, db):
        self.db = db

    def _extract_nifty_close(self, yf_data):
        close = yf_data['Close']
        if isinstance(close, pd.DataFrame):
            s = close.iloc[:, 0]
        else:
            s = close
        s = s.squeeze()
        s.index = pd.to_datetime(s.index).tz_localize(None)
        return s.sort_index().dropna().rename("benchmark")

    async def get_benchmark_comparison(self, user_id: int, timeframe: str = "yearly", benchmark_symbol: str = "^NSEI", force_refresh: bool = False):
        """
        Calculates the historical portfolio value vs a selected benchmark.
        Normalized to 100 at the start of the period.
        """
        cache_key = f"{timeframe}_{benchmark_symbol}"
        if not force_refresh:
            stmt = select(PerformanceCache).where(
                and_(
                    PerformanceCache.user_id == user_id,
                    PerformanceCache.cache_type == "benchmark",
                    PerformanceCache.cache_key == cache_key
                )
            )
            res = await self.db.execute(stmt)
            entry = res.scalar_one_or_none()
            if entry:
                # 4 hour cache
                if (datetime.datetime.utcnow() - entry.updated_at).total_seconds() < 14400:
                    logger.info(f"Returning DB cached performance for {cache_key}")
                    return entry.data

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
                col = df_pivot[tx.symbol].astype(float)
                col = col[col.index >= entry_date]
                portfolio_series = portfolio_series.add(col * qty, fill_value=0)

        # 5. Fetch Benchmark Index
        nifty_symbol = benchmark_symbol
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
                
            nifty_series = self._extract_nifty_close(yf_data)
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
        first_nonzero = comparison_df['portfolio'][comparison_df['portfolio'] > 0]
        if first_nonzero.empty:
             return {"error": "Portfolio has zero or negative value at period start"}
        comparison_df = comparison_df[comparison_df.index >= first_nonzero.index[0]]
        first_p = float(comparison_df['portfolio'].iloc[0])
        first_b = float(comparison_df['benchmark'].iloc[0])

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

        result = {
            "timeframe": timeframe,
            "chart_data": chart_data,
            "metrics": {
                "portfolio_return": round(comparison_df['portfolio_norm'].iloc[-1] - 100, 2),
                "benchmark_return": round(comparison_df['benchmark_norm'].iloc[-1] - 100, 2),
                "alpha": round(
                    (comparison_df['portfolio_norm'].iloc[-1] - 100) -
                    (comparison_df['benchmark_norm'].iloc[-1] - 100), 2
                ),
                "start_date": comparison_df.index[0].strftime('%Y-%m-%d'),
                "end_date": comparison_df.index[-1].strftime('%Y-%m-%d')
            }
        }
        
        # Save to DB cache (Upsert logic)
        try:
            # Delete old entry
            await self.db.execute(
                delete(PerformanceCache).where(
                    and_(
                        PerformanceCache.user_id == user_id,
                        PerformanceCache.cache_type == "benchmark",
                        PerformanceCache.cache_key == cache_key
                    )
                )
            )
            # Insert new
            new_cache = PerformanceCache(
                user_id=user_id,
                cache_type="benchmark",
                cache_key=cache_key,
                data=result,
                updated_at=datetime.datetime.utcnow()
            )
            self.db.add(new_cache)
            await self.db.commit()
        except Exception as ce:
            logger.error(f"Failed to save performance cache: {ce}")
            await self.db.rollback()

        return result

    async def get_yearly_breakdown(self, user_id: int, force_refresh: bool = False):
        """
        Returns year-by-year portfolio return vs Nifty 50 return.
        Uses available PriceHistory from DB + yfinance for Nifty.
        """
        if not force_refresh:
            stmt = select(PerformanceCache).where(
                and_(
                    PerformanceCache.user_id == user_id,
                    PerformanceCache.cache_type == "yearly",
                    PerformanceCache.cache_key == "all"
                )
            )
            res = await self.db.execute(stmt)
            entry = res.scalar_one_or_none()
            if entry:
                if (datetime.datetime.utcnow() - entry.updated_at).total_seconds() < 14400:
                    return entry.data
        

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
        stocks = tx_res.all()

        if not stocks:
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
                col = df_pivot[s.symbol].astype(float)
                col = col[col.index >= entry]
                portfolio_series = portfolio_series.add(col * qty, fill_value=0)

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
            nifty_series = self._extract_nifty_close(yf_data)
        except Exception as e:
            return {"error": f"Benchmark fetch failed: {e}"}

        years = list(range(min_year, max_year + 1))
        breakdown = []
        for yr in years:
            p_yr = portfolio_series[portfolio_series.index.year == yr]
            n_yr = nifty_series[nifty_series.index.year == yr]
            
            if p_yr.empty or n_yr.empty:
                continue

            # Filter zero-value days for the portfolio (prevents inf on first buy)
            p_yr_active = p_yr[p_yr > 0]
            if p_yr_active.empty:
                p_return = 0.0
            else:
                p_daily_ret = p_yr_active.pct_change().dropna()
                # Clean any accidental inf/nan to ensure JSON compliance
                p_daily_ret = p_daily_ret.replace([np.inf, -np.inf], np.nan).dropna()
                p_return = round(((1 + p_daily_ret).prod() - 1) * 100, 2) if not p_daily_ret.empty else 0.0
            
            n_first = float(n_yr.iloc[0])
            n_return = round(((float(n_yr.iloc[-1]) / n_first) - 1) * 100, 2) if n_first > 0 else 0
            
            breakdown.append({
                "year": yr,
                "portfolio_return": p_return,
                "nifty_return": n_return,
                "alpha": round(p_return - n_return, 2)
            })

        res = {"years": breakdown, "available_years": [b["year"] for b in breakdown]}
        
        # Save to DB cache
        try:
            await self.db.execute(
                delete(PerformanceCache).where(
                    and_(
                        PerformanceCache.user_id == user_id,
                        PerformanceCache.cache_type == "yearly",
                        PerformanceCache.cache_key == "all"
                    )
                )
            )
            new_cache = PerformanceCache(
                user_id=user_id,
                cache_type="yearly",
                cache_key="all",
                data=res,
                updated_at=datetime.datetime.utcnow()
            )
            self.db.add(new_cache)
            await self.db.commit()
        except Exception as ce:
            logger.error(f"Failed to save yearly cache: {ce}")
            await self.db.rollback()

        return res

    async def get_stock_performance_matrix(self, user_id: int, force_refresh: bool = False):
        """
        Calculates a matrix of [Stock] x [Year] performance.
        Includes OPTIMAL WEIGHTS from PyPortfolioOpt.
        Caches in DB to avoid hitting yfinance every time.
        """
        # A. Check Cache First
        if not force_refresh:
            c_stmt = select(PerformanceCache).where(
                and_(
                    PerformanceCache.user_id == user_id, 
                    PerformanceCache.cache_type == "STOCK_MATRIX",
                    PerformanceCache.cache_key == "ALL_ASSETS"
                )
            )
            c_res = await self.db.execute(c_stmt)
            cache = c_res.scalar_one_or_none()
            if cache:
                age = (datetime.datetime.utcnow() - cache.updated_at).total_seconds()
                if age < 14400:  # 4 hour TTL same as others
                    return cache.data

        # B. Fetch active portfolio
        stmt = select(StockMaster).where(
            and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        )
        res = await self.db.execute(stmt)
        stocks = res.scalars().all()
        if not stocks:
            return {"error": "Portfolio is empty"}

        symbols = [s.symbol for s in stocks]
        yf_symbols = [f"{s}.NS" if "^" not in s and "." not in s else s for s in symbols]
        
        # C. Get Optimal Weights (Integration with PyPortfolioOpt)
        opt_weights = {}
        try:
            from backend.features.portfolio_opt.opt_engine import PortfolioOptEngine
            opt_engine = PortfolioOptEngine(self.db)
            opt_res = await opt_engine.optimize_portfolio(symbols)
            if "weights" in opt_res:
                opt_weights = opt_res["weights"]
        except Exception as e:
            logger.warning(f"Heatmap Opt integration failed: {e}")

        # D. Fetch Historical Data (Adjusted)
        try:
            start_date = "2021-01-01"
            data = await asyncio.to_thread(
                yf.download, 
                yf_symbols, 
                start=start_date, 
                progress=False,
                auto_adjust=True
            )
            if data.empty:
                return {"error": "Failed to fetch market data for heatmap"}

            close_data = data['Close']
            if isinstance(close_data, pd.Series):
                close_data = close_data.to_frame()
                close_data.columns = [yf_symbols[0]]
            
        except Exception as e:
            logger.error(f"Heatmap YF error: {e}")
            return {"error": f"Market data sync failed: {str(e)}"}

        close_data.index = pd.to_datetime(close_data.index).tz_localize(None)
        all_years = sorted([int(yr) for yr in close_data.index.year.unique() if int(yr) >= 2021])
        
        matrix = []
        for stock in stocks:
            sym = stock.symbol
            yf_sym = f"{sym}.NS" if "^" not in sym and "." not in sym else sym
            
            if yf_sym not in close_data.columns:
                continue

            sym_series = close_data[yf_sym].dropna()
            if sym_series.empty:
                continue

            row = {
                "symbol": sym, 
                "company": stock.company_name, 
                "optimal_weight": float(round(opt_weights.get(sym, 0) * 100, 2)),
                "years": {}
            }
            buy_year = int(stock.buy_date.year) if stock.buy_date else all_years[0]

            for yr in all_years:
                if yr < buy_year:
                    row["years"][str(yr)] = "N/A"
                    continue
                
                yr_data = sym_series[sym_series.index.year == yr]
                if stock.buy_date and yr == buy_year:
                    yr_data = yr_data[yr_data.index >= pd.to_datetime(stock.buy_date)]
                
                if yr_data.empty:
                    row["years"][str(yr)] = "N/A"
                    continue
                
                first_price = float(yr_data.iloc[0])
                last_price = float(yr_data.iloc[-1])
                
                if first_price > 0:
                    pct = round(((last_price / first_price) - 1) * 100, 2)
                    row["years"][str(yr)] = float(pct)
                else:
                    row["years"][str(yr)] = 0.0

            matrix.append(row)

        final_data = {
            "years": all_years,
            "matrix": matrix,
            "cached_at": datetime.datetime.now().isoformat()
        }

        # E. Save to Cache
        upsert_stmt = insert(PerformanceCache).values(
            user_id=user_id,
            cache_type="STOCK_MATRIX",
            cache_key="ALL_ASSETS",
            data=final_data,
            updated_at=datetime.datetime.utcnow()
        ).on_duplicate_key_update(
            data=final_data,
            updated_at=datetime.datetime.utcnow()
        )
        await self.db.execute(upsert_stmt)
        await self.db.commit()

        return final_data

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

        tx_stmt = select(
            PortfolioTransaction.symbol,
            PortfolioTransaction.buy_price,
            PortfolioTransaction.quantity
        ).where(
            and_(
                PortfolioTransaction.user_id == user_id,
                PortfolioTransaction.status == "OPEN"
            )
        )
        tx_res = await self.db.execute(tx_stmt)
        tx_rows = tx_res.all()
        vwap_map = {}
        for t in tx_rows:
            sym = t.symbol
            if sym not in vwap_map:
                vwap_map[sym] = {"cost": 0.0, "qty": 0.0}
            vwap_map[sym]["cost"] += float(t.buy_price) * float(t.quantity)
            vwap_map[sym]["qty"] += float(t.quantity)
        vwap_price = {
            sym: d["cost"] / d["qty"]
            for sym, d in vwap_map.items() if d["qty"] > 0
        }

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
                buy_price = vwap_price.get(s.symbol, float(s.avg_buy_price) if s.avg_buy_price else first_price)
                ret = ((last_price / buy_price) - 1) * 100 if buy_price > 0 else 0
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
    async def get_performance_dashboard_summary(self, user_id: int, force_refresh: bool = False):
        """
        Comprehensive dashboard stats with DB caching.
        """
        # 1. Check Cache
        if not force_refresh:
            c_stmt = select(PerformanceCache).where(
                and_(
                    PerformanceCache.user_id == user_id, 
                    PerformanceCache.cache_type == "PERFORMANCE_SUMMARY",
                    PerformanceCache.cache_key == "DASHBOARD"
                )
            )
            c_res = await self.db.execute(c_stmt)
            cache = c_res.scalar_one_or_none()
            if cache:
                return cache.data

        # ... (Rest of calculation starts here)
        # A. Fetch Portfolio
        stmt = select(StockMaster).where(
            and_(StockMaster.user_id == user_id, StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        )
        res = await self.db.execute(stmt)
        stocks = res.scalars().all()
        if not stocks:
            return {"error": "Portfolio is empty"}

        symbols = [s.symbol for s in stocks]
        
        # B. Timing setup
        today = date.today()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        year_ago = today - timedelta(days=365)
        start_2021 = date(2021, 1, 1)

        # C. Portfolio YoY Growth calculation
        # We fetch prices for all years for our symbols
        p_stmt = select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close).where(
            and_(PriceHistory.symbol.in_(symbols), PriceHistory.date >= start_2021)
        ).order_by(PriceHistory.date.asc())
        p_res = await self.db.execute(p_stmt)
        history = pd.DataFrame(p_res.all(), columns=['symbol', 'date', 'close'])
        history['date'] = pd.to_datetime(history['date'])
        history['year'] = history['date'].dt.year

        # Build VWAP from actual transaction lots (split-adjusted, source of truth)
        tx_stmt = select(
            PortfolioTransaction.symbol,
            PortfolioTransaction.buy_price,
            PortfolioTransaction.quantity
        ).where(
            and_(
                PortfolioTransaction.user_id == user_id,
                PortfolioTransaction.status == "OPEN"
            )
        )
        tx_res = await self.db.execute(tx_stmt)
        tx_rows = tx_res.all()
        vwap_cost = {}  # symbol -> total invested (buy_price * qty summed across lots)
        for t in tx_rows:
            vwap_cost[t.symbol] = vwap_cost.get(t.symbol, 0.0) + float(t.buy_price) * float(t.quantity)

        yoy_growth = []
        running_total_profit = 0
        initial_investment_base = 0
        
        for i, yr in enumerate(sorted(history['year'].unique())):
            yr_data = history[history['year'] == yr]
            total_yr_start = 0
            total_yr_end = 0
            
            for s in stocks:
                s_data = yr_data[yr_data['symbol'] == s.symbol]
                if s_data.empty: continue
                
                qty = float(s.quantity or 0)
                buy_yr = s.buy_date.year if s.buy_date else 2021
                
                if yr == buy_yr:
                    total_yr_start += vwap_cost.get(s.symbol, float(s.avg_buy_price or 0) * qty)
                else:
                    total_yr_start += float(s_data.iloc[0]['close']) * qty
                
                total_yr_end += float(s_data.iloc[-1]['close']) * qty
            
            if total_yr_start > 0:
                # Capture the original investment base for cumulative % calculations
                if i == 0: initial_investment_base = total_yr_start

                growth_pct = round(((total_yr_end / total_yr_start) - 1) * 100, 2)
                profit_amt = round(total_yr_end - total_yr_start, 2)
                running_total_profit += profit_amt
                
                # Cumulative ROI relative to starting point
                cumulative_roi = 0
                if initial_investment_base > 0:
                    cumulative_roi = round((running_total_profit / initial_investment_base) * 100, 2)

                yoy_growth.append({
                    "year": int(yr), 
                    "growth": float(growth_pct),
                    "profit": float(profit_amt),
                    "cumulative_profit": float(round(running_total_profit, 2)),
                    "cumulative_roi": float(cumulative_roi)
                })

        # Calculate TOTAL Portfolio Profit accurately (Current Market Value - Total Investment)
        # This will match the Portfolio page exactly.
        total_current_value = 0
        total_invested = 0
        for s in stocks:
            # We use the latest price from the history dataframe we already fetched
            s_history = history[history['symbol'] == s.symbol]
            if not s_history.empty:
                current_p = float(s_history.iloc[-1]['close'])
                qty = float(s.quantity or 0)
                
                total_current_value += current_p * qty
                total_invested += vwap_cost.get(s.symbol, float(s.avg_buy_price or 0) * qty)
        
        grand_total_profit = round(total_current_value - total_invested, 2)
        grand_total_roi = round((grand_total_profit / total_invested * 100) if total_invested > 0 else 0, 2)

        # D. Top Performers (Portfolio)
        def get_top_gainers(df, ref_date, limit=3):
            gainers = []
            current_date = df['date'].max()
            for sym in df['symbol'].unique():
                sym_df = df[df['symbol'] == sym].sort_values('date')
                # Find closest price to ref_date
                ref_prices = sym_df[sym_df['date'] <= pd.to_datetime(ref_date)]
                if ref_prices.empty: continue
                
                start_p = float(ref_prices.iloc[-1]['close'])
                end_p = float(sym_df.iloc[-1]['close'])
                if start_p > 0:
                    gain = round(((end_p / start_p) - 1) * 100, 2)
                    gainers.append({"symbol": sym, "gain": float(gain)})
            
            return sorted(gainers, key=lambda x: x['gain'], reverse=True)[:limit]

        portfolio_top = {
            "week": get_top_gainers(history, week_ago),
            "month": get_top_gainers(history, month_ago),
            "year": get_top_gainers(history, year_ago)
        }

        # E. Market Leaders (Global) - Fast Query with Name Resolution
        async def get_market_leaders(ref_dt, limit=5):
            # We need prices on exactly today and ref_dt (or closest)
            l_stmt = select(PriceHistory.date).order_by(PriceHistory.date.desc()).limit(1)
            l_res = await self.db.execute(l_stmt)
            latest_dt = l_res.scalar()
            if not latest_dt: return {"nse": [], "bse": []}

            # Subquery for prices at closest to ref_dt
            s2_sub = select(PriceHistory.date).where(PriceHistory.date <= ref_dt).order_by(PriceHistory.date.desc()).limit(1)
            s2_dt_res = await self.db.execute(s2_sub)
            s2_dt = s2_dt_res.scalar()
            if not s2_dt: return {"nse": [], "bse": []}

            results = {"nse": [], "bse": []}
            
            for exch in ['NSE', 'BSE']:
                # Subquery for prices at latest_dt for this exchange
                s1 = select(PriceHistory.symbol, PriceHistory.close).where(
                    and_(PriceHistory.date == latest_dt, PriceHistory.exchange == exch)
                ).alias('s1')
                
                s2 = select(PriceHistory.symbol, PriceHistory.close).where(
                    and_(PriceHistory.date == s2_dt, PriceHistory.exchange == exch)
                ).alias('s2')

                # Join: fetch limit * 6 to give enough candidates (true gainers + splits)
                final_stmt = select(
                    s1.c.symbol, s1.c.close, s2.c.close
                ).join(s2, s1.c.symbol == s2.c.symbol).order_by(((s1.c.close/s2.c.close)-1).desc()).limit(limit * 6)
                
                f_res = await self.db.execute(final_stmt)
                candidates = f_res.all()
                if not candidates: continue

                yf_tickers = []
                yf_map = {}
                for r in candidates:
                    yf_sym = f"{r[0]}.BO" if exch == "BSE" else f"{r[0]}.NS"
                    yf_tickers.append(yf_sym)
                    yf_map[yf_sym] = r[0]

                try:
                    # Download recent data (from ref_dt forward) for these candidates
                    # yfinance automatically returns Adjusted Close for 'Close' by default
                    yf_data = await asyncio.to_thread(
                        yf.download, yf_tickers, start=(ref_dt - timedelta(days=7)).strftime('%Y-%m-%d'), progress=False
                    )
                    
                    if not yf_data.empty:
                        close_data = yf_data['Close']
                        if not isinstance(close_data, pd.DataFrame):
                            close_data = pd.DataFrame({yf_tickers[0]: close_data})
                        
                        valid_leaders = []
                        for yf_sym, sym in yf_map.items():
                            if yf_sym in close_data.columns:
                                col = close_data[yf_sym].dropna()
                                if not col.empty:
                                    last_p = col.iloc[-1]
                                    start_p_df = col[col.index >= pd.to_datetime(ref_dt)]
                                    start_p = start_p_df.iloc[0] if not start_p_df.empty else col.iloc[0]
                                    
                                    if start_p > 0:
                                        yf_gain = ((last_p / start_p) - 1) * 100
                                        valid_leaders.append({
                                            "symbol": sym,
                                            "yf_sym": yf_sym,
                                            "gain": float(yf_gain)
                                        })
                        
                        # Sort by true adjusted gain and take top
                        valid_leaders.sort(key=lambda x: x['gain'], reverse=True)
                        def _get_name(ticker, fallback):
                            try:
                                info = yf.Ticker(ticker).info
                                return info.get('longName', info.get('shortName', fallback))
                            except:
                                return fallback

                        for vl in valid_leaders[:limit]:
                            name = vl["symbol"]
                            if exch == 'BSE' or name.isdigit():
                                name = await asyncio.to_thread(_get_name, vl["yf_sym"], name)
                            results[exch.lower()].append({
                                "symbol": vl["symbol"],
                                "name": name,
                                "gain": round(vl['gain'], 2)
                            })
                    else:
                        raise Exception("Empty yfinance data")
                except Exception as e:
                    logger.error(f"Sanity check failed for {exch}: {e}")
                    # Fallback to pure DB if failed
                    count = 0
                    for r in candidates:
                        if count >= limit: break
                        results[exch.lower()].append({
                            "symbol": r[0], 
                            "name": r[0],
                            "gain": round(((float(r[1])/float(r[2])) - 1) * 100, 2)
                        })
                        count += 1
            return results

        market_leaders = {
            "week": await get_market_leaders(week_ago),
            "month": await get_market_leaders(month_ago),
            "year": await get_market_leaders(year_ago)
        }

        analysis_date = today.isoformat()
        try:
            l_stmt = select(PriceHistory.date).order_by(PriceHistory.date.desc()).limit(1)
            l_res = await self.db.execute(l_stmt)
            _ld = l_res.scalar()
            if _ld:
                analysis_date = _ld.isoformat()
        except:
            pass

        final_data = {
            "yoy_growth": yoy_growth,
            "grand_total_profit": grand_total_profit,
            "grand_total_roi": grand_total_roi,
            "portfolio_performers": portfolio_top,
            "market_leaders": market_leaders,
            "analysis_date": analysis_date
        }

        # Save to Cache
        upsert_stmt = insert(PerformanceCache).values(
            user_id=user_id,
            cache_type="PERFORMANCE_SUMMARY",
            cache_key="DASHBOARD",
            data=final_data,
            updated_at=datetime.datetime.utcnow()
        ).on_duplicate_key_update(
            data=final_data,
            updated_at=datetime.datetime.utcnow()
        )
        await self.db.execute(upsert_stmt)
        await self.db.commit()

        return final_data
