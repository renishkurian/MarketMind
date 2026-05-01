import React, { useState } from 'react';
import { useStockStore } from '../store/stockStore';
import { ArrowUpRight, ArrowRight, ArrowDownRight, Plus, Search, X } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

import SignalBadge from '../components/SignalBadge';
import MarketIndicesTicker from '../components/MarketIndicesTicker';

export default function Watchlist() {
  const { stocks } = useStockStore();
  const navigate = useNavigate();
  const [newSymbol, setNewSymbol] = useState('');
  const [loading, setLoading] = useState(false);
  const [removing, setRemoving] = useState(null);

  const watchlistStocks = Object.values(stocks).filter(s => s.type === 'WATCHLIST');

  const addStock = async (e) => {
    e.preventDefault();
    if (!newSymbol.trim()) return;
    setLoading(true);
    try {
      await axios.post(`${API_URL}/api/watchlist/add`, { symbol: newSymbol.toUpperCase().trim() });
      toast.success(`${newSymbol.toUpperCase()} added to Watchlist`);
      setNewSymbol('');
    } catch (err) {
      const msg = err.response?.data?.detail || 'Failed to add stock';
      toast.error(msg);
    }
    setLoading(false);
  };

  const removeStock = async (symbol, e) => {
    e.stopPropagation();
    setRemoving(symbol);
    try {
      await axios.delete(`${API_URL}/api/watchlist/${symbol}`);
      toast.success(`${symbol} removed from Watchlist`);
    } catch {
      toast.error('Failed to remove stock');
    }
    setRemoving(null);
  };

  return (
    <>
      <MarketIndicesTicker />
      <div className="flex flex-col min-h-screen p-6">
        <div className="mb-6">
          <h2 className="text-xl font-bold mb-1">Watchlist</h2>
          <p className="text-dark-muted text-sm">{watchlistStocks.length} stocks being tracked</p>
        </div>

      {/* Add Stock Form */}
      <form onSubmit={addStock} className="flex items-center gap-3 mb-8">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={16} />
          <input
            type="text"
            placeholder="NSE symbol (e.g. INFY)"
            value={newSymbol}
            onChange={e => setNewSymbol(e.target.value.toUpperCase())}
            className="pl-9 pr-4 py-2.5 rounded-xl border border-dark-border bg-dark-card text-dark-text placeholder-dark-muted focus:outline-none focus:ring-2 focus:ring-accent w-64 text-sm font-mono"
          />
        </div>
        <button
          type="submit"
          disabled={loading || !newSymbol.trim()}
          className="flex items-center gap-2 bg-accent text-white px-4 py-2.5 rounded-xl hover:bg-blue-500 transition-colors text-sm font-medium disabled:opacity-50"
        >
          {loading ? (
            <span className="flex items-center gap-2"><div className="w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />Adding…</span>
          ) : (
            <><Plus size={16} />Add to Watchlist</>
          )}
        </button>
      </form>

      {/* Card Grid */}
      {watchlistStocks.length === 0 ? (
        <div className="flex-1 flex flex-col items-center justify-center text-dark-muted gap-4">
          <div className="w-16 h-16 rounded-2xl bg-dark-card border border-dark-border flex items-center justify-center">
            <Search size={28} className="opacity-30" />
          </div>
          <p className="text-sm">Your watchlist is empty.</p>
          <p className="text-xs text-dark-muted/60">Search for an NSE symbol above to start tracking.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {watchlistStocks.map(stock => {
            const sig = stock.signal || {};
            const changePos = (sig.change_pct ?? 0) > 0;
            const changeNeg = (sig.change_pct ?? 0) < 0;
            return (
              <div
                key={stock.symbol}
                onClick={() => navigate(`/stock/${stock.symbol}`)}
                className="bg-dark-card border border-dark-border rounded-2xl p-5 hover:shadow-xl hover:border-accent/30 cursor-pointer transition-all group relative"
              >
                {/* Remove button */}
                <button
                  onClick={e => removeStock(stock.symbol, e)}
                  disabled={removing === stock.symbol}
                  className="absolute top-3 right-3 p-1.5 rounded-lg text-dark-muted hover:text-signal-sell hover:bg-signal-sell/10 transition-colors opacity-0 group-hover:opacity-100"
                  title="Remove from watchlist"
                >
                  {removing === stock.symbol
                    ? <div className="w-3 h-3 border-2 border-signal-sell/40 border-t-signal-sell rounded-full animate-spin" />
                    : <X size={13} />
                  }
                </button>

                <div className="flex justify-between items-start mb-4">
                  <div className="min-w-0 pr-6">
                    <h3 className="font-bold font-mono text-lg text-accent group-hover:underline">{stock.symbol}</h3>
                    <p className="text-xs text-dark-muted truncate mt-0.5">{stock.company_name}</p>
                    {stock.sector && (
                      <span className="mt-1 inline-block text-[10px] px-1.5 py-0.5 bg-accent/10 text-accent/70 border border-accent/20 rounded font-mono">
                        {stock.sector}
                      </span>
                    )}
                  </div>
                  <div className="text-right shrink-0">
                    <p className="font-mono text-lg font-bold text-[#1E293B] dark:text-[#F8FAFC]">
                      {sig.current_price != null ? `₹${sig.current_price.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
                    </p>
                    <p className={`font-mono text-xs font-semibold ${changePos ? 'text-signal-buy' : changeNeg ? 'text-signal-sell' : 'text-[#64748B] dark:text-[#94A3B8]'}`}>
                      {sig.change_pct != null ? `${changePos ? '+' : ''}${sig.change_pct.toFixed(2)}%` : '—'}
                    </p>
                  </div>
                </div>

                <div className="flex justify-between items-center border-t border-dark-border pt-3">
                  <div>
                    <p className="text-[10px] text-dark-muted mb-1 uppercase tracking-wide">Short-Term</p>
                    <SignalBadge signal={sig.st_signal} />
                  </div>
                  <div className="text-right">
                    <p className="text-[10px] text-dark-muted mb-1 uppercase tracking-wide">Long-Term</p>
                    <SignalBadge signal={sig.lt_signal} />
                  </div>
                  {sig.confidence_pct != null && (
                    <div className="text-right">
                      <p className="text-[10px] text-dark-muted mb-1 uppercase tracking-wide">Confidence</p>
                      <p className="text-xs font-mono font-bold text-accent">{sig.confidence_pct.toFixed(0)}%</p>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
    </>
  );
}
