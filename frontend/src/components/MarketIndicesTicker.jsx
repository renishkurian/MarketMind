import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { useStockStore } from '../store/stockStore';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function MarketIndicesTicker() {
  const { tickerEnabled } = useStockStore();
  const [indices, setIndices] = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError] = useState(false);

  const fetchIndices = async () => {
    if (!tickerEnabled) return;
    try {
      const res = await axios.get(`${API_URL}/api/market/indices`);
      if (res.data && res.data.length > 0) {
        setIndices(res.data);
        setLastUpdated(new Date());
        setError(false);
      } else {
        if (indices.length === 0) {
          setError(true);
        }
      }
    } catch (err) {
      console.error("Failed to fetch market indices:", err);
      if (indices.length === 0) {
        setError(true);
      }
    }
  };

  useEffect(() => {
    fetchIndices();
    const interval = setInterval(fetchIndices, 60000); // 60 seconds
    return () => clearInterval(interval);
  }, [tickerEnabled]);

  if (!tickerEnabled || error || indices.length === 0) {
    return null;
  }

  // Shorten names for the ticker
  const formatName = (name) => {
    const map = {
      "NIFTY 50": "NIFTY 50",
      "NIFTY BANK": "BANK NIFTY",
      "NIFTY IT": "NIFTY IT",
      "NIFTY MIDCAP 100": "MIDCAP 100",
      "NIFTY SMALLCAP 100": "SMALLCAP 100",
      "NIFTY FMCG": "FMCG",
      "NIFTY PHARMA": "PHARMA",
      "NIFTY AUTO": "AUTO",
      "NIFTY REALTY": "REALTY",
      "NIFTY METAL": "METAL",
      "INDIA VIX": "INDIA VIX"
    };
    return map[name] || name;
  };

  const formatPrice = (val) => {
    return val.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  return (
    <div className="w-full bg-dark-bg border-b border-dark-border py-2.5 overflow-hidden flex flex-col justify-center relative">
      <style>{`
        .hide-scroll::-webkit-scrollbar {
          display: none;
        }
      `}</style>
      <div 
        className="flex items-center gap-6 overflow-x-auto px-6 pb-1 hide-scroll"
        style={{ msOverflowStyle: 'none', scrollbarWidth: 'none' }}
      >
        {indices.map((idx, i) => {
          const isUp = idx.direction === 'up';
          const isDown = idx.direction === 'down';
          const colorClass = isUp ? 'text-signal-buy' : isDown ? 'text-signal-sell' : 'text-dark-muted';
          
          return (
            <div key={idx.name} className="flex flex-col flex-shrink-0 min-w-max border-r border-dark-border pr-6 last:border-0 last:pr-0">
              <span className="text-[10px] font-bold text-dark-muted uppercase tracking-wider mb-0.5">{formatName(idx.name)}</span>
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm font-bold text-dark-text">{formatPrice(idx.last)}</span>
                <div className={`flex items-center font-mono text-xs font-semibold ${colorClass}`}>
                  {isUp && <TrendingUp size={12} className="mr-0.5" />}
                  {isDown && <TrendingDown size={12} className="mr-0.5" />}
                  {!isUp && !isDown && <Minus size={12} className="mr-0.5" />}
                  {idx.change > 0 ? '+' : ''}{idx.change.toFixed(2)} ({idx.change > 0 ? '+' : ''}{idx.pct_change.toFixed(2)}%)
                </div>
              </div>
            </div>
          );
        })}
      </div>
      {lastUpdated && (
        <div className="absolute right-6 top-1 text-[9px] font-mono text-dark-muted/60">
          Last updated: {lastUpdated.toLocaleTimeString()}
        </div>
      )}
    </div>
  );
}
