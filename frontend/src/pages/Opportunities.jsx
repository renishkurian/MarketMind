import React, { useMemo, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import { useNavigate } from 'react-router-dom';
import {
  TrendingUp, ArrowUpRight, ArrowDownRight, ArrowRight, Filter, Zap
} from 'lucide-react';

const SIGNAL_COLORS = {
  BUY: { text: 'text-signal-buy', bg: 'bg-signal-buy/15', border: 'border-signal-buy/30' },
  HOLD: { text: 'text-signal-hold', bg: 'bg-signal-hold/15', border: 'border-signal-hold/30' },
  SELL: { text: 'text-signal-sell', bg: 'bg-signal-sell/15', border: 'border-signal-sell/30' },
};

const SignalIcon = ({ signal }) => {
  if (signal === 'BUY') return <ArrowUpRight size={14} />;
  if (signal === 'SELL') return <ArrowDownRight size={14} />;
  return <ArrowRight size={14} />;
};

const ConfidenceBar = ({ value }) => {
  const pct = value ?? 0;
  const color = pct >= 70 ? 'bg-signal-buy' : pct >= 45 ? 'bg-signal-hold' : 'bg-signal-sell';
  return (
    <div className="flex items-center gap-2 w-full">
      <div className="flex-1 h-1.5 bg-gray-700/50 rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-700 ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="font-mono text-xs w-8 text-right">{pct.toFixed(0)}%</span>
    </div>
  );
};

const RankBadge = ({ rank }) => {
  const colors = ['bg-yellow-400/20 text-yellow-300 border-yellow-400/40', 'bg-gray-400/20 text-gray-300 border-gray-400/40', 'bg-orange-400/20 text-orange-300 border-orange-400/40'];
  const cls = colors[rank - 1] || 'bg-accent/10 text-accent border-accent/20';
  return (
    <span className={`inline-flex items-center justify-center w-7 h-7 rounded-full border text-xs font-bold ${cls}`}>
      {rank}
    </span>
  );
};

export default function Opportunities() {
  const { stocks } = useStockStore();
  const navigate = useNavigate();
  const [signalFilter, setSignalFilter] = useState('ALL');
  const [minConfidence, setMinConfidence] = useState(0);

  const ranked = useMemo(() => {
    return Object.values(stocks)
      .filter(s => {
        const sig = s.signal;
        if (!sig) return false;
        if (signalFilter !== 'ALL' && sig.st_signal !== signalFilter) return false;
        if ((sig.confidence_pct ?? 0) < minConfidence) return false;
        return true;
      })
      .sort((a, b) => (b.signal?.confidence_pct ?? 0) - (a.signal?.confidence_pct ?? 0));
  }, [stocks, signalFilter, minConfidence]);

  const totals = useMemo(() => {
    const all = Object.values(stocks).filter(s => s.signal);
    return {
      all: all.length,
      buy: all.filter(s => s.signal?.st_signal === 'BUY').length,
      hold: all.filter(s => s.signal?.st_signal === 'HOLD').length,
      sell: all.filter(s => s.signal?.st_signal === 'SELL').length,
    };
  }, [stocks]);

  return (
    <div className="p-6 space-y-8">
      {/* Page Title */}
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Opportunities Matrix</h1>
        <p className="text-dark-muted mt-2">Ranked potential entries based on multi-timeframe signals and confidence.</p>
      </div>

      {/* Summary Strip */}
      <div className="grid grid-cols-4 gap-3 mb-8">
        {[
          { label: 'Universe', val: totals.all, color: 'text-accent', icon: null },
          { label: 'BUY', val: totals.buy, color: 'text-signal-buy', icon: <ArrowUpRight size={16} /> },
          { label: 'HOLD', val: totals.hold, color: 'text-signal-hold', icon: <ArrowRight size={16} /> },
          { label: 'SELL', val: totals.sell, color: 'text-signal-sell', icon: <ArrowDownRight size={16} /> },
        ].map(({ label, val, color, icon }) => (
          <div key={label} className="bg-dark-card border border-dark-border rounded-xl p-4 flex items-center gap-4">
            <div>
              <p className="text-dark-muted text-xs font-medium mb-0.5">{label}</p>
              <p className={`text-3xl font-bold font-mono ${color}`}>{val}</p>
            </div>
            {icon && <span className={`ml-auto ${color}`}>{icon}</span>}
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4 mb-6">
        <Filter size={16} className="text-dark-muted" />
        <div className="flex gap-2">
          {['ALL', 'BUY', 'HOLD', 'SELL'].map(f => {
            const active = signalFilter === f;
            const colors = { BUY: 'bg-signal-buy text-white', HOLD: 'bg-signal-hold text-white', SELL: 'bg-signal-sell text-white', ALL: 'bg-accent text-white' };
            return (
              <button
                key={f}
                onClick={() => setSignalFilter(f)}
                className={`px-4 py-1.5 rounded-full text-xs font-semibold border transition-all ${
                  active ? colors[f] : 'border-dark-border text-dark-muted hover:border-accent hover:text-accent'
                }`}
              >
                {f}
              </button>
            );
          })}
        </div>
        <div className="flex items-center gap-3 ml-auto text-sm">
          <label className="text-dark-muted text-xs">Min Confidence</label>
          <input
            type="range" min={0} max={100} step={5}
            value={minConfidence}
            onChange={e => setMinConfidence(Number(e.target.value))}
            className="w-28 accent-accent"
          />
          <span className="font-mono text-xs text-accent w-8">{minConfidence}%</span>
        </div>
      </div>

      {/* Ranked Table */}
      <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden shadow-xl">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-dark-border bg-gray-900/60 text-dark-muted">
                <th className="px-5 py-4 text-left font-semibold w-12">#</th>
                <th className="px-5 py-4 text-left font-semibold">Symbol</th>
                <th className="px-5 py-4 text-right font-semibold">Price</th>
                <th className="px-5 py-4 text-right font-semibold">Change</th>
                <th className="px-5 py-4 text-center font-semibold">ST Signal</th>
                <th className="px-5 py-4 text-center font-semibold">LT Signal</th>
                <th className="px-5 py-4 font-semibold w-48">Confidence</th>
                <th className="px-5 py-4 text-center font-semibold">Data Quality</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-dark-border">
              {ranked.map((stock, idx) => {
                const sig = stock.signal || {};
                const stColors = SIGNAL_COLORS[sig.st_signal] || SIGNAL_COLORS.HOLD;
                const ltColors = SIGNAL_COLORS[sig.lt_signal] || SIGNAL_COLORS.HOLD;
                return (
                  <tr
                    key={stock.symbol}
                    onClick={() => navigate(`/stock/${stock.symbol}`)}
                    className="hover:bg-gray-800/40 cursor-pointer transition-colors group"
                  >
                    <td className="px-5 py-4">
                      <RankBadge rank={idx + 1} />
                    </td>
                    <td className="px-5 py-4">
                      <div>
                        <p className="font-mono font-bold text-accent group-hover:underline">{stock.symbol}</p>
                        <p className="text-xs text-dark-muted truncate max-w-[180px]">{stock.company_name}</p>
                      </div>
                    </td>
                    <td className="px-5 py-4 text-right font-mono font-semibold">
                      ₹{sig.current_price?.toFixed(2) ?? '—'}
                    </td>
                    <td className={`px-5 py-4 text-right font-mono text-sm ${
                      (sig.change_pct ?? 0) > 0 ? 'text-signal-buy' : (sig.change_pct ?? 0) < 0 ? 'text-signal-sell' : 'text-dark-muted'
                    }`}>
                      {sig.change_pct != null ? `${sig.change_pct > 0 ? '+' : ''}${sig.change_pct.toFixed(2)}%` : '—'}
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex justify-center">
                        <span className={`flex items-center gap-1 px-2.5 py-1 rounded-lg border text-xs font-semibold ${stColors.bg} ${stColors.text} ${stColors.border}`}>
                          <SignalIcon signal={sig.st_signal} />
                          {sig.st_signal ?? '—'}
                        </span>
                      </div>
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex justify-center">
                        <span className={`flex items-center gap-1 px-2.5 py-1 rounded-lg border text-xs font-semibold ${ltColors.bg} ${ltColors.text} ${ltColors.border}`}>
                          <SignalIcon signal={sig.lt_signal} />
                          {sig.lt_signal ?? '—'}
                        </span>
                      </div>
                    </td>
                    <td className="px-5 py-4">
                      <ConfidenceBar value={sig.confidence_pct} />
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex justify-center">
                        <span className={`text-xs px-2 py-0.5 rounded font-mono border ${
                          sig.data_quality === 'HIGH' ? 'text-signal-buy border-signal-buy/30 bg-signal-buy/10' :
                          sig.data_quality === 'MEDIUM' ? 'text-signal-hold border-signal-hold/30 bg-signal-hold/10' :
                          'text-dark-muted border-dark-border'
                        }`}>
                          {sig.data_quality ?? 'N/A'}
                        </span>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {ranked.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-6 py-20 text-center">
                    <TrendingUp size={40} className="mx-auto text-dark-muted mb-3 opacity-40" />
                    <p className="text-dark-muted text-sm">
                      {Object.keys(stocks).length === 0
                        ? 'Connecting to live feed...'
                        : 'No stocks match your current filters.'}
                    </p>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {ranked.length > 0 && (
          <div className="px-5 py-3 border-t border-dark-border text-xs text-dark-muted flex justify-between items-center">
            <span>Showing {ranked.length} of {totals.all} stocks</span>
            <span>Ranked by confidence score ↓</span>
          </div>
        )}
      </div>
    </div>
  );
}
